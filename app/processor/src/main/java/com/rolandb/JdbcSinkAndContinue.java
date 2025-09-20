package com.rolandb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkAndContinue<K, E extends SequencedRow> extends KeyedProcessFunction<K, E, E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkAndContinue.class);

    public static interface StatementFunction<E> extends Serializable {
        public abstract void apply(PreparedStatement statement, E event) throws SQLException;
    }

    private final int retires;
    private final long batchMs;
    private final long batchSize;
    private final JdbcConnectionOptions jdbcOptions;
    private final String sqlInsert;
    private final StatementFunction<E> stmtFunction;
    // Event class needed to create the state descriptor for saving the temporarily
    // buffered events.
    private final Class<E> eventClass;

    // Currently buffered events.
    private transient ListState<E> buffer;
    // Last sequence number, used in case the event stream does not contain any.
    // It is to be considered undefined behavior if some events in an event stream
    // have sequence numbers, while others don't.
    private transient ValueState<Long> sequenceNumber;
    // Buffer size of the current buffer, to detect when to flush.
    private transient ValueState<Long> bufferSize;
    // Currently set timer timestamp. For deleting it in case of early flush.
    private transient ValueState<Long> currentTimer;

    // A single connection.
    private transient Connection connection;
    private transient PreparedStatement ps;

    public JdbcSinkAndContinue(
            JdbcConnectionOptions jdbcOptions, int retries, long batchSize, Duration batchDuration,
            String sqlInsert, StatementFunction<E> stmtFunction, Class<E> eventClass) {
        this.jdbcOptions = jdbcOptions;
        this.retires = retries;
        this.batchMs = batchDuration.toMillis();
        this.batchSize = batchSize;
        this.sqlInsert = sqlInsert;
        this.stmtFunction = stmtFunction;
        this.eventClass = eventClass;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Long> sequenceNumberDesc = new ValueStateDescriptor<>("sequenceNumber", Long.class);
        ValueStateDescriptor<Long> currentTimerDesc = new ValueStateDescriptor<>("currentTimer", Long.class);
        ValueStateDescriptor<Long> bufferSizeDesc = new ValueStateDescriptor<>("bufferSize", Long.class);
        ListStateDescriptor<E> bufferDesc = new ListStateDescriptor<>("buffer", eventClass);
        sequenceNumber = getRuntimeContext().getState(sequenceNumberDesc);
        currentTimer = getRuntimeContext().getState(currentTimerDesc);
        bufferSize = getRuntimeContext().getState(bufferSizeDesc);
        buffer = getRuntimeContext().getListState(bufferDesc);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            assert ps != null;
            ps.close();
            connection.close();
        }
    }

    private void flushBuffer(Context ctx, Collector<E> out) throws Exception {
        for (int r = 0;; r++) {
            try {
                long timestamp = Instant.now().toEpochMilli() * 1000;
                Long lastSeq = sequenceNumber.value();
                if (lastSeq == null || lastSeq < timestamp) {
                    // Use at least the current timestamp as teh next sequence number.
                    // This ensures that in case of a crash, the new events will
                    // override the old events, avoiding issues where sequence numbers
                    // are assigned in a different order for the retry.
                    lastSeq = timestamp - 1;
                }
                if (ps == null) {
                    assert connection == null;
                    connection = DriverManager.getConnection(
                            jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(), jdbcOptions.getPassword().get());
                    connection.setAutoCommit(false);
                    ps = connection.prepareStatement(sqlInsert);
                }
                connection.beginRequest();
                List<E> events = new ArrayList<>();
                try {
                    for (E event : buffer.get()) {
                        if (event.seqNum == null) {
                            // We fallback to a timestamp based sequence number.
                            lastSeq++;
                            event.seqNum = lastSeq;
                        }
                        stmtFunction.apply(ps, event);
                        ps.addBatch();
                        events.add(event);
                    }
                    ps.executeBatch();
                    connection.commit();
                } catch (SQLException exc) {
                    connection.rollback();
                    throw exc;
                } finally {
                    connection.endRequest();
                }
                // Now that the data is committed in the database, emit the sequenced
                // events for further processing.
                for (E event : events) {
                    out.collect(event);
                }
                buffer.clear();
                bufferSize.clear();
                Long timer = currentTimer.value();
                if (timer != null) {
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    currentTimer.clear();
                }
                sequenceNumber.update(lastSeq);
                return;
            } catch (SQLException exc) {
                if (r >= retires) {
                    throw new SQLException("Failed to insert elements into db", exc);
                } else {
                    LOGGER.error("Failed to insert elements into db", exc);
                    ps.close();
                    connection.close();
                    ps = null;
                    connection = null;
                    // Delay with some linear backoff.
                    Thread.sleep(1000 * (r + 1));
                }
            }
        }
    }

    @Override
    public void processElement(E event, Context ctx, Collector<E> out) throws Exception {
        Long size = bufferSize.value();
        if (size == null) {
            size = 0L;
        }
        buffer.add(event);
        bufferSize.update(size + 1);
        if (size + 1 >= batchSize) {
            flushBuffer(ctx, out);
        } else if (currentTimer.value() == null) {
            long timer = ctx.timerService().currentProcessingTime() + batchMs;
            ctx.timerService().registerProcessingTimeTimer(timer);
            currentTimer.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<E> out) throws Exception {
        currentTimer.clear();
        flushBuffer(ctx, out);
    }
}
