package com.rolandb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkAndContinue<K, E> extends KeyedProcessFunction<K, E, SequencedRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkAndContinue.class);

    public static interface StatementFunction<E> extends Serializable {
        public abstract void apply(PreparedStatement statement, SequencedRow<E> event) throws SQLException;
    }

    private final int retires;
    private final long batchMs;
    private final long batchSize;
    private final JdbcConnectionOptions jdbcOptions;
    private final String sqlInsert;
    private final StatementFunction<E> stmtFunction;

    // The next sequence number,
    private transient ValueState<Long> seqNumber;
    // Currently buffered events.
    private transient ListState<SequencedRow<E>> buffer;
    // Buffer size of the current buffer, to detect when to flush.
    private transient ValueState<Long> bufferSize;
    // Currently set timer timestamp. For deleting it in case of early flush.
    private transient ValueState<Long> currentTimer;

    // A single connection.
    private transient Connection connection;
    private transient PreparedStatement ps;

    public JdbcSinkAndContinue(
            JdbcConnectionOptions jdbcOptions, int retries, long batchSize, Duration batchDuration,
            String sqlInsert, StatementFunction<E> stmtFunction) {
        this.jdbcOptions = jdbcOptions;
        this.retires = retries;
        this.batchMs = batchDuration.toMillis();
        this.batchSize = batchSize;
        this.sqlInsert = sqlInsert;
        this.stmtFunction = stmtFunction;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Long> seqNumDesc = new ValueStateDescriptor<>("sewNumber", Long.class);
        ValueStateDescriptor<Long> currentTimerDesc = new ValueStateDescriptor<>("currentTimer", Long.class);
        ValueStateDescriptor<Long> bufferSizeDesc = new ValueStateDescriptor<>("bufferSize", Long.class);
        ListStateDescriptor<SequencedRow<E>> bufferDesc = new ListStateDescriptor<>("buffer",
                TypeInformation.of(new TypeHint<SequencedRow<E>>() {
                }));
        seqNumber = getRuntimeContext().getState(seqNumDesc);
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

    private void flushBuffer(Context ctx, Collector<SequencedRow> out) throws Exception {
        for (int r = 0;; r++) {
            try {
                if (ps == null) {
                    assert connection == null;
                    connection = DriverManager.getConnection(
                            jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(), jdbcOptions.getPassword().get());
                    ps = connection.prepareStatement(sqlInsert);
                }
                for (SequencedRow<E> event : buffer.get()) {
                    stmtFunction.apply(ps, event);
                    ps.addBatch();
                }
                ps.executeBatch();
                // Now that the data is committed in the database, emit the sequenced
                // events for further processing.
                for (SequencedRow<E> event : buffer.get()) {
                    out.collect(event);
                }
                buffer.clear();
                bufferSize.clear();
                Long timer = currentTimer.value();
                if (timer != null) {
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    currentTimer.clear();
                }
                return;
            } catch (SQLException exc) {
                if (r >= retires) {
                    throw new SQLException("failed to insert elements into db", exc);
                } else {
                    LOGGER.error("failed to insert elements into db", exc);
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
    public void processElement(E event, Context ctx, Collector<SequencedRow> out) throws Exception {
        Long seqNum = seqNumber.value();
        if (seqNum == null) {
            seqNum = 0L;
        }
        Long size = bufferSize.value();
        if (size == null) {
            size = 0L;
        }
        buffer.add(new SequencedRow<>(event, seqNum));
        seqNumber.update(seqNum + 1);
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
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SequencedRow> out) throws Exception {
        currentTimer.clear();
        flushBuffer(ctx, out);
    }
}
