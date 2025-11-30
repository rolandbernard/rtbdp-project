package com.rolandb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

/**
 * This is a custom JDBC sink that in after committing the events into the
 * database emits them again so that they can be processed further. It also
 * assigns them sequence numbers in monotonically increasing order to enure that
 * the client can correctly handle them.
 * 
 * @param <K>
 *            The type of key used in the stream.
 * @param <E>
 *            The type of event used in the stream.
 */
public class JdbcSinkAndContinue<K, E extends SequencedRow> extends KeyedProcessFunction<K, E, E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkAndContinue.class);

    /**
     * A interface for the function that should be provided for populating the
     * prepared statement.
     * 
     * @param <E>
     *            The type of event.
     */
    public static interface StatementFunction<E> extends Serializable {
        /**
         * Populate the provided statement with the values from the given event.
         * 
         * @param statement
         *            The statement to fill.
         * @param event
         *            The event to use.
         * @throws SQLException
         *             In case of errors.
         */
        public abstract void apply(PreparedStatement statement, E event) throws SQLException;
    }

    /** The number of retires to attempt on intermittent issues. */
    private final int retires;
    /** The maximum delay before an event is flushed. */
    private final long batchMs;
    /** The maximum number of events before we flush them. */
    private final long batchSize;
    /** The JDBC connection options to use for connecting to the db. */
    private final JdbcConnectionOptions jdbcOptions;
    /** The SQL statement to use for insertion. */
    private final String sqlInsert;
    /** A function with which to populate the prepared statement. */
    private final StatementFunction<E> stmtFunction;
    /**
     * Event class needed to create the state descriptor for saving the temporarily
     * buffered events.
     */
    private final Class<E> eventClass;

    /** Currently buffered events. */
    private transient ListState<E> buffer;
    /**
     * Last sequence number, used in case the event stream does not contain any.
     * It is to be considered undefined behavior if some events in an event stream
     * have sequence numbers, while others don't.
     */
    private transient ValueState<Long> sequenceNumber;
    /** Buffer size of the current buffer, to detect when to flush. */
    private transient ValueState<Long> bufferSize;
    /** Currently set timer timestamp. For deleting it in case of early flush. */
    private transient ValueState<Long> currentTimer;

    /** A single connection. */
    private transient Connection connection;
    private transient PreparedStatement ps;

    /**
     * Create a new instance of the sink.
     * 
     * @param jdbcOptions
     *            JDBC connection options.
     * @param retries
     *            The number of reties in case of transient database connection
     *            issues.
     * @param batchSize
     *            The batch size to use when batching the events.
     * @param batchDuration
     *            The maximum delay in processing time before flushing an event.
     * @param sqlInsert
     *            The SQL statement to use for inserting into the database.
     * @param stmtFunction
     *            A function that populates a prepared statement created based on
     *            the query in {@code sqlInsert}:
     * @param eventClass
     *            The class of events we want to store.
     */
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
                    // Use at least the current timestamp as the next sequence number.
                    // This ensures that in case of a crash, the new events will
                    // override the old events, avoiding issues where sequence numbers
                    // are assigned in a different order for the retry.
                    lastSeq = timestamp - 1;
                }
                if (ps == null) {
                    assert connection == null;
                    connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getProperties());
                    connection.setAutoCommit(false);
                    ps = connection.prepareStatement(sqlInsert);
                }
                Map<List<?>, E> events = new HashMap<>();
                for (E event : buffer.get()) {
                    if (event.seqNum == null) {
                        // We fallback to a timestamp based sequence number.
                        lastSeq++;
                        event.seqNum = lastSeq;
                    }
                    List<?> key = event.getKey();
                    if (events.containsKey(key)) {
                        events.get(key).mergeWith(event);
                    } else {
                        events.put(key, event);
                    }
                }
                connection.beginRequest();
                try {
                    for (E event : events.values()) {
                        stmtFunction.apply(ps, event);
                        ps.addBatch();
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
                for (E event : events.values()) {
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
                    if (ps != null) {
                        try {
                            ps.close();
                        } catch (SQLException e) {
                            LOGGER.error("Error closing prepared statement", exc);
                        }
                        ps = null;
                    }
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            LOGGER.error("Error closing database connection", exc);
                        }
                        connection = null;
                    }
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
