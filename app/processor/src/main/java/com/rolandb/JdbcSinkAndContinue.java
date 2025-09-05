package com.rolandb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class JdbcSinkAndContinue<K, E> extends KeyedProcessFunction<K, E, SequencedRow>
        implements CheckpointedFunction {
    public static interface StatementFunction<E> extends Serializable {
        public abstract void apply(PreparedStatement statement, SequencedRow<E> event) throws SQLException;
    }

    private final long batchMs;
    private final long batchSize;
    private final JdbcConnectionOptions jdbcOptions;
    private final String sqlInsert;
    private final StatementFunction<E> stmtFunction;

    // The next sequence number (Per-Key State)
    private transient ValueState<Long> seqNumber;
    // State handle for fault tolerance (Operator State)
    private transient ListState<SequencedRow<E>> bufferState;

    // Buffer of events that still have to be committed.
    private transient List<SequencedRow<E>> buffer;

    // A single connection.
    private transient Connection connection;
    private transient PreparedStatement ps;

    public JdbcSinkAndContinue(
            JdbcConnectionOptions jdbcOptions, long batchSize, Duration batchDuration, String sqlInsert,
            StatementFunction<E> stmtFunction) {
        this.jdbcOptions = jdbcOptions;
        this.batchMs = batchDuration.toMillis();
        this.batchSize = batchSize;
        this.sqlInsert = sqlInsert;
        this.stmtFunction = stmtFunction;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Long> seqNumDesc = new ValueStateDescriptor<>("sewNumber", Long.class);
        seqNumber = getRuntimeContext().getState(seqNumDesc);
        buffer = new ArrayList<>();
        connection = DriverManager.getConnection(
                jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(), jdbcOptions.getPassword().get());
        ps = connection.prepareStatement(sqlInsert);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private void flushBuffer(Collector<SequencedRow> out) throws Exception {
        for (SequencedRow<E> event : buffer) {
            stmtFunction.apply(ps, event);
            ps.addBatch();
        }
        ps.executeBatch();
        // Now that the data is committed in the database, emit the sequenced
        // events for further processing.
        for (SequencedRow<E> event : buffer) {
            out.collect(event);
        }
        buffer.clear();
    }

    @Override
    public void processElement(E event, Context ctx, Collector<SequencedRow> out) throws Exception {
        Long seqNum = seqNumber.value();
        if (seqNum == null) {
            seqNum = 0L;
        }
        buffer.add(new SequencedRow<>(event, seqNum));
        seqNumber.update(seqNum + 1);
        if (buffer.size() >= batchSize) {
            flushBuffer(out);
        } else if (buffer.size() == 1) {
            long time = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(time + batchMs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SequencedRow> out) throws Exception {
        flushBuffer(out);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        bufferState.clear();
        for (SequencedRow<E> event : buffer) {
            bufferState.add(event);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<SequencedRow<E>> bufferDesc = new ListStateDescriptor<>("buffer",
                TypeInformation.of(new TypeHint<SequencedRow<E>>() {
                }));
        bufferState = context.getOperatorStateStore().getListState(bufferDesc);
        // If recovering, restore the in-memory batch from the checkpointed state.
        if (context.isRestored()) {
            buffer.clear();
            for (SequencedRow<E> event : bufferState.get()) {
                buffer.add(event);
            }
        }
    }
}
