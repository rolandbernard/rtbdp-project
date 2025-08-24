package com.rolandb;

import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.fasterxml.jackson.databind.JsonNode;

import static org.apache.flink.table.api.Expressions.*;

/**
 * This class contains the basic logic for writing a computed table to both a
 * PostgreSQL table and simultaneously the change log also to a separate Kafka
 * topic. The goal is to make it possible for a client to connect to the Kafka
 * topic, fetch the latest state from the database and then update it based on
 * the messages from the Kafka topic.
 * This is intended to be an subclassed, but if used as it it will simply write
 * out the events table as-is.
 */
public class AbstractTableBuilder {
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tenv;
    protected Map<String, DataStream<?>> streams = new HashMap<>();
    protected Map<String, Table> tables = new HashMap<>();
    private JdbcConnectionOptions jdbcOptions;
    private String bootstrapServers = "localhost:29092";
    private boolean dryRun = false;
    private int numPartitions = 1;
    private int replicationFactor = 1;
    private String tableName;

    public AbstractTableBuilder setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public AbstractTableBuilder setTableEnv(StreamTableEnvironment tenv) {
        this.tenv = tenv;
        return this;
    }

    public AbstractTableBuilder addStream(String name, DataStream<?> stream) {
        this.streams.put(name, stream);
        return this;
    }

    public AbstractTableBuilder addTable(String name, Table table) {
        this.tables.put(name, table);
        return this;
    }

    public AbstractTableBuilder setJdbcOptions(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        return this;
    }

    public AbstractTableBuilder setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public AbstractTableBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public AbstractTableBuilder setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public AbstractTableBuilder setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    public AbstractTableBuilder setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
        return this;
    }

    @SuppressWarnings("unchecked")
    protected <T> DataStream<T> getStream(String name) {
        return (DataStream<T>) streams.get(name);
    }

    protected Table getTable(String name) {
        return tables.get(name);
    }

    protected DataStream<JsonNode> getRawEventStream() {
        return getStream("rawEvents");
    }

    protected DataStream<GithubEvent> getEventStream() {
        DataStream<GithubEvent> stream = getStream("events");
        if (stream == null) {
            stream = getRawEventStream()
                    .map(jsonNode -> new GithubEvent(jsonNode))
                    // We assume events can be up to 10 seconds late, but otherwise in-order.
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<GithubEvent>forBoundedOutOfOrderness(
                                            Duration.ofSeconds(10))
                                    .withTimestampAssigner((event, timestamp) -> event
                                            .getCreatedAt().toEpochMilli()))
                    .name("Event Stream");
            streams.put("events", stream);
        }
        return stream;
    }

    protected Table getEventTable() {
        Table table = getTable("events");
        if (table == null) {
            table = tenv.fromDataStream(getEventStream(),
                    Schema.newBuilder()
                            .column("eventType", DataTypes.STRING())
                            .column("createdAt", DataTypes.TIMESTAMP_LTZ(3))
                            .column("username", DataTypes.STRING())
                            .column("reponame", DataTypes.STRING())
                            .watermark("createdAt", call("SOURCE_WATERMARK"))
                            .build())
                    .as("kind", "created_at", "username", "reponame");
                    // .renameColumns($("eventType").as("kind"))
                    // .renameColumns($("createdAt").as("created_at"));
            tables.put("events", table);
        }
        return table;
    }

    protected Table computeTable() {
        return getEventTable();
    }

    protected String[] getPrimaryKeyNames() {
        return new String[0];
    }

    protected String buildJdbcSinkStatement(List<String> columnNames, List<String> keyNames) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(tableName);
        builder.append(" (");
        for (String column : columnNames) {
            builder.append(column);
            builder.append(", ");
        }
        builder.append("ts_write) VALUES (");
        for (int i = 0; i < columnNames.size(); i++) {
            builder.append("?, ");
        }
        builder.append("?)");
        // If there is a conflict, we only want to update the non-key values.
        if (!keyNames.isEmpty()) {
            builder.append(" ON CONFLICT (");
            boolean first = true;
            for (String key : keyNames) {
                if (!first) {
                    builder.append(", ");
                }
                first = false;
                builder.append(key);
            }
            builder.append(") DO UPDATE SET ");
            for (String column : columnNames) {
                if (!keyNames.contains(column)) {
                    builder.append(column);
                    builder.append(" = EXCLUDED.");
                    builder.append(column);
                    builder.append(", ");
                }
            }
            builder.append("ts_write = EXCLUDED.ts_write");
        }
        return builder.toString();
    }

    protected SinkFunction<TimedRow> buildJdbcSink(List<String> columnNames, List<String> keyNames) {
        return JdbcSink.<TimedRow>sink(
                // The UPSERT SQL statement for PostgreSQL.
                buildJdbcSinkStatement(columnNames, keyNames),
                // A lambda function to map the Row objects to the prepared statement.
                (statement, row) -> {
                    int idx = 1;
                    for (String column : columnNames) {
                        Object value = row.getFieldAs(column);
                        if (value instanceof Instant) {
                            statement.setObject(idx++, Timestamp.from((Instant) value));
                        } else {
                            statement.setObject(idx++, value);
                        }
                    }
                    statement.setTimestamp(idx, Timestamp.from(row.getTime()));
                },
                // JDBC execution options.
                JdbcExecutionOptions.builder()
                        .withBatchSize(10_000)
                        .withBatchIntervalMs(250)
                        .withMaxRetries(5)
                        .build(),
                // Use connection setting from setter.
                jdbcOptions);
    }

    protected KafkaSink<TimedRow> buildKafkaSink(List<String> columnNames, List<String> keyNames) {
        return KafkaSink.<TimedRow>builder()
                .setBootstrapServers(bootstrapServers)
                // We use a custom serialization schema, because otherwise it will set the
                // event-time as the created time, with is not really what we want, especially
                // when using the dummy data.
                .setRecordSerializer(new KafkaTimedRowSerializer(tableName, columnNames, keyNames))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
                .setProperty(ProducerConfig.ACKS_CONFIG, "1") // Wait for only one in-sync replica to acknowledge.
                .setProperty(ProducerConfig.RETRIES_CONFIG, "5") // Retry up to 5 times on transient failures.
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "250") // Allow some batching.
                .build();
    }

    protected AbstractTableBuilder build() throws ExecutionException, InterruptedException {
        Table table = computeTable();
        ResolvedSchema schema = table.getResolvedSchema();
        List<String> columnNames = schema.getColumnNames();
        String[] keyNames = getPrimaryKeyNames();
        DataStream<Row> filteredStream = tenv.toChangelogStream(table)
                .filter(row -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
                .name("Upsert Filter");
        if (keyNames.length != 0) {
            // We partition here by key so that all rows for the same key are
            // handled by the same subtask, ensuring timestamps are monotonic.
            filteredStream = filteredStream.<Row>keyBy(row -> Row.project(row, keyNames));
        }
        DataStream<TimedRow> dataStream = filteredStream.map(row -> new TimedRow(row)).name("Adding Process Time");
        if (dryRun) {
            dataStream.print().setParallelism(1);
        } else {
            KafkaUtil.setupTopic(tableName, bootstrapServers, numPartitions, replicationFactor);
            dataStream.addSink(buildJdbcSink(columnNames, List.of(keyNames))).name("PostgreSQL Sink");
            dataStream.sinkTo(buildKafkaSink(columnNames, List.of(keyNames))).name("Kafka Sink");
        }
        return this;
    }

    public AbstractTableBuilder build(String tableName, Class<? extends AbstractTableBuilder> clazz)
            throws ExecutionException, InterruptedException {
        AbstractTableBuilder instance;
        try {
            instance = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("There should always be a default constructor", e);
        }
        instance.env = this.env;
        instance.tenv = this.tenv;
        instance.streams = this.streams;
        instance.tables = this.tables;
        instance.jdbcOptions = this.jdbcOptions;
        instance.bootstrapServers = this.bootstrapServers;
        instance.dryRun = this.dryRun;
        instance.numPartitions = this.numPartitions;
        instance.replicationFactor = this.replicationFactor;
        instance.tableName = tableName;
        instance.build();
        return this;
    }
}
