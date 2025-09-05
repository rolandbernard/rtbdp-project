package com.rolandb;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.Jdbc;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

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
    /**
     * This class is used to indicate in a table output type, which values are
     * part of the key.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface TableEventKey {
    }

    protected StreamExecutionEnvironment env;
    protected Map<String, Object> streams = new HashMap<>();
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

    public AbstractTableBuilder addStream(String name, Object stream) {
        this.streams.put(name, stream);
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
    protected <T> T getStream(String name) {
        return (T) streams.get(name);
    }

    /**
     * Create or reuse a named stream defined by the given supplier. This is
     * intended for easier reuse between the different tables. Ideally, only define
     * each method using this only once.
     * 
     * @param <T>
     *            The type of stream to return.
     * @param name
     *            The name of the stream.
     * @param create
     *            The computation defining the stream.
     * @return The cached stream or a newly created one.
     */
    protected <T> T getStream(String name, Supplier<T> create) {
        T stream = getStream(name);
        if (stream == null) {
            stream = create.get();
            streams.put(name, stream);
        }
        return stream;
    }

    protected DataStream<JsonNode> getRawEventStream() {
        return getStream("rawEvents");
    }

    protected DataStream<GithubEvent> getEventStream() {
        return getStream("events", () -> {
            return getRawEventStream()
                    .map(jsonNode -> new GithubEvent(jsonNode))
                    // We assume events can be up to 10 seconds late, but otherwise in-order.
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<GithubEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner((event, timestamp) -> event.createdAt.toEpochMilli()))
                    .name("Event Stream");
        });
    }

    protected KeyedStream<GithubEvent, String> getEventsByTypeStream() {
        return getStream("eventsByType", () -> {
            return getEventStream().keyBy(event -> event.eventType);
        });
    }

    protected DataStream<?> computeTable() {
        return getEventStream();
    }

    protected Class<?> getOutputType() {
        return GithubEvent.class;
    }

    protected String buildJdbcSinkStatement(Class<?> output) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(tableName);
        builder.append(" (");
        Field[] fields = output.getFields();
        for (Field field : fields) {
            JsonProperty prop = field.getAnnotation(JsonProperty.class);
            builder.append(prop == null ? field.getName() : prop.value());
            builder.append(", ");
        }
        builder.append("ts_write) VALUES (");
        for (int i = 0; i < fields.length; i++) {
            builder.append("?, ");
        }
        builder.append("?)");
        // If there is a conflict, we only want to update the non-key values.
        StringBuilder keyNames = new StringBuilder();
        boolean first = true;
        for (Field field : fields) {
            if (field.getAnnotation(TableEventKey.class) != null) {
                if (!first) {
                    keyNames.append(", ");
                }
                first = false;
                JsonProperty prop = field.getAnnotation(JsonProperty.class);
                keyNames.append(prop == null ? field.getName() : prop.value());
            }
        }
        if (!first) {
            builder.append(" ON CONFLICT (");
            builder.append(keyNames);
            builder.append(") DO UPDATE SET ");
            for (Field field : fields) {
                if (field.getAnnotation(TableEventKey.class) == null) {
                    JsonProperty prop = field.getAnnotation(JsonProperty.class);
                    String name = prop == null ? field.getName() : prop.value();
                    builder.append(name);
                    builder.append(" = EXCLUDED.");
                    builder.append(name);
                    builder.append(", ");
                }
            }
            builder.append("ts_write = EXCLUDED.ts_write");
        }
        return builder.toString();
    }

    protected JdbcSink<TimedRow> buildJdbcSink() {
        return Jdbc.<TimedRow>sinkBuilder()
                .withQueryStatement(
                        // The UPSERT SQL statement for PostgreSQL.
                        buildJdbcSinkStatement(getOutputType()),
                        // A lambda function to map the Row objects to the prepared statement.
                        (statement, row) -> {
                            int idx = 1;
                            for (Object value : row.getValues()) {
                                if (value instanceof Instant) {
                                    statement.setTimestamp(idx++, Timestamp.from((Instant) value));
                                } else {
                                    statement.setObject(idx++, value);
                                }
                            }
                            statement.setTimestamp(idx, Timestamp.from(row.getTime()));
                        })
                // JDBC execution options.
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(10_000) // Allow some batching.
                        .withBatchIntervalMs(100)
                        .withMaxRetries(5) // Retry up to 5 times on transient failures.
                        .build())
                // Use connection setting from setter.
                .buildAtLeastOnce(jdbcOptions);
    }

    protected KafkaSink<TimedRow> buildKafkaSink() {
        return KafkaSink.<TimedRow>builder()
                .setBootstrapServers(bootstrapServers)
                // We use a custom serialization schema, because otherwise it will set the
                // event-time as the created time, with is not really what we want, especially
                // when using the dummy data.
                .setRecordSerializer(new KafkaTimedRowSerializer(tableName))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
                .setProperty(ProducerConfig.ACKS_CONFIG, "1") // Wait for only one in-sync replica to acknowledge.
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000") // Allow some batching.
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
                .setProperty(ProducerConfig.RETRIES_CONFIG, "5") // Retry up to 5 times on transient failures.
                .build();
    }

    protected AbstractTableBuilder build() throws ExecutionException, InterruptedException {
        DataStream<?> stream = computeTable();
        // We partition here by key so that all rows for the same key are
        // handled by the same subtask, ensuring timestamps are monotonic.
        if (TimedRow.hasKeyIn(getOutputType())) {
            stream = stream.<Object>keyBy(row -> TimedRow.readKeyFrom(row));
        }
        DataStream<TimedRow> dataStream = stream.map(row -> new TimedRow(row)).name("Adding Process Time");
        if (dryRun) {
            dataStream.print().setParallelism(1);
        } else {
            KafkaUtil.setupTopic(tableName, bootstrapServers, numPartitions, replicationFactor);
            dataStream.sinkTo(buildJdbcSink()).name("PostgreSQL Sink");
            dataStream.sinkTo(buildKafkaSink()).name("Kafka Sink");
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
        instance.streams = this.streams;
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
