package com.rolandb;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
public abstract class AbstractTable<E extends SequencedRow> {
    /**
     * This class is used to indicate in a table output type, which values are
     * part of the key.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface TableEventKey {
    }

    public static class TableBuilder {
        protected StreamExecutionEnvironment env;
        protected Map<String, Object> streams = new HashMap<>();
        private JdbcConnectionOptions jdbcOptions;
        private String bootstrapServers = "localhost:29092";
        private boolean dryRun = false;
        private int numPartitions = 1;
        private int replicationFactor = 1;
        private long retentionMs = 604800000;

        public TableBuilder setEnv(StreamExecutionEnvironment env) {
            this.env = env;
            return this;
        }

        public TableBuilder addStream(String name, Object stream) {
            this.streams.put(name, stream);
            return this;
        }

        public TableBuilder setJdbcOptions(JdbcConnectionOptions jdbcOptions) {
            this.jdbcOptions = jdbcOptions;
            return this;
        }

        public TableBuilder setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public TableBuilder setDryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        public TableBuilder setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public TableBuilder setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public TableBuilder setRetentionMs(long retentionMs) {
            this.retentionMs = retentionMs;
            return this;
        }

        public <E extends SequencedRow, T extends AbstractTable<E>> T get(String tableName, Class<T> clazz) {
            T instance;
            try {
                instance = clazz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException
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
            instance.retentionMs = this.retentionMs;
            instance.tableName = tableName;
            return instance;
        }

        public <E extends SequencedRow, T extends AbstractTable<E>> TableBuilder build(String tableName, Class<T> clazz)
                throws ExecutionException, InterruptedException {
            T instance = get(tableName, clazz);
            instance.build();
            return this;
        }
    }

    protected StreamExecutionEnvironment env;
    protected Map<String, Object> streams = new HashMap<>();
    protected String tableName;
    protected JdbcConnectionOptions jdbcOptions;
    protected String bootstrapServers = "localhost:29092";
    protected boolean dryRun = false;
    protected int numPartitions = 1;
    protected int replicationFactor = 1;
    protected long retentionMs = 604800000;

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

    protected DataStream<GithubEvent> getEventsWithAllStream() {
        return getStream("eventsWithAll", () -> {
            return getEventStream()
                    // Add in an additional "fake" event with kind "all". This is
                    // so we conveniently compute also the aggregate count over all
                    // event kinds.
                    .<GithubEvent>flatMap((event, out) -> {
                        out.collect(event);
                        out.collect(new GithubEvent(
                                GithubEventType.ALL, event.createdAt, event.userId, event.repoId, event.seqNum));
                    })
                    .returns(GithubEvent.class)
                    .name("Add 'all' Events");
        });
    }

    protected KeyedStream<GithubEvent, String> getEventsByTypeStream() {
        return getStream("eventsByType", () -> {
            return getEventsWithAllStream().keyBy(event -> event.eventType.toString());
        });
    }

    protected KeyedStream<GithubEvent, Long> getEventsByRepoStream() {
        return getStream("eventsByRepo", () -> {
            return getEventStream().keyBy(event -> event.repoId);
        });
    }

    protected KeyedStream<GithubEvent, Long> getEventsByUserStream() {
        return getStream("eventsByUser", () -> {
            return getEventStream().keyBy(event -> event.userId);
        });
    }

    protected abstract DataStream<E> computeTable();

    protected abstract Class<E> getOutputType();

    protected void buildSqlConflictResolution(String keyNames, Field[] fields, StringBuilder builder) {
        builder.append(" ON CONFLICT (");
        builder.append(keyNames);
        builder.append(") DO UPDATE SET ");
        boolean first = true;
        for (Field field : fields) {
            if (field.getAnnotation(TableEventKey.class) == null) {
                JsonProperty prop = field.getAnnotation(JsonProperty.class);
                String name = prop == null ? field.getName() : prop.value();
                if (!first) {
                    builder.append(", ");
                }
                first = false;
                builder.append(name);
                builder.append(" = EXCLUDED.");
                builder.append(name);
            }
        }
        builder.append(" WHERE ");
        builder.append(tableName);
        builder.append(".seq_num < EXCLUDED.seq_num");
    }

    protected void buildSqlExtraFields(Class<E> output, StringBuilder builder) {
        // To be implemented in a subclass.
    }

    protected void buildSqlExtraValues(Class<E> output, StringBuilder builder) {
        // To be implemented in a subclass.
    }

    protected String buildJdbcSinkStatement(Class<E> output) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(tableName);
        builder.append(" (");
        Field[] fields = output.getFields();
        boolean first = true;
        for (Field field : fields) {
            JsonProperty prop = field.getAnnotation(JsonProperty.class);
            if (!first) {
                builder.append(", ");
            }
            first = false;
            builder.append(prop == null ? field.getName() : prop.value());
        }
        buildSqlExtraFields(output, builder);
        builder.append(") VALUES (");
        first = true;
        for (int i = 0; i < fields.length; i++) {
            if (!first) {
                builder.append(", ");
            }
            first = false;
            builder.append("?");
        }
        buildSqlExtraValues(output, builder);
        builder.append(")");
        // If there is a conflict, we only want to update the non-key values.
        StringBuilder keyNames = new StringBuilder();
        first = true;
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
            buildSqlConflictResolution(keyNames.toString(), fields, builder);
        }
        return builder.toString();
    }

    private static String removeInvalidCodePoints(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length();) {
            int cp = s.codePointAt(i);
            if (cp != 0 && Character.isValidCodePoint(cp)) {
                sb.appendCodePoint(cp);
            }
            i += Character.charCount(cp);
        }
        return sb.toString();
    }

    protected static void jdbcSinkSetStatementValues(PreparedStatement statement, SequencedRow row)
            throws SQLException {
        int idx = 1;
        for (Object value : row.getValues()) {
            if (value instanceof Instant) {
                statement.setTimestamp(idx++, Timestamp.from((Instant) value));
            } else if (value instanceof Enum) {
                statement.setObject(idx++, value.toString());
            } else if (value instanceof String) {
                statement.setObject(idx++, removeInvalidCodePoints((String) value));
            } else {
                statement.setObject(idx++, value);
            }
        }
    }

    protected JdbcSink<SequencedRow> buildJdbcSink() {
        return Jdbc.<SequencedRow>sinkBuilder()
                .withQueryStatement(
                        // The UPSERT SQL statement for PostgreSQL.
                        buildJdbcSinkStatement(getOutputType()),
                        // A lambda function to map the Row objects to the prepared statement.
                        AbstractTable::jdbcSinkSetStatementValues)
                // JDBC execution options.
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(10_000) // Allow some batching.
                        .withBatchIntervalMs(100)
                        .withMaxRetries(5) // Retry up to 5 times on transient failures.
                        .build())
                // Use connection setting from setter.
                .buildAtLeastOnce(jdbcOptions);
    }

    protected JdbcSinkAndContinue<String, E> buildJdbcSinkAndContinue() {
        return new JdbcSinkAndContinue<>(
                // Use connection setting from setter.
                jdbcOptions,
                // JDBC execution options.
                5, 10_000, Duration.ofMillis(100),
                // The UPSERT SQL statement for PostgreSQL.
                buildJdbcSinkStatement(getOutputType()),
                // A lambda function to map the Row objects to the prepared statement.
                AbstractTable::jdbcSinkSetStatementValues,
                getOutputType());
    }

    protected <T extends SequencedRow> KafkaSink<T> buildKafkaSink() {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                // We use a custom serialization schema, because otherwise it will set the
                // event-time as the created time, with is not really what we want, especially
                // when using the dummy data.
                .setRecordSerializer(new KafkaTimedRowSerializer<T>(tableName))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
                .setProperty(ProducerConfig.ACKS_CONFIG, "1") // Wait for only one in-sync replica to acknowledge.
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000") // Allow some batching.
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
                .setProperty(ProducerConfig.RETRIES_CONFIG, "5") // Retry up to 5 times on transient failures.
                .build();
    }

    protected DataStream<E> sinkToPostgres(DataStream<E> stream) {
        return stream
                .keyBy(row -> "dummyKey")
                .process(buildJdbcSinkAndContinue())
                .returns(getOutputType())
                .name("PostgreSQL Sink")
                // One writer per-table/topic should be sufficient. Also, we
                // key by a dummy, so there is no parallelism anyway.
                .setParallelism(1);
    }

    protected void sinkToKafka(DataStream<E> stream) throws ExecutionException, InterruptedException {
        KafkaUtil.setupTopic(tableName, bootstrapServers, numPartitions, replicationFactor, retentionMs);
        stream.sinkTo(buildKafkaSink()).name("Kafka Sink");
    }

    public void build() throws ExecutionException, InterruptedException {
        // We partition here by key so that all rows for the same key are
        // handled by the same subtask, ensuring timestamps are monotonic.
        DataStream<E> rawStream = computeTable();
        if (dryRun) {
            rawStream.print().setParallelism(1);
        } else {
            DataStream<E> committedStream = sinkToPostgres(rawStream);
            streams.put("[table]" + tableName, committedStream);
            sinkToKafka(committedStream);
        }
    }
}
