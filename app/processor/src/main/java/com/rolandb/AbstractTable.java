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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
 * 
 * @param <E>
 *            The type of event this table outputs.
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

    /**
     * A builder for configuring and instantiating a table instance.
     */
    public static class TableBuilder {
        /** The execution environment to run on. */
        protected StreamExecutionEnvironment env;
        /** A map for sharing computation results between tables. */
        protected Map<String, Object> streams;
        private JdbcConnectionOptions jdbcOptions;
        private String bootstrapServers;
        private boolean dryRun;
        private int numPartitions;
        private int replicationFactor;
        private long retentionMs;

        /**
         * Instantiate a new builder with default parameters.
         */
        public TableBuilder() {
            streams = new HashMap<>();
            dryRun = false;
            numPartitions = 1;
            replicationFactor = 1;
            retentionMs = 604800000;
        }

        /**
         * Set the execution environment.
         * 
         * @param env
         *            The execution environment.
         * @return {@code this}
         */
        public TableBuilder setEnv(StreamExecutionEnvironment env) {
            this.env = env;
            return this;
        }

        /**
         * Add a stream to the shared stream map.
         * 
         * @param name
         *            The name of the stream.
         * @param stream
         *            The stream to add.
         * @return {@code this}
         */
        public TableBuilder addStream(String name, Object stream) {
            this.streams.put(name, stream);
            return this;
        }

        /**
         * Set the JDB connection options.
         * 
         * @param jdbcOptions
         *            The JDBC connection options.
         * @return {@code this}
         */
        public TableBuilder setJdbcOptions(JdbcConnectionOptions jdbcOptions) {
            this.jdbcOptions = jdbcOptions;
            return this;
        }

        /**
         * Set the bootstrap sever address.
         * 
         * @param bootstrapServers
         *            The bootstrap server.
         * @return {@code this}
         */
        public TableBuilder setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        /**
         * Set whether this should be for a dry run.
         * 
         * @param dryRun
         *            {@code true} if a dry run.
         * @return {@code this}
         */
        public TableBuilder setDryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        /**
         * Set the number of partitions for the Kafka topic.
         * 
         * @param numPartitions
         *            The number of partitions.
         * @return {@code this}
         */
        public TableBuilder setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Set the replication factor for the Kafka topic.
         * 
         * @param replicationFactor
         *            The replication factor.
         * @return {@code this}
         */
        public TableBuilder setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        /**
         * Set the retention time for the Kafka topic.
         * 
         * @param retentionMs
         *            The retention time.
         * @return {@code this}
         */
        public TableBuilder setRetentionMs(long retentionMs) {
            this.retentionMs = retentionMs;
            return this;
        }

        /**
         * Instantiate the table and return it.
         * 
         * @param <E>
         *            The type of event the table works with.
         * @param <T>
         *            The type of the table.
         * @param tableName
         *            The name for the table.
         * @param clazz
         *            The class to instantiate.
         * @return The instantiated and configured table.
         */
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

        /**
         * Instantiate the table and build the computation graph.
         * 
         * @param <E>
         *            The type of event the table works with.
         * @param <T>
         *            The type of the table.
         * @param tableName
         *            The name for the table.
         * @param clazz
         *            The class to instantiate.
         * @return {@code this}
         * @throws ExecutionException
         *             If there is a Kafka issue.
         * @throws InterruptedException
         *             If interrupted.
         */
        public <E extends SequencedRow, T extends AbstractTable<E>> TableBuilder build(String tableName, Class<T> clazz)
                throws ExecutionException, InterruptedException {
            T instance = get(tableName, clazz);
            instance.build();
            return this;
        }
    }

    /** The Flink environment to operate in. */
    protected StreamExecutionEnvironment env;
    /** A map of streams for easier reuse of the same computation between tables. */
    protected Map<String, Object> streams;
    /** The name of this table. */
    protected String tableName;
    /** The JDBC options to use for connecting to PostgreSQL. */
    protected JdbcConnectionOptions jdbcOptions;
    /** The Kafka broker to connect to. */
    protected String bootstrapServers;
    /** If this is a dry run, we only pint to standard output. */
    protected boolean dryRun;
    /** The number of partitions to create in the Kafka topic. */
    protected int numPartitions;
    /** The replication factor to create the Kafka topic with- */
    protected int replicationFactor;
    /** The retention time to create the Kafka topic with. */
    protected long retentionMs;

    /**
     * Instantiate the table with default parameters.
     */
    protected AbstractTable() {
        streams = new HashMap<>();
        dryRun = false;
        numPartitions = 1;
        replicationFactor = 1;
        retentionMs = 604800000;
    }

    /**
     * Get a named stream from the shared dictionary.
     * 
     * @param <T>
     *            The type of the stream.
     * @param name
     *            The name in the shared map.
     * @return The stream of {@code null} if not found.
     */
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

    /**
     * Get the stream of raw events.
     * 
     * @return The raw event stream.
     */
    protected DataStream<JsonNode> getRawEventStream() {
        return getStream("rawEventsDedup", () -> {
            return this.<DataStream<JsonNode>>getStream("rawEvents")
                    .keyBy(jsonNode -> jsonNode.at("/id").asText())
                    .filter(new Deduplicate<>())
                    .uid("event-dedup-01")
                    .name("Events Deduplicated");
        });
    }

    /**
     * Get the stream of parsed events.
     * 
     * @return The event stream.
     */
    protected DataStream<GithubEvent> getEventStream() {
        return getStream("events", () -> {
            return getRawEventStream()
                    .map(jsonNode -> new GithubEvent(jsonNode))
                    .uid("event-stream-map-01")
                    // We assume events can be up to 10 seconds late, but otherwise in-order.
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<GithubEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner((event, timestamp) -> event.createdAt.toEpochMilli()))
                    .uid("event-stream-01")
                    .name("Event Stream");
        });
    }

    /**
     * Get the stream of events that additionally contains a an additional
     * {@code all} event for every regular event.
     * 
     * @return The events stream.
     */
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
                    .uid("event-stream-with-all-01")
                    .name("Add 'all' Events");
        });
    }

    /**
     * Get the event stream keyed by type of event.
     * 
     * @return The keyed stream.
     */
    protected KeyedStream<GithubEvent, String> getEventsByTypeStream() {
        return getStream("eventsByType", () -> {
            return getEventsWithAllStream().keyBy(event -> event.eventType.toString());
        });
    }

    /**
     * Get the event stream keyed by repository of event.
     * 
     * @return The keyed stream.
     */
    protected KeyedStream<GithubEvent, Long> getEventsByRepoStream() {
        return getStream("eventsByRepo", () -> {
            return getEventStream().keyBy(event -> event.repoId);
        });
    }

    /**
     * Get the stream of starring events keyed by type of event.
     * 
     * @return The keyed stream.
     */
    protected KeyedStream<GithubEvent, Long> getStarEventsByRepoStream() {
        return getStream("starsByRepo", () -> {
            return getEventStream()
                    .filter(e -> e.eventType == GithubEventType.WATCH)
                    .uid("star-events-01")
                    .name("Filter Star Events")
                    .keyBy(event -> event.repoId);
        });
    }

    /**
     * Get the stream events keyed by user of event.
     * 
     * @return The keyed stream.
     */
    protected KeyedStream<GithubEvent, Long> getEventsByUserStream() {
        return getStream("eventsByUser", () -> {
            return getEventStream().keyBy(event -> event.userId);
        });
    }

    /**
     * Compute the output events that should be streamed into the table.
     * 
     * @return The stream out output events.
     */
    protected abstract DataStream<E> computeTable();

    /**
     * Get the class of output events.
     * 
     * @return The class of output events.
     */
    protected abstract Class<E> getOutputType();

    /**
     * Create the SQL expression to use for resolving key conflicts.
     * 
     * @param keyNames
     *            The names of the key fields.
     * @param fields
     *            All fields in the output type.
     * @param builder
     *            The string builder to write into.
     */
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

    /**
     * Build SQL statement for any extra fields to output.
     * 
     * @param output
     *            The type to create the statement for.
     * @param builder
     *            The builder to write into.
     */
    protected void buildSqlExtraFields(Class<E> output, StringBuilder builder) {
        // To be implemented in a subclass.
    }

    /**
     * Build SQL statement for any extra values to output.
     * 
     * @param output
     *            The type to create the statement for.
     * @param builder
     *            The builder to write into.
     */
    protected void buildSqlExtraValues(Class<E> output, StringBuilder builder) {
        // To be implemented in a subclass.
    }

    /**
     * Build the SQL statement used for inserting result object of this table into
     * PostgreSQL.
     * 
     * @param output
     *            Tht output type.
     * @return The SQL statement string.
     */
    public String buildJdbcSinkStatement(Class<E> output) {
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

    private static boolean isSurrogate(int cp) {
        return cp <= 0xffff && Character.isSurrogate((char) cp);
    }

    private static String removeInvalidCodePoints(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length();) {
            int cp = s.codePointAt(i);
            if (cp != 0 && Character.isValidCodePoint(cp) && !isSurrogate(cp)) {
                sb.appendCodePoint(cp);
            }
            i += Character.charCount(cp);
        }
        return sb.toString();
    }

    private static void jdbcSinkSetStatementValues(PreparedStatement statement, SequencedRow row)
            throws SQLException {
        if (row.seqNum == null) {
            throw new IllegalStateException("Attempting to store event without sequence number: " + row);
        }
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

    private JdbcSinkAndContinue<E> buildJdbcSinkAndContinue() {
        return new JdbcSinkAndContinue<>(
                // Use connection setting from setter.
                jdbcOptions,
                // JDBC execution options.
                5,
                // The UPSERT SQL statement for PostgreSQL.
                buildJdbcSinkStatement(getOutputType()),
                // A lambda function to map the Row objects to the prepared statement.
                AbstractTable::jdbcSinkSetStatementValues);
    }

    /**
     * Build the Kafka sink for this table.
     * 
     * @param <T>
     *            The type of events to write into Kafka.
     * @return The Kafka sink.
     */
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
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1024") // Allow some batching.
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
                .setProperty(ProducerConfig.RETRIES_CONFIG, "5") // Retry up to 5 times on transient failures.
                .build();
    }

    /**
     * An object with which to key the stream before writing it out to the
     * PostgreSQL database. This is important only for cases in which the sequence
     * numbers need to be guaranteed to be in order.
     * 
     * @return The object with which to key the stream.
     */
    protected abstract KeySelector<E, ?> tableOrderingKeySelector();

    /**
     * Get the parallelism of this table. That is, the number of unique values
     * possible returned by {@link AbstractTable#tableOrderingKeySelector}.
     * 
     * @return The desired parallelism or -1 to be unbounded.
     */
    protected int tableParallelism() {
        return -1;
    }

    /**
     * Sink the given data stream to this tables PostgreSQL table.
     * 
     * @param stream
     *            The stream to sink.
     * @return A datastream that contains events with added sequence number and only
     *         outputs them after they have been committed in PostgreSQL.
     */
    protected DataStream<E> sinkToPostgres(DataStream<E> stream) {
        int tableP = tableParallelism();
        KeySelector<E, ?> keySelector = tableOrderingKeySelector();
        SingleOutputStreamOperator<E> sequencedStream = stream
                .keyBy(keySelector)
                .map(new SequenceAssigner<>())
                .uid("sequence-assigner-01-" + tableName)
                .name("Sequence Assigner");
        if (tableP != -1) {
            sequencedStream.setParallelism(tableP);
        }
        int parallelism = tableP == -1 ? env.getParallelism() : tableP;
        SingleOutputStreamOperator<List<E>> batchedStream = sequencedStream
                .keyBy(tableP != -1 && tableP <= parallelism
                        ? (row -> keySelector.getKey(row).hashCode())
                        : (row -> Math.abs(keySelector.getKey(row).hashCode() % parallelism)))
                .process(new CountAndTimeBatcher<>(1024, Duration.ofMillis(100), getOutputType()))
                .uid("event-batcher-01-" + tableName)
                .name("Event Batcher");
        if (tableP != -1) {
            batchedStream.setParallelism(tableP);
        }
        SingleOutputStreamOperator<E> committedStream = AsyncDataStream.orderedWait(
                batchedStream, buildJdbcSinkAndContinue(), 10, TimeUnit.MINUTES, 8)
                .returns(getOutputType())
                .uid("postgres-sink-01-" + tableName)
                .name("PostgreSQL Sink");
        if (tableP != -1) {
            committedStream.setParallelism(tableP);
        }
        return committedStream;
    }

    /**
     * Sink the given datastream to the Kafka topic for this table.
     * 
     * @param stream
     *            The stream to sink.
     * @throws ExecutionException
     *             If the Kafka client throes errors.
     * @throws InterruptedException
     *             If interrupted.
     */
    protected void sinkToKafka(DataStream<E> stream) throws ExecutionException, InterruptedException {
        KafkaUtil.setupTopic(tableName, bootstrapServers, numPartitions, replicationFactor, retentionMs);
        stream.sinkTo(buildKafkaSink())
                .uid("kafka-sink-01-" + tableName)
                .name("Kafka Sink");
    }

    /**
     * Build the computation for this table.
     * 
     * @throws ExecutionException
     *             If there is a issue writing to Kafka.
     * @throws InterruptedException
     *             If interrupted.
     */
    public void build() throws ExecutionException, InterruptedException {
        // We partition here by key so that all rows for the same key are
        // handled by the same subtask, ensuring timestamps are monotonic.
        DataStream<E> rawStream = computeTable();
        if (dryRun) {
            rawStream.print().setParallelism(1);
        } else {
            DataStream<E> committedStream;
            if (jdbcOptions != null) {
                committedStream = sinkToPostgres(rawStream);
            } else {
                committedStream = rawStream;
            }
            streams.put("[table]" + tableName, committedStream);
            if (bootstrapServers != null) {
                sinkToKafka(committedStream);
            }
        }
    }
}
