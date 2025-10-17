package com.rolandb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;

/**
 * In this application, a table consists of both a relational table in
 * PostgreSQL storing the latest state, and a Kafka topic that streams all of
 * the
 * changes to that table. This class provides an abstraction over that.
 */
public class Table {
    public static enum FieldKind {
        NORMAL, INDEXED, KEY, SORTED_KEY
    }

    public static class Field {
        public final String name;
        public final FieldKind kind;
        public final Class<?> type;
        public final Long cardinality;
        public final boolean inReplay;

        public Field(String name, FieldKind kind, Class<?> type, Long cardinality, boolean inReplay) {
            this.name = name;
            this.kind = kind;
            this.type = type;
            this.cardinality = cardinality;
            this.inReplay = inReplay;
        }

        public Field(String name, FieldKind kind, Class<?> type, Long cardinality) {
            this(name, kind, type, cardinality, true);
        }

        public Field(String name, Class<?> type) {
            this(name, FieldKind.NORMAL, type, null, true);
        }

        public Field(String name, Class<?> type, boolean inReplay) {
            this(name, FieldKind.NORMAL, type, null, inReplay);
        }

        public boolean canFilter() {
            return kind == FieldKind.INDEXED || kind == FieldKind.KEY || kind == FieldKind.SORTED_KEY;
        }

        public boolean isKey() {
            return kind == FieldKind.KEY || kind == FieldKind.SORTED_KEY;
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    public final String name;
    public final Long maxLimit;
    public final List<Field> fields;
    private Thread livePollThread;
    private Observable<Map<String, ?>> liveObservable;

    public Table(String name, Long maxLimit, List<Field> fields) {
        this.name = name;
        this.maxLimit = maxLimit;
        this.fields = fields;
    }

    public void startLiveObservable(Properties kafkaProperties) {
        if (liveObservable == null) {
            PublishSubject<Map<String, ?>> subject = PublishSubject.create();
            livePollThread = new Thread(() -> {
                try {
                    KafkaUtil.waitForTopics(kafkaProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), name);
                } catch (InterruptedException | ExecutionException e) {
                    throw new IllegalStateException("unable to connect to Kafka topics", e);
                }
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
                try {
                    consumer.subscribe(Collections.singletonList(name));
                    while (!Thread.interrupted()) {
                        consumer.poll(java.time.Duration.ofMillis(100))
                                .forEach(record -> {
                                    try {
                                        Map<String, ?> row = objectMapper.readValue(record.value(),
                                                new TypeReference<Map<String, ?>>() {
                                                });
                                        subject.onNext(row);
                                    } catch (JsonProcessingException e) {
                                        LOGGER.error("Failed to parse Kafka message", e);
                                    }
                                });
                    }
                } catch (InterruptException e) {
                    // Just close normally after resetting the interrupt.
                    Thread.interrupted();
                } finally {
                    consumer.unsubscribe();
                    consumer.close();
                    subject.onComplete();
                }
            });
            livePollThread.start();
            liveObservable = subject.share();
        }
    }

    public void stopLiveObservable() throws InterruptedException {
        if (liveObservable != null) {
            liveObservable = null;
            livePollThread.interrupt();
            livePollThread.join();
        }
    }

    /**
     * Get the observable for the live event updates. Note that this may return
     * {@code null} in the case that the live observable has not been created
     * yet.
     *
     * @return A hot observable that emits new rows as soon as they are received
     *         from the Kafka topic.
     */
    public Observable<Map<String, ?>> getLiveObservable() {
        return liveObservable;
    }

    /**
     * Get the SQL query expression that would load the complete table, with all
     * of its fields. The query consists of the `SELECT` and `FROM` parts, but
     * does not include any other clause, so a `WHERE` could be added to it.
     *
     * @return The SQL query expression.
     */
    public String asSqlQuery() {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        boolean first = true;
        for (Field field : fields) {
            if (field.inReplay) {
                if (!first) {
                    builder.append(", ");
                }
                first = false;
                builder.append(field.name);
            }
        }
        builder.append(" FROM ");
        builder.append(name);
        return builder.toString();
    }

    /**
     * Get the SQL query order expression to use for this table. This is only
     * used if the client asked for a limited set of results.
     *
     * @return The SQL order expression.
     */
    public String asSqlQueryOrder() {
        if (fields.stream().anyMatch(f -> f.kind == FieldKind.SORTED_KEY)) {
            StringBuilder builder = new StringBuilder();
            builder.append(" ORDER BY ");
            boolean first = true;
            for (Field field : fields) {
                if (field.kind == FieldKind.SORTED_KEY) {
                    if (!first) {
                        builder.append(", ");
                    }
                    first = false;
                    builder.append(field.name);
                    builder.append(" DESC");
                }
            }
            builder.append(" ");
            return builder.toString();
        } else {
            return "";
        }
    }

    /**
     * Get an observable that is reading the database table with the given
     * connection. This can be used to replay the events for the current
     * snapshot in the database.
     *
     * @param subscription
     *            The subscription determining which rows to read.
     * @param pool
     *            The connection pool to use for connecting to the db.
     * @return A cold observable that reads the selected table rows table.
     */
    public Observable<Map<String, ?>> getReplayObservable(Subscription subscription, DbConnectionPool pool) {
        if (fields.isEmpty()) {
            return Observable.empty();
        } else {
            return Observable.using(
                    () -> {
                        return pool.getConnection();
                    },
                    con -> Observable.create(emitter -> {
                        try (Statement st = con.createStatement(
                                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                            st.setFetchSize(1_000);
                            String query = asSqlQuery() + " WHERE " + subscription.asSqlQueryCondition();
                            if (subscription.isSorted()) {
                                query += asSqlQueryOrder() + subscription.asSqlQueryLimit();
                            }
                            ResultSet rs = st
                                    .executeQuery(query);
                            while (rs.next() && !emitter.isDisposed()) {
                                Map<String, Object> row = new HashMap<>();
                                for (Field field : fields) {
                                    if (field.inReplay) {
                                        Object value = rs.getObject(field.name);
                                        if (value == null || value.getClass() == field.type) {
                                            row.put(field.name, value);
                                        } else if (value instanceof Integer && field.type == Long.class) {
                                            row.put(field.name, Long.valueOf((int) value));
                                        } else if (value instanceof Float && field.type == Double.class) {
                                            row.put(field.name, Double.valueOf((float) value));
                                        } else if (value instanceof Timestamp && field.type == String.class) {
                                            row.put(field.name, ((Timestamp) value).toInstant().toString());
                                        } else {
                                            throw new IllegalArgumentException("Unsupported conversion from "
                                                    + value.getClass() + " to " + field.type);
                                        }
                                    }
                                }
                                emitter.onNext(row);
                            }
                            rs.close(); // Close the result set
                            emitter.onComplete();
                        } catch (SQLException e) {
                            emitter.onError(e);
                        }
                    }),
                    con -> {
                        pool.returnConnection(con);
                    });
        }
    }
}
