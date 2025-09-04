package com.rolandb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import io.reactivex.rxjava3.subjects.PublishSubject;

/**
 * In this application, a table consists of both a relational table in
 * PostgreSQL storing the latest state, and a Kafka topic that streams all of
 * the
 * changes to that table. This class provides an abstraction over that.
 */
public class Table {
    public static class TableField {
        public final String name;
        public final boolean isKey;
        public final Class<?> type;

        public TableField(String name, boolean isKey, Class<?> type) {
            this.name = name;
            this.isKey = isKey;
            this.type = type;
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);

    public final String name;
    public final List<TableField> fields;
    private Thread kafkaPollThread;
    private ConnectableObservable<Map<String, ?>> liveObservable;

    public Table(String name, List<TableField> fields) {
        this.name = name;
        this.fields = fields;
    }

    public void startLiveObservable(Properties kafkaProperties) {
        if (liveObservable == null) {
            PublishSubject<Map<String, ?>> subject = PublishSubject.create();
            kafkaPollThread = new Thread(() -> {
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
            kafkaPollThread.start();
            liveObservable = subject.replay(5, TimeUnit.SECONDS);
            liveObservable.connect();
        }
    }

    public void stopLiveObservable() throws InterruptedException {
        if (liveObservable != null) {
            liveObservable = null;
            kafkaPollThread.interrupt();
            kafkaPollThread.join();
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
        for (TableField field : fields) {
            if (!first) {
                builder.append(", ");
            }
            first = false;
            builder.append(field.name);
        }
        builder.append(" FROM ");
        builder.append(name);
        return builder.toString();
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
                        Connection connection = pool.getConnection();
                        connection.setAutoCommit(false);
                        return connection;
                    },
                    con -> Observable.create(emitter -> {
                        try (Statement st = con.createStatement(
                                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                            st.setFetchSize(1_000);
                            ResultSet rs = st
                                    .executeQuery(asSqlQuery() + " WHERE " + subscription.asSqlQueryCondition());
                            while (rs.next() && !emitter.isDisposed()) {
                                Map<String, Object> row = new HashMap<>();
                                for (TableField field : fields) {
                                    Object value = rs.getObject(field.name);
                                    if (value.getClass() == field.type) {
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
