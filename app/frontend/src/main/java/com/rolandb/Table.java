package com.rolandb;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import io.reactivex.rxjava3.core.Observable;

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
        public final boolean isNumeric;

        public TableField(String name, boolean isKey, boolean isNumeric) {
            this.name = name;
            this.isKey = isKey;
            this.isNumeric = isNumeric;
        }
    };

    public final String name;
    public final List<TableField> fields;
    private final boolean onlyLive;
    private Observable<Map<String, ?>> liveObservable;

    public Table(String name, List<TableField> fields, boolean onlyLive) {
        this.name = name;
        this.fields = fields;
        this.onlyLive = onlyLive;
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
        if (onlyLive) {
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
                             ResultSet.TYPE_FORWARD_ONLY,
                             ResultSet.CONCUR_READ_ONLY)) {

                            st.setFetchSize(100); // Set a small fetch size
                            ResultSet rs = st.executeQuery(SQL_QUERY);

                            while (rs.next() && !emitter.isDisposed()) {
                                long id = rs.getLong("id");
                                String name = rs.getString("name");
                                emitter.onNext(new YourObject(id, name));
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
