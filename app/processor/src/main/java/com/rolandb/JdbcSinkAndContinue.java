package com.rolandb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a custom JDBC sink that in after committing the events into the
 * database emits them again so that they can be processed further. It expects
 * events to come pre-batched and then emits them individually.
 * 
 * @param <E>
 *            The type of event used in the stream.
 */
public class JdbcSinkAndContinue<E extends SequencedRow> extends RichAsyncFunction<List<E>, E> {
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
    /** The JDBC connection options to use for connecting to the db. */
    private final JdbcConnectionOptions jdbcOptions;
    /** The SQL statement to use for insertion. */
    private final String sqlInsert;
    /** A function with which to populate the prepared statement. */
    private final StatementFunction<E> stmtFunction;

    /** The connection poll we get connection from. */
    private transient DbConnectionPool connectionPool;
    /** The executor on which we run the database requests. */
    private transient ExecutorService executor;

    /**
     * Create a new instance of the sink.
     * 
     * @param jdbcOptions
     *            JDBC connection options.
     * @param retries
     *            The number of reties in case of transient database connection
     *            issues.
     * @param sqlInsert
     *            The SQL statement to use for inserting into the database.
     * @param stmtFunction
     *            A function that populates a prepared statement created based on
     *            the query in {@code sqlInsert}:
     */
    public JdbcSinkAndContinue(
            JdbcConnectionOptions jdbcOptions, int retries, String sqlInsert, StatementFunction<E> stmtFunction) {
        this.jdbcOptions = jdbcOptions;
        this.retires = retries;
        this.sqlInsert = sqlInsert;
        this.stmtFunction = stmtFunction;
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        connectionPool = DbConnectionPool.getGlobalInstance(jdbcOptions);
        connectionPool.upRef();
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void close() {
        connectionPool.downRef();
        executor.shutdown();
    }

    private void commitBuffer(List<E> events) throws SQLException, InterruptedException {
        for (int r = 0;; r++) {
            try {
                Connection connection = connectionPool.getConnection();
                try {
                    connection.beginRequest();
                    try (PreparedStatement ps = connection.prepareStatement(sqlInsert)) {
                        for (E event : events) {
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
                    return;
                } catch (SQLException exc) {
                    try {
                        // Close connection to avoid related errors. The connection pool knows how to
                        // handle closed connections.
                        connection.close();
                    } catch (SQLException e) {
                        // Ignore the error. Something else is wrong here.
                    }
                    throw exc;
                } finally {
                    connectionPool.returnConnection(connection);
                }
            } catch (SQLException exc) {
                if (r >= retires) {
                    throw new SQLException("Failed to insert elements into db", exc);
                } else {
                    LOGGER.error("Failed to insert elements into db", exc);
                }
            }
            // Try again in case of transient errors, e.g. lock failures.
            Thread.sleep(1000);
        }
    }

    @Override
    public void asyncInvoke(List<E> input, ResultFuture<E> resultFuture) throws Exception {
        CompletableFuture.runAsync(() -> {
            try {
                commitBuffer(input);
                resultFuture.complete(input);
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        }, executor);
    }
}
