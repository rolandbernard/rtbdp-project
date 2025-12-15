package com.rolandb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small object that contains a number of connection. A connection will be
 * opened if one is requested but there are no free ones. Otherwise, one of the
 * existing open connections will be used.
 */
public class DbConnectionPool implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbConnectionPool.class);

    /** This is a global instance of the connection pool. */
    private static DbConnectionPool globalInstance;

    /** The URL used to connect to the database. */
    private final String jdbcUrl;
    /** The set of currently maintained active connections. */
    private final List<Connection> allConnections = new ArrayList<>();
    /** Available connections that are not currently given out. */
    private final List<Connection> connections = new ArrayList<>();
    /** Semaphore with the number of allowed connections as permits. */
    private Semaphore connectionSemaphore;
    /** The number of references to the DbConnectionPool. */
    private int references = 0;

    /**
     * Create a new connection pool that will create new connection by connecting to
     * the given JDBC URL.
     * 
     * @param jdbcUrl
     *            The URL to connect to for opening new connections.
     * @param maxConnections
     *            The maximum number of concurrent connections allowed by this
     *            connection pool.
     */
    private DbConnectionPool(String jdbcUrl, int maxConnections) {
        this.jdbcUrl = jdbcUrl;
        this.connectionSemaphore = new Semaphore(maxConnections, true);
    }

    /**
     * Get the global instance of the connection pool, creating it in case one does
     * not already exist using the given parameters, and checking that the
     * parameters are the same in the case one already exists.
     * 
     * @param jdbcUrl
     *            The JDBC URL to connect with to the database.
     * @param maxConnections
     *            The maximum number of allowed concurrent connections.
     * @return The global connection pool.
     */
    public static synchronized DbConnectionPool getGlobalInstance(String jdbcUrl, int maxConnections) {
        if (globalInstance != null && !globalInstance.jdbcUrl.equals(jdbcUrl)) {
            throw new IllegalArgumentException("Conflicting parameters for global connection pool.");
        }
        if (globalInstance == null) {
            globalInstance = new DbConnectionPool(jdbcUrl, maxConnections);
        }
        return globalInstance;
    }

    /**
     * Like {@link DbConnectionPool#getGlobalInstance(String, int)} but taking the
     * options not the URL and with the default number of concurrent connections.
     * 
     * @param jdbcOption
     *            The JDBC connection options to connect with.
     * @return The global connection pool.
     */
    public static DbConnectionPool getGlobalInstance(JdbcConnectionOptions jdbcOption) {
        Properties props = jdbcOption.getProperties();
        StringBuffer fullUrl = new StringBuffer(jdbcOption.getDbURL());
        boolean first = true;
        for (String name : props.stringPropertyNames().stream().sorted().toList()) {
            if (first) {
                fullUrl.append("?");
                first = false;
            } else {
                fullUrl.append("&");
            }
            fullUrl.append(name);
            fullUrl.append("=");
            fullUrl.append(props.getProperty(name));
        }
        return getGlobalInstance(fullUrl.toString(), 32);
    }

    /**
     * Get a new or reused connection from the connection pool.
     *
     * @return The connection.
     * @throws SQLException
     *             In case we are unable to open a connection.
     * @throws InterruptedException
     *             If interrupted.
     */
    public Connection getConnection() throws SQLException, InterruptedException {
        connectionSemaphore.acquire();
        try {
            synchronized (connections) {
                while (true) {
                    if (connections.isEmpty()) {
                        Connection newConnection = DriverManager.getConnection(jdbcUrl);
                        allConnections.add(newConnection);
                        newConnection.setAutoCommit(false);
                        return newConnection;
                    } else {
                        Connection connection = connections.remove(connections.size() - 1);
                        try {
                            if (connection.isValid(5)) {
                                return connection;
                            }
                        } catch (SQLException ex) {
                            // Close the connection since it is seemingly broken.
                        }
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            // Ignore the error. We are closing the connection new.
                            LOGGER.warn("Failed to close invalid connection", e);
                        }
                        allConnections.remove(connection);
                    }
                }
            }
        } catch (Exception e) {
            connectionSemaphore.release();
            throw e;
        }
    }

    /**
     * Return a borrowed connection to the connection pool. The connection does
     * not necessarily have to have been created with {@code getConnection()},
     * but this is normally expected.
     *
     * @param connection
     *            The connection to return.
     */
    public void returnConnection(Connection connection) {
        synchronized (connections) {
            if (allConnections.contains(connection)) {
                connections.add(connection);
            } else {
                LOGGER.error("Trying to return a connection not originating from this pool");
            }
        }
        connectionSemaphore.release();
    }

    public synchronized void upRef() {
        references++;
    }

    public synchronized void downRef() {
        references--;
        if (references == 0) {
            try {
                close();
            } catch (SQLException e) {
                LOGGER.warn("Failed to close db connection pool", e);
            }
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        synchronized (connections) {
            for (Connection connection : allConnections) {
                connection.close();
            }
            allConnections.clear();
            connections.clear();
        }
    }
}
