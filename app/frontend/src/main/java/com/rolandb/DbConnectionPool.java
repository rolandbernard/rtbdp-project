package com.rolandb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A small object that contains a number of connection. A connection will be
 * opened if one is requested but there are no free ones. Otherwise, one of the
 * existing open connections will be used.
 */
public class DbConnectionPool implements AutoCloseable {
    /** The URL used to connect to the database. */
    private final String jdbcUrl;
    /** The maximum number of concurrently open connections. */
    private final int maxConnections;
    /** The set of currently maintained active connections. */
    private final List<Connection> connections = new ArrayList<>();
    /** The current number of open connections. */
    private int openConnections = 0;

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
    public DbConnectionPool(String jdbcUrl, int maxConnections) {
        this.jdbcUrl = jdbcUrl;
        this.maxConnections = maxConnections;
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
    public synchronized Connection getConnection() throws SQLException, InterruptedException {
        while (true) {
            if (connections.isEmpty()) {
                if (openConnections < maxConnections) {
                    Connection newConnection = DriverManager.getConnection(jdbcUrl);
                    newConnection.setReadOnly(true);
                    newConnection.setAutoCommit(false);
                    openConnections++;
                    return newConnection;
                } else {
                    // Retry when someone has returned a connection.
                    this.wait();
                }
            } else {
                Connection connection = connections.remove(connections.size() - 1);
                try {
                    if (connection.isValid(5)) {
                        return connection;
                    }
                } catch (SQLException ex) {
                    // This connection is no longer valid. We should get another one.
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        // Ignore the error. We are closing the connection new.
                    }
                }
                openConnections--;
            }
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
    public synchronized void returnConnection(Connection connection) {
        connections.add(connection);
        this.notify();
    }

    @Override
    public synchronized void close() throws SQLException {
        for (Connection connection : connections) {
            connection.close();
        }
        connections.clear();
    }
}
