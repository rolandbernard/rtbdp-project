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
public class DbConnectionPool {
    private final String jdbcUrl;
    private final List<Connection> connections = new ArrayList<>();

    public DbConnectionPool(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Get a new or reused connection from the connection pool.
     * 
     * @return The connection.
     * @throws SQLException
     */
    public synchronized Connection getConnection() throws SQLException {
        if (connections.isEmpty()) {
            return DriverManager.getConnection(jdbcUrl);
        } else {
            return connections.remove(connections.size() - 1);
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
    }
}
