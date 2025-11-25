package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DbConnectionPoolTest {
    private DbConnectionPool dbConnectionPool;

    @BeforeEach
    public void setUp() throws SQLException {
        dbConnectionPool = new DbConnectionPool("dummy");
    }

    @Test
    public void testGetConnectionNewConnection() throws SQLException {
        assertThrows(SQLException.class, () -> dbConnectionPool.getConnection());
    }

    @Test
    public void testGetConnection_ReusedConnection() throws SQLException {
        dbConnectionPool.returnConnection(null);
        assertThrows(NullPointerException.class, () -> dbConnectionPool.getConnection());
    }
}
