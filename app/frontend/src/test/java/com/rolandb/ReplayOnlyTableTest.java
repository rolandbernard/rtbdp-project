package com.rolandb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ReplayOnlyTableTest {
    List<Table.Field> fields;
    ReplayOnlyTable table;

    @BeforeEach
    public void setUp() {
        fields = List.of(new Table.Field("test_field", Table.FieldKind.INDEXED, String.class, 100L));
        table = new ReplayOnlyTable("test_table", 1000L, fields);
    }

    @Test
    void testConstructor() {
        assertEquals("test_table", table.name);
        assertEquals(1000L, table.maxLimit);
        assertEquals(fields, table.fields);
    }

    @Test
    void testStartLiveObservable() {
        Properties props = new Properties();
        assertDoesNotThrow(() -> table.startLiveObservable(props));
    }

    @Test
    void testStopLiveObservable() {
        assertDoesNotThrow(() -> table.stopLiveObservable());
    }

    @Test
    void testGetLiveObservable() {
        assertNotNull(table.getLiveObservable());
        List<Map<String, ?>> result = table.getLiveObservable().toList().blockingGet();
        assertTrue(result.isEmpty());
    }
}
