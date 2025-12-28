package com.rolandb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RankingTableTest {
    List<Table.Field> fields;
    RankingTable table;

    @BeforeEach
    public void setUp() {
        fields = List.of(new Table.Field("test_field", Table.FieldKind.INDEXED, String.class, 100L));
        table = new RankingTable("test_table", 1000L, 5L, fields);
    }

    @Test
    void testConstructor() {
        assertEquals("test_table", table.name);
        assertEquals(1000L, table.maxLimit);
        List<Table.Field> tableFields = table.fields;
        assertNotNull(tableFields);
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("test_field")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("row_number")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("rank")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("max_rank")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("old_row_number")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("old_rank")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("old_max_rank")));
    }

    @Test
    void testAsSqlQueryTableNameWithSmallCardinality() {
        List<TableRowFilter> filters = List.of();
        Subscription subscription = new Subscription(1L, "test_table", filters, null);
        String result = table.asSqlQueryTableName(subscription);
        assertEquals("test_table_point", result);
    }

    @Test
    void testAsSqlQueryTableNameWithLargeCardinality() {
        TableRowFilter filter = new TableRowFilter();
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        List<TableRowFilter> filters = List.of(filter);
        Subscription subscription = new Subscription(1L, "test_table", filters, null);
        String result = table.asSqlQueryTableName(subscription);
        assertEquals("test_table", result);
    }

    @Test
    void testAsSqlQueryTableNameWithNullCardinality() {
        Subscription subscription = new Subscription(1L, "test_table", null, null);
        String result = table.asSqlQueryTableName(subscription);
        assertEquals("test_table", result);
    }
}
