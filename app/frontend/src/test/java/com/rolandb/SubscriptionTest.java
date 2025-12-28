package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SubscriptionTest {
    private static ObjectMapper objectMapper = new ObjectMapper();
    List<Table.Field> fields;
    RankingTable table;

    @BeforeEach
    public void setUp() {
        fields = List.of(new Table.Field("id", Table.FieldKind.KEY, String.class, 100L));
        table = new RankingTable("test", 1000L, 5L, fields);
    }

    @Test
    public void testJsonCreator() throws IOException {
        String json = "{\"id\": 123, \"table\": \"events\", \"filters\": [], \"limit\": 10}";
        Subscription subscription = objectMapper.readValue(json, Subscription.class);
        assertEquals(123L, subscription.id);
        assertEquals("events", subscription.tableName);
        assertNotNull(subscription.filters);
        assertTrue(subscription.filters.isEmpty());
        assertEquals(10L, subscription.limit);
    }

    @Test
    public void testJsonCreatorEverythingMissing() throws IOException {
        String json = "{\"id\": 123}";
        Subscription subscription = objectMapper.readValue(json, Subscription.class);
        assertEquals(123L, subscription.id);
        assertEquals("", subscription.tableName);
        assertNull(subscription.filters);
        assertNull(subscription.limit);
    }

    @Test
    public void testUsesOnlyKey() {
        TableRowFilter filter = new TableRowFilter();
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        Subscription subscription = new Subscription(1, "test", List.of(filter), null);
        assertFalse(subscription.usesOnlyKey(table));

        filter = new TableRowFilter();
        filter.setKeyFilters("id", new TableValueFilter<Long>(10L, 110L, null, false, null));
        subscription = new Subscription(1, "test", List.of(filter), null);
        assertTrue(subscription.usesOnlyKey(table));

        subscription = new Subscription(3, "my_table", null, null);
        assertTrue(subscription.usesOnlyKey(table));
    }

    @Test
    public void testEstimateCardinality() {
        TableRowFilter filter = new TableRowFilter();
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        Subscription subscription = new Subscription(1, "test", List.of(filter), null);
        assertEquals(500, subscription.estimateCardinality(table));

        filter = new TableRowFilter();
        filter.setKeyFilters("id", new TableValueFilter<Long>(null, null, null, false, List.of(1L)));
        subscription = new Subscription(1, "test", List.of(filter), null);
        assertEquals(100, subscription.estimateCardinality(table));

        subscription = new Subscription(3, "my_table", null, null);
        assertEquals(null, subscription.estimateCardinality(table));
    }

    @Test
    public void testApplicableTo() {
        TableRowFilter filter = new TableRowFilter();
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        Subscription subscription = new Subscription(1, "test", List.of(filter), null);
        assertTrue(subscription.applicableTo(table, true));
        assertTrue(subscription.applicableTo(table, false));

        filter = new TableRowFilter();
        filter.setKeyFilters("id", new TableValueFilter<String>(null, null, null, false, List.of("abc")));
        subscription = new Subscription(1, "test", List.of(filter), null);
        assertTrue(subscription.applicableTo(table, true));
        assertTrue(subscription.applicableTo(table, false));

        filter = new TableRowFilter();
        filter.setKeyFilters("id", new TableValueFilter<Long>(null, null, null, false, List.of(1L)));
        subscription = new Subscription(1, "test", List.of(filter), null);
        assertFalse(subscription.applicableTo(table, true));
        assertFalse(subscription.applicableTo(table, false));

        subscription = new Subscription(3, "my_table", null, null);
        assertFalse(subscription.applicableTo(table, true));
        assertTrue(subscription.applicableTo(table, false));
    }

    @Test
    public void testIsSorted() {
        Subscription subscription = new Subscription(1, "tbl", null, 10L);
        assertTrue(subscription.isSorted());

        subscription = new Subscription(2, "tbl", null, null);
        assertFalse(subscription.isSorted());
    }

    @Test
    public void testAccept() {
        TableRowFilter filter = new TableRowFilter();
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        Subscription subscription = new Subscription(1, "test", List.of(filter), null);
        assertFalse(subscription.accept(Map.of("row_number", 5L)));
        assertTrue(subscription.accept(Map.of("row_number", 50L)));
        assertFalse(subscription.accept(Map.of("row_number", 150L)));
    }

    @Test
    public void testAsSqlQueryCondition() {
        TableRowFilter filter = new TableRowFilter();
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        Subscription subscription = new Subscription(1, "test", List.of(filter), null);
        assertEquals("(((row_number >= 10 AND row_number < 110)))", subscription.asSqlQueryCondition());

        filter = new TableRowFilter();
        filter.setKeyFilters("id", new TableValueFilter<String>(null, null, null, false, List.of("abc")));
        subscription = new Subscription(1, "test", List.of(filter), null);
        assertEquals("((((id IN ('abc')))))", subscription.asSqlQueryCondition());

        filter = new TableRowFilter();
        filter.setKeyFilters("id", new TableValueFilter<String>(null, null, null, false, List.of("abc")));
        filter.setKeyFilters("row_number", new TableValueFilter<Long>(10L, 110L, null, false, null));
        TableRowFilter filter2 = new TableRowFilter();
        filter2.setKeyFilters("id", new TableValueFilter<String>(null, null, null, false, List.of("cde")));
        subscription = new Subscription(1, "test", List.of(filter, filter2), null);
        assertEquals(
                "((((id IN ('abc'))) AND (row_number >= 10 AND row_number < 110)) OR (((id IN ('cde')))))",
                subscription.asSqlQueryCondition());
    }

    @Test
    public void testAsSqlQueryLimit() {
        Subscription subscription = new Subscription(1, "tbl", null, 100L);
        assertEquals(" LIMIT 100", subscription.asSqlQueryLimit());

        subscription = new Subscription(2, "tbl", null, null);
        assertEquals("", subscription.asSqlQueryLimit());
    }
}
