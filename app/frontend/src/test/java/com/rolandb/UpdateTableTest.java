package com.rolandb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UpdateTableTest {

    @Test
    void testConstructor() {
        List<Table.Field> fields = List.of(
                new Table.Field("id", Table.FieldKind.KEY, Long.class, 1L),
                new Table.Field("name", Table.FieldKind.INDEXED, String.class, 100L),
                new Table.Field("description", String.class),
                new Table.Field("seq_num", Long.class));
        UpdateTable table = new UpdateTable("test_table", 1000L, fields);
        assertEquals("test_table", table.name);
        assertEquals(1000L, table.maxLimit);
        List<Table.Field> tableFields = table.fields;
        assertNotNull(tableFields);
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("id")));
        assertFalse(tableFields.stream().anyMatch(f -> f.name.equals("id_seq_num")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("name")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("name_seq_num")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("description")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("description_seq_num")));
        assertTrue(tableFields.stream().anyMatch(f -> f.name.equals("seq_num")));
        assertFalse(tableFields.stream().anyMatch(f -> f.name.equals("seq_num_seq_num")));
    }
}
