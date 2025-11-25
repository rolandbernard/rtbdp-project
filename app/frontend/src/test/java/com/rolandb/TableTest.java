package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rolandb.Table.Field;
import com.rolandb.Table.FieldKind;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TableTest {

    @Test
    public void testTableConstructor() {
        List<Field> fields = Arrays.asList(new Field("id", Long.class));
        Table table = new Table("test_table", 100L, fields);
        assertEquals("test_table", table.name);
        assertEquals(100L, table.maxLimit);
        assertEquals(fields, table.fields);
    }

    @Test
    public void testFieldConstructorAndGetters() {
        Field field1 = new Field("id", FieldKind.KEY, Long.class, 100L, true);
        assertEquals("id", field1.name);
        assertEquals(FieldKind.KEY, field1.kind);
        assertEquals(Long.class, field1.type);
        assertEquals(100L, field1.cardinality);
        assertTrue(field1.inReplay);
        assertTrue(field1.canFilter());
        assertTrue(field1.isKey());

        Field field2 = new Field("name", String.class);
        assertEquals("name", field2.name);
        assertEquals(FieldKind.NORMAL, field2.kind);
        assertEquals(String.class, field2.type);
        assertEquals(null, field2.cardinality);
        assertTrue(field2.inReplay);
        assertFalse(field2.canFilter());
        assertFalse(field2.isKey());
    }

    @Test
    public void testAsSqlQueryTableName() {
        Table table = new Table("my_table", null, Collections.emptyList());
        assertEquals("my_table", table.asSqlQueryTableName(null));
    }

    @Test
    public void testAsSqlQuery() {
        List<Field> fields = Arrays.asList(
                new Field("id", FieldKind.KEY, Long.class, null, true),
                new Field("name", FieldKind.NORMAL, String.class, null, true),
                new Field("hidden", FieldKind.NORMAL, Boolean.class, null, false));
        Table table = new Table("my_table", null, fields);
        String expectedQuery = "SELECT id, name FROM my_table";
        assertEquals(expectedQuery, table.asSqlQuery(null));
    }

    @Test
    public void testAsSqlQueryOrder() {
        List<Field> fields = Arrays.asList(
                new Field("id", FieldKind.KEY, Long.class, null),
                new Field("name", FieldKind.NORMAL, String.class, null));
        Table table = new Table("my_table", null, fields);
        assertEquals("", table.asSqlQueryOrder());

        fields = Arrays.asList(
                new Field("timestamp", FieldKind.SORTED_KEY, Long.class, null),
                new Field("id", FieldKind.SORTED_KEY, Long.class, null),
                new Field("name", FieldKind.NORMAL, String.class, null));
        table = new Table("my_table", null, fields);
        assertEquals(" ORDER BY timestamp DESC, id DESC ", table.asSqlQueryOrder());
    }
}
