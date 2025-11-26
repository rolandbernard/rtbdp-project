package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable.TableEventKey;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SequencedRowTest {

    private static class TestRow extends SequencedRow {
        @TableEventKey
        @JsonProperty("id")
        public Long id = 123L;
        @TableEventKey
        @JsonProperty("name")
        public String name = "test";
        @JsonProperty("value")
        public Integer value = 456;
    }

    @Test
    public void testGetField() throws NoSuchFieldException {
        TestRow row = new TestRow();
        assertEquals(123L, row.getField(TestRow.class.getField("id")));
        assertEquals("test", row.getField(TestRow.class.getField("name")));
        assertEquals(456, row.getField(TestRow.class.getField("value")));
    }

    @Test
    public void testGetKey() {
        TestRow row = new TestRow();
        List<?> key = row.getKey();
        assertEquals(2, key.size());
        assertEquals(123L, key.get(0));
        assertEquals("test", key.get(1));
    }

    @Test
    public void testReadKeyFrom() {
        TestRow row = new TestRow();
        List<?> key = SequencedRow.readKeyFrom(row);
        assertEquals(2, key.size());
        assertEquals(123L, key.get(0));
        assertEquals("test", key.get(1));
    }

    @Test
    public void testGetValues() {
        TestRow row = new TestRow();
        row.seqNum = 100L;
        Object[] values = row.getValues();
        assertEquals(4, values.length);
        assertEquals(123L, values[0]);
        assertEquals("test", values[1]);
        assertEquals(456, values[2]);
        assertEquals(100L, values[3]);
    }

    @Test
    public void testReadValuesFrom() {
        TestRow row = new TestRow();
        row.seqNum = 100L;
        Object[] values = SequencedRow.readValuesFrom(row);
        assertEquals(4, values.length);
        assertEquals(123L, values[0]);
        assertEquals("test", values[1]);
        assertEquals(456, values[2]);
        assertEquals(100L, values[3]);
    }

    @Test
    public void testHasKeyIn() {
        assertTrue(SequencedRow.hasKeyIn(TestRow.class));
        assertFalse(SequencedRow.hasKeyIn(SequencedRow.class));
    }
}
