package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable.TableEventKey;
import com.rolandb.AbstractUpdateTable.UpdateSeqRow;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Test;

public class AbstractUpdateTableTest {

    public static class TestUpdateRow extends UpdateSeqRow {
        @TableEventKey
        @JsonProperty("id")
        public Long id = 1L;

        @TableEventKey
        @JsonProperty("name")
        public String name = "test";

        @JsonProperty("value")
        public String value = "some_value";

        @JsonProperty("created_at")
        public Instant createdAt = Instant.now();
    }

    private static class ConcreteUpdateTable extends AbstractUpdateTable<TestUpdateRow> {
        @Override
        protected DataStream<TestUpdateRow> computeTable() {
            return null;
        }

        @Override
        protected Class<TestUpdateRow> getOutputType() {
            return TestUpdateRow.class;
        }

        @Override
        protected KeySelector<TestUpdateRow, ?> tableOrderingKeySelector() {
            return row -> row.id;
        }
    }

    @Test
    public void testUpdateSeqRowSeqFields() {
        TestUpdateRow row = new TestUpdateRow();
        row.seqNum = 100L;
        Map<String, Long> perFieldSeq = row.perFieldSeqNumbers();
        assertNotNull(perFieldSeq);
        assertEquals(2, perFieldSeq.size());
        assertEquals(100L, perFieldSeq.get("value_seq_num"));
        assertEquals(100L, perFieldSeq.get("created_at_seq_num"));
    }

    @Test
    public void testUpdateSeqRowSeqFieldsMissing() {
        TestUpdateRow row = new TestUpdateRow();
        row.seqNum = 100L;
        row.value = null;
        row.createdAt = null;
        Map<String, Long> perFieldSeq = row.perFieldSeqNumbers();
        assertNotNull(perFieldSeq);
        assertEquals(2, perFieldSeq.size());
        assertEquals(0L, perFieldSeq.get("value_seq_num"));
        assertEquals(0L, perFieldSeq.get("created_at_seq_num"));
    }

    @Test
    public void testUpdateSeqRowSeqNames() {
        List<String> names = UpdateSeqRow.perFieldSeqNumNames(TestUpdateRow.class);
        assertNotNull(names);
        assertEquals(2, names.size());
        assertTrue(names.contains("value_seq_num"));
        assertTrue(names.contains("created_at_seq_num"));
    }

    @Test
    public void testUpdateSeqRowValues() {
        TestUpdateRow row = new TestUpdateRow();
        row.seqNum = 100L;
        Object[] values = row.getValues();
        assertNotNull(values);
        assertEquals(7, values.length);
        assertEquals(row.id, values[0]);
        assertEquals(row.name, values[1]);
        assertEquals(row.value, values[2]);
        assertEquals(row.createdAt, values[3]);
        assertEquals(100L, values[4]);
        assertEquals(100L, values[5]);
        assertEquals(100L, values[6]);
    }

    @Test
    public void testBuildSqlConflictResolution() throws Exception {
        ConcreteUpdateTable table = new ConcreteUpdateTable();
        table.tableName = "test";
        StringBuilder builder = new StringBuilder();
        Method method = AbstractUpdateTable.class.getDeclaredMethod(
                "buildSqlConflictResolution", String.class, Field[].class, StringBuilder.class);
        method.setAccessible(true);
        method.invoke(table, "id, name", TestUpdateRow.class.getFields(), builder);
        String expectedSql = " ON CONFLICT (id, name) DO UPDATE SET "
                + "value = CASE WHEN EXCLUDED.value_seq_num > test.value_seq_num THEN EXCLUDED.value ELSE test.value END, "
                + "value_seq_num = GREATEST(EXCLUDED.value_seq_num,test.value_seq_num), "
                + "created_at = CASE WHEN EXCLUDED.created_at_seq_num > test.created_at_seq_num THEN EXCLUDED.created_at ELSE test.created_at END, "
                + "created_at_seq_num = GREATEST(EXCLUDED.created_at_seq_num,test.created_at_seq_num), "
                + "seq_num = GREATEST(EXCLUDED.seq_num,test.seq_num)";
        assertEquals(expectedSql, builder.toString());
    }

    @Test
    public void testBuildSqlExtraFields() throws Exception {
        ConcreteUpdateTable table = new ConcreteUpdateTable();
        StringBuilder builder = new StringBuilder();
        Method method = AbstractUpdateTable.class.getDeclaredMethod(
                "buildSqlExtraFields", Class.class, StringBuilder.class);
        method.setAccessible(true);
        method.invoke(table, TestUpdateRow.class, builder);
        assertEquals(", value_seq_num, created_at_seq_num", builder.toString());
    }

    @Test
    public void testBuildSqlExtraValues() throws Exception {
        ConcreteUpdateTable table = new ConcreteUpdateTable();
        StringBuilder builder = new StringBuilder();
        Method method = AbstractUpdateTable.class.getDeclaredMethod(
                "buildSqlExtraValues", Class.class, StringBuilder.class);
        method.setAccessible(true);
        method.invoke(table, TestUpdateRow.class, builder);
        assertEquals(", ?, ?", builder.toString());
    }
}
