package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable.TableBuilder;
import com.rolandb.AbstractTable.TableEventKey;

import java.lang.reflect.Method;
import java.time.Instant;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class AbstractTableTest {
    public static class TestRow extends SequencedRow {
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

    public static class ConcreteTable extends AbstractTable<TestRow> {
        @Override
        protected DataStream<TestRow> computeTable() {
            return null;
        }

        @Override
        protected Class<TestRow> getOutputType() {
            return TestRow.class;
        }
    }

    @Test
    public void testTableBuilder() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://localhost:5432/db").withDriverName("org.postgresql.Driver").build();
        TableBuilder builder = new TableBuilder()
                .setEnv(env)
                .addStream("stream1", "dummy_stream_obj")
                .setJdbcOptions(jdbcOptions)
                .setBootstrapServers("kafka:9092")
                .setDryRun(true)
                .setNumPartitions(3)
                .setReplicationFactor(2)
                .setRetentionMs(86400000L);
        ConcreteTable table = builder.get("my_table", ConcreteTable.class);
        assertNotNull(table);
        assertEquals(env, table.env);
        assertEquals(1, table.streams.size());
        assertEquals("dummy_stream_obj", table.streams.get("stream1"));
        assertEquals(jdbcOptions, table.jdbcOptions);
        assertEquals("kafka:9092", table.bootstrapServers);
        assertTrue(table.dryRun);
        assertEquals(3, table.numPartitions);
        assertEquals(2, table.replicationFactor);
        assertEquals(86400000L, table.retentionMs);
        assertEquals("my_table", table.tableName);
    }

    @Test
    public void testBuildJdbcSinkStatement() {
        ConcreteTable table = new ConcreteTable();
        table.tableName = "test_table";
        String expectedSql = "INSERT INTO test_table (id, name, value, created_at, seq_num) "
                + "VALUES (?, ?, ?, ?, ?) ON CONFLICT (id, name) DO UPDATE "
                + "SET value = EXCLUDED.value, created_at = EXCLUDED.created_at, seq_num = EXCLUDED.seq_num "
                + "WHERE test_table.seq_num < EXCLUDED.seq_num";
        assertEquals(expectedSql, table.buildJdbcSinkStatement(TestRow.class));
    }

    @Test
    public void testRemoveInvalidCodePoints() throws Exception {
        Method method = AbstractTable.class.getDeclaredMethod("removeInvalidCodePoints", String.class);
        method.setAccessible(true);
        assertEquals("hello world", method.invoke(null, "hello world"));
        assertEquals("hello", method.invoke(null, "hello\u0000"));
        assertEquals("test", method.invoke(null, "test\uD800"));
        assertEquals("test\uD800\uDC00", method.invoke(null, "test\uD800\uDC00"));
        assertEquals("valid", method.invoke(null, "valid"));
        assertEquals("valid äöü \u1F600", method.invoke(null, "valid äöü \u1F600"));
        assertEquals("valid \u1F9D1\u200D\u1F9AF\u200D\u27A1\uFE0F",
                method.invoke(null, "valid \u1F9D1\u200D\u1F9AF\u200D\u27A1\uFE0F"));
        assertEquals("", method.invoke(null, "\u0000\uD800"));
    }
}
