package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.rolandb.AbstractTable.TableEventKey;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

public class KafkaTimedRowSerializerTest {

    public static class TestKafkaRow extends SequencedRow {
        @TableEventKey
        public Long id = 123L;
        public String name = "test";
        public Instant timestamp = Instant.parse("2023-01-01T10:00:00Z");
    }

    @Test
    public void testConstructor() {
        KafkaTimedRowSerializer<TestKafkaRow> serializer = new KafkaTimedRowSerializer<>("test_topic");
        assertNotNull(serializer);
    }

    @Test
    public void testSerialize() throws Exception {
        KafkaTimedRowSerializer<TestKafkaRow> serializer = new KafkaTimedRowSerializer<>("test_topic");
        serializer.open(null, null);
        TestKafkaRow row = new TestKafkaRow();
        row.seqNum = 1L;
        ProducerRecord<byte[], byte[]> record = serializer.serialize(row, null, null);
        assertNotNull(record);
        assertEquals("test_topic", record.topic());
        assertEquals("[123]", new String(record.key(), StandardCharsets.UTF_8));
        String expectedValue = "{\"id\":123,\"name\":\"test\",\"timestamp\":\"2023-01-01T10:00:00Z\",\"seq_num\":1}";
        assertEquals(expectedValue, new String(record.value(), StandardCharsets.UTF_8));
    }
}
