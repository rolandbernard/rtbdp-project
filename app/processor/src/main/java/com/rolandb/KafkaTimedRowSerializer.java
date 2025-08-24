package com.rolandb;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * This is a serializer for the KafkaSink. We can't build this from the default
 * {@link KafkaRecordSerializationSchemaBuilder} because we don't actually want
 * to use the event time as the creation time in the Kafka topic. Doing so would
 * cause the dummy data to be deleted (or ready for deletion) immediately, as
 * that data has timestamps from the past.
 */
public class KafkaTimedRowSerializer implements KafkaRecordSerializationSchema<TimedRow> {
    private final String tableName;
    private final List<String> columnNames;
    private final List<String> keyNames;
    private final ObjectMapper objectMapper;

    public KafkaTimedRowSerializer(String tableName, List<String> columnNames, List<String> keyNames) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.keyNames = keyNames;
        objectMapper = new ObjectMapper();
    }

    @Override
    public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(TimedRow row, KafkaSinkContext context, Long timestamp) {
        try {
            // Set as key the combination of key columns. This ensures in-order
            // delivery per-key, ensuring timestamps are monotonic.
            ArrayNode keyNode = objectMapper.createArrayNode();
            for (String column : keyNames) {
                keyNode.add(objectMapper.valueToTree(row.getFieldAs(column)));
            }
            byte[] key = objectMapper.writeValueAsBytes(keyNode);
            // We serialize each row as key-value pairs.
            ObjectNode valueNode = objectMapper.createObjectNode();
            for (String column : columnNames) {
                valueNode.set(column, objectMapper.valueToTree(row.getFieldAs(column)));
            }
            valueNode.set("tz_write", objectMapper.valueToTree(row.getTime()));
            byte[] value = objectMapper.writeValueAsBytes(valueNode);
            return new ProducerRecord<>(tableName, key, value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialization should never fail", e);
        }
    }
}
