package com.rolandb;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * This is a serializer for the KafkaSink. We can't build this from the default
 * {@link KafkaRecordSerializationSchemaBuilder} because we don't actually want
 * to use the event time as the creation time in the Kafka topic. Doing so would
 * cause the dummy data to be deleted (or ready for deletion) immediately, as
 * that data has timestamps from the past.
 * 
 * @param <T>
 *            The type of row to serialize.
 */
public class KafkaTimedRowSerializer<T extends SequencedRow> implements KafkaRecordSerializationSchema<T> {
    /** Name of the table for which we serialize. */
    private final String tableName;
    /** Object mapper used for serialization. */
    private final ObjectMapper objectMapper;

    /**
     * Create a new serializer for the given table name. The table will be used as
     * topic name in the Kafka producer record.
     * 
     * @param tableName
     *            The name of the table this serializer is for.
     */
    public KafkaTimedRowSerializer(String tableName) {
        this.tableName = tableName;
        objectMapper = new ObjectMapper();
    }

    @Override
    public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(SequencedRow row, KafkaSinkContext context, Long timestamp) {
        if (row.seqNum == null) {
            throw new IllegalStateException("Attempting to store event without sequence number: " + row);
        }
        try {
            // Set as key the combination of key columns. This ensures in-order
            // delivery per-key, ensuring timestamps are monotonic.
            byte[] key = objectMapper.writeValueAsBytes(row.getKey());
            // We serialize each row as key-value pairs.
            ObjectNode valueNode = (ObjectNode) objectMapper.valueToTree(row);
            byte[] value = objectMapper.writeValueAsBytes(valueNode);
            return new ProducerRecord<>(tableName, key, value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialization should never fail", e);
        }
    }
}
