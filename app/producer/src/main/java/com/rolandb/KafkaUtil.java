package com.rolandb;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

/**
 * Utility methods related to the Kafka Java client. These have been taken from
 * the lab exercises, with some minor modifications.
 */
public class KafkaUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * Ensures that the given topic exists with the supplied number of partitions
     * and replication factor. If the topic does not exist, it is created with
     * the given settings. If the topic already exists, the method checks that
     * its number of partitions and replication factor match the ones specified
     * in the method arguments, throwing an {@link IllegalStateException} otherwise.
     *
     * @param topic
     *                          the topic that must exist
     * @param bootstrapServers
     *                          the address:port of one or more bootstrap servers,
     *                          required for using the admin client
     * @param numPartitions
     *                          the desired number of partitions
     * @param replicationFactor
     *                          the desired replication factor
     * @return true, if the topic was created, false if it already exists
     * @throws Exception
     *                   on failure
     */
    public static boolean setupTopic(String topic, String bootstrapServers, int numPartitions, int replicationFactor)
            throws Exception {
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", bootstrapServers);
        AdminClient client = AdminClient.create(config);
        boolean created = false;
        try {
            // Try creating the specified topic
            NewTopic t = new NewTopic(topic, numPartitions, (short) replicationFactor);
            CreateTopicsResult result = client.createTopics(Arrays.asList(t));
            result.all().get();
            created = true;
        } catch (ExecutionException ex) {
            // We handle only TopicExistsException
            if (!(ex.getCause() instanceof TopicExistsException)) {
                throw ex;
            }
            // Check that the topic already exists with the desired configuration
            DescribeTopicsResult result = client.describeTopics(Arrays.asList(topic));
            TopicDescription td = result.allTopicNames().get().get(topic);
            if (td.partitions().size() != numPartitions
                    || td.partitions().get(0).replicas().size() != replicationFactor) {
                throw new IllegalStateException("Expected topic '" + topic + "' with " + numPartitions
                        + " partitions and replication factor " + replicationFactor);
            }
        } finally {
            // Always close the AdminClient
            client.close();
        }
        LOGGER.info("{} topic '{}' with {} partitions and replication factor {}",
                created ? "Created" : "Found", topic, numPartitions, replicationFactor);
        return created;
    }

    /**
     * Returns a {@link ConsumerRebalanceListener} that logs the assigned partitions
     * (if enabled) and executes the specified actions. The returned listener can
     * be passed to
     * {@link KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)}.
     *
     * @param actions
     *                the action to execute in order, if any
     * @return the {@code ConsumerRebalanceListener} executing the actions at
     *         partition assignment
     */
    public static ConsumerRebalanceListener onAssign(Runnable... actions) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Do nothing. In general, this callback method should be followed by a call to
                // onPartitionAssigned() with the newly assigned partitions that replace the old
                // ones, so any action can be done there.
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // Log assignment, if enabled
                if (LOGGER.isInfoEnabled()) {
                    for (TopicPartition partition : partitions) {
                        LOGGER.info("Assigned {}", partition);
                    }
                }
                // Run the actions, if any
                for (Runnable action : actions) {
                    action.run();
                }
            }
        };
    }

    /**
     * Set the current offset to {@code partitionEndOffset - relativeOffset} on all
     * the partitions controlled by the supplied consumer. Note that this work only
     * <b>after</b> partitions have been assigned (i.e., not immediately after
     * subscribe()), so you should call this method either after poll() has been
     * called at least once, to reposition yourself in the stream, or as part of
     * the {@link ConsumerRebalanceListener} callback supplied to subscribe(), for
     * instance using {@link #onAssign(Runnable...)} as follows:
     * {@code subscribe(topics, Util.onAssign(() -> seekBeforeEnd(...))}
     *
     * @param consumer
     *                       the consumer
     * @param relativeOffset
     *                       the number of messages to read again starting from the
     *                       end of each partition (if 0, will
     *                       read only new messages coming after this method is
     *                       called, if 1 will re-read the last
     *                       message, and so on)
     */
    public static void seekBeforeEnd(KafkaConsumer<?, ?> consumer, int relativeOffset) {
        Set<TopicPartition> partitions = consumer.assignment();
        if (relativeOffset <= 0) {
            consumer.seekToEnd(partitions);
        } else {
            Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            for (TopicPartition p : partitions) {
                consumer.seek(p, Math.max(endOffsets.get(p) - relativeOffset, startOffsets.get(p)));
            }
        }
    }

    /**
     * Set the current offset to {@code partitionStartOffset + relativeOffset} on
     * all the partitions controlled by the supplied consumer. Note that this
     * work only <b>after</b> partitions have been assigned (i.e., not immediately
     * after subscribe()), so you should call this method either after poll() has
     * been called at least once, to reposition yourself in the stream, or as part
     * of the {@link ConsumerRebalanceListener} callback supplied to subscribe(),
     * for instance using {@link #onAssign(Runnable...)} as follows:
     * {@code subscribe(topics, Util.onAssign(() -> seekAfterStart(...))}
     *
     * @param consumer
     *                       the consumer
     * @param relativeOffset
     *                       the number of messages to skip from the beginning of
     *                       each partition (if 0, will
     *                       read all messages from beginning, if 1 will skip the
     *                       first message, and so on)
     */
    public static void seekAfterStart(KafkaConsumer<?, ?> consumer, int relativeOffset) {
        Set<TopicPartition> partitions = consumer.assignment();
        if (relativeOffset <= 0) {
            consumer.seekToBeginning(partitions);
        } else {
            Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            for (TopicPartition p : partitions) {
                consumer.seek(p, Math.min(startOffsets.get(p) + relativeOffset, endOffsets.get(p)));
            }
        }
    }
}
