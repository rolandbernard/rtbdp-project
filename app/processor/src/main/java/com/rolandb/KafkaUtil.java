package com.rolandb;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
     *            The topic that must exist
     * @param bootstrapServers
     *            The address:port of one or more bootstrap servers,
     *            required for using the admin client
     * @param numPartitions
     *            The desired number of partitions
     * @param replicationFactor
     *            The desired replication factor
     * @param retentionMs
     *            The desired retention time for teh topic in milliseconds
     * @return true, if the topic was created, false if it already exists
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static boolean setupTopic(
            String topic, String bootstrapServers, int numPartitions, int replicationFactor, long retentionMs)
            throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        boolean created = false;
        try (AdminClient client = AdminClient.create(props)) {
            try {
                // Try creating the specified topic
                NewTopic t = new NewTopic(topic, numPartitions, (short) replicationFactor);
                t.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs)));
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
            }
        }
        LOGGER.info("{} topic '{}' with {} partitions and replication factor {}",
                created ? "Created" : "Found", topic, numPartitions, replicationFactor);
        return created;
    }

    /**
     * Block until the specified topics are available in Kafka before returning
     * from the method. The list of available topics will be polled one a second
     * until all of the requested ones are available.
     *
     * @param bootstrapServers
     *            The address:port of one or more bootstrap servers, required for
     *            using the admin client.
     * @param topics
     *            The list of topics that we want to wait for before continuing.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void waitForTopics(String bootstrapServers, String... topics)
            throws InterruptedException, ExecutionException {
        Set<String> expectedTopics = Set.of(topics);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        while (true) {
            try (AdminClient client = AdminClient.create(props)) {
                while (true) {
                    Set<String> existingTopics = client.listTopics().names().get();
                    if (existingTopics.containsAll(expectedTopics)) {
                        LOGGER.info("Found topics {}", expectedTopics);
                        return;
                    }
                    LOGGER.info("Waiting for topics {}", expectedTopics);
                    Thread.sleep(1000);
                }
            } catch (KafkaException ex) {
                LOGGER.error("Unable to connect to Kafka", ex);
                Thread.sleep(5000);
            }
        }
    }

    /**
     * Find the number of partitions in the given topic.
     *
     * @param bootstrapServers
     *            The address:port of one or more bootstrap servers, required for
     *            using the admin client.
     * @param topics
     *            The name of the topic that we want to get the info for.
     * @return The number of partitions used for the requested Kafka topic.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static int partitionsForTopic(String bootstrapServers, String topic)
            throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient client = AdminClient.create(props)) {
            DescribeTopicsResult result = client.describeTopics(Arrays.asList(topic));
            TopicDescription td = result.allTopicNames().get().get(topic);
            return td.partitions().size();
        }
    }
}
