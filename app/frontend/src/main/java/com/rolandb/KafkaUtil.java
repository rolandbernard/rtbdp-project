package com.rolandb;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Utility methods related to the Kafka Java client. These have been taken from
 * the lab exercises, with some minor modifications.
 */
public final class KafkaUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * This class is only for static methods. You should not instantiate it.
     */
    private KafkaUtil() {
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
     *             If thrown by the Kafka client.
     * @throws InterruptedException
     *             If interrupted.
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
}
