package com.rolandb;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * The main application class of the Kafka producer. It will setup the polling
 * service, subscribe to it's observable and write all of the events into a
 * Kafka topic.
 */
public class Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Parse command line.
        ArgumentParser parser = ArgumentParsers.newFor("Producer").build()
                .description("Kafka producer for public GitHub Events via REST API");
        parser.addArgument("--bootstrap-servers").metavar("HOST:PORT")
                .setDefault("localhost:29092").help("bootstrap servers");
        parser.addArgument("--topic").metavar("TOPIC").setDefault("raw_events")
                .help("Kafka topic name to output events data");
        parser.addArgument("--num-partitions").metavar("PARTITIONS").type(Integer.class)
                .setDefault(4).help("# partitions for Kafka topic");
        parser.addArgument("--replication-factor").metavar("REPLICATION").type(Integer.class)
                .setDefault(1).help("replication factor for Kafka topic");
        // This is the default retention period for Kafka.
        parser.addArgument("--retention-ms").metavar("MILLIS").type(Long.class)
                .setDefault(1000L * 60L * 60L * 24L * 7L).help("retention time for the Kafka topics");
        parser.addArgument("--dry-run").action(Arguments.storeTrue()).help("send events to stdout instead of Kafka");
        parser.addArgument("--url").metavar("URL")
                .setDefault("http://localhost:8889").help("GitHub API URL to poll from");
        parser.addArgument("--gh-token").metavar("GITHUB_TOKEN")
                .setDefault("github_dummy").help("GitHub API access token");
        parser.addArgument("--poll-ms").metavar("POLL_MS").type(Integer.class)
                .setDefault(2250).help("polling interval in milliseconds");
        parser.addArgument("--poll-depth").metavar("POLL_DEPTH").type(Integer.class)
                .setDefault(300).help("number of events to poll each time");
        parser.addArgument("--log-level").type(String.class).setDefault("debug")
                .help("configures the log level (default: debug; values: all|trace|debug|info|warn|error|off");
        Namespace cmd;
        try {
            cmd = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return;
        }
        // Read options.
        String bootstrapServers = cmd.getString("bootstrap_servers");
        String topic = cmd.getString("topic");
        int numPartitions = cmd.getInt("num_partitions");
        int replicationFactor = cmd.getInt("replication_factor");
        long retentionMs = cmd.getLong("retention_ms");
        String url = cmd.getString("url");
        String accessToken = cmd.getString("gh_token");
        boolean dryRun = cmd.getBoolean("dry_run");
        int pollMs = cmd.getInt("poll_ms");
        int pollDepth = cmd.getInt("poll_depth");
        String logLevel = cmd.getString("log_level");
        // Configures logging.
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(logLevel));
        loggerContext.getLogger("com.rolandb").setLevel(Level.toLevel(logLevel));
        KafkaProducer<String, String> kafkaProducer;
        if (!dryRun) {
            // Create & check topic.
            KafkaUtil.setupTopic(topic, bootstrapServers, numPartitions, replicationFactor, retentionMs);
            // Create producer.
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.ACKS_CONFIG, "1"); // Wait for only one in-sync replica to acknowledge the write.
            props.put(ProducerConfig.RETRIES_CONFIG, 5); // Retry up to 5 times on transient failures.
            props.put(ProducerConfig.LINGER_MS_CONFIG, 250); // Allow some batching.
            kafkaProducer = new KafkaProducer<>(props);
        } else {
            kafkaProducer = null;
        }
        // Setup REST API Client.
        RestApiClient restApiClient = new RestApiClient(url, accessToken);
        EventPollService pollingService = new EventPollService(restApiClient, pollMs, pollDepth);
        ObjectMapper objectMapper = new ObjectMapper();
        // Subscribe to the event observable and send events to Kafka.
        pollingService.getEventsStream().subscribe(
                event -> {
                    try {
                        ObjectNode rawEvent = (ObjectNode) event.getRawEvent();
                        rawEvent.set("seq_num", LongNode.valueOf(event.seqNum));
                        String eventJson = objectMapper.writeValueAsString(rawEvent);
                        if (kafkaProducer == null) {
                            // For dry-run, print to stdout.
                            System.out.println(eventJson);
                            System.out.flush();
                        } else {
                            // Asynchronously send the record.
                            ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getId(),
                                    eventJson);
                            kafkaProducer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    pollingService.unmarkEvent(event);
                                    LOGGER.error("Failed to send event with ID '{}' to Kafka", event.getId(),
                                            exception);
                                }
                            });
                        }
                    } catch (JsonProcessingException e) {
                        pollingService.unmarkEvent(event);
                        LOGGER.error("Failed to serialize event with ID '{}' to JSON", event.getId(), e);
                    }
                },
                error -> LOGGER.error("Polling stream encountered an error", error));
        // Start the polling the REST service.
        pollingService.startPolling();
        // Add a shutdown hook to ensure a clean exit.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down application");
            pollingService.stopPolling();
            if (kafkaProducer != null) {
                kafkaProducer.flush();
                kafkaProducer.close(Duration.ofSeconds(10));
                LOGGER.info("Kafka producer closed");
            }
        }));
        // Wait for program to be terminated.
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
