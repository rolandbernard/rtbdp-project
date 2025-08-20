package com.rolandb;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

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

    public static void main(String[] args) {
        // Parse command line
        ArgumentParser parser = ArgumentParsers.newFor("KafkaProducer").build()
                .description("Kafka producer for public GitHub Events via REST API");
        parser.addArgument("--bootstrap-servers").metavar("SERVERS")
                .setDefault("localhost:29092").help("bootstrap servers");
        parser.addArgument("--topic").metavar("TOPIC").setDefault("events").help("topic name");
        parser.addArgument("--num-partitions").metavar("PARTITIONS").type(Integer.class)
                .setDefault(1).help("# partitions");
        parser.addArgument("--replication-factor").metavar("REPLICATION").type(Integer.class)
                .setDefault(1).help("replication factor");
        parser.addArgument("--dry-run").action(Arguments.storeTrue()).help("send to stdout instead of Kafka");
        parser.addArgument("--url").metavar("URL")
                .setDefault("https://api.github.com").help("GitHub API URL");
        parser.addArgument("--poll-ms").metavar("POLL_MS").type(Integer.class)
                .setDefault(1).help("polling interval in milliseconds");
        parser.addArgument("--poll-depth").metavar("POLL_DEPTH").type(Integer.class)
                .setDefault(1).help("number of events to poll each time");
        parser.addArgument("--log-level").type(String.class).setDefault("debug")
                .help("configures the log level (default: debug; values: all|trace|debug|info|warn|error|off");
        Namespace cmd;
        try {
            cmd = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return;
        }
        // Read options
        String bootstrapServers = cmd.getString("bootstrap_servers");
        String topic = cmd.getString("topic");
        int numPartitions = cmd.getInt("num_partitions");
        int replicationFactor = cmd.getInt("replication_factor");
        String url = cmd.getString("url");
        boolean dryRun = cmd.getBoolean("dry_run");
        int pollMs = cmd.getInt("poll_ms");
        int pollDepth = cmd.getInt("poll_depth");
        String logLevel = cmd.getString("log_level");
        // Configures logging
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(logLevel));

        KafkaProducer<Void, String> producer = null;
        if (!dryRun) {
            // Create & check topic
            KafkaUtil.setupTopic(topic, bootstrapServers, numPartitions, replicationFactor);
            // Create producer
            producer = new KafkaProducer<>(ImmutableMap.of(
                    "key.serializer", "org.apache.kafka.common.serialization.VoidSerializer",
                    "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                    "bootstrap.servers", bootstrapServers,
                    "compression.type", "gzip",
                    "acks", "0", // happily drop messages if cluster not available (reasonable here)
                    "linger.ms", "250" // allow some batching
            ));
        }
    }
}
