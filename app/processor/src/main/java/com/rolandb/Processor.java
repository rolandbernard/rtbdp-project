package com.rolandb;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Processor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

    public static void main(String[] args) throws Exception {
        // Parse command line.
        ArgumentParser parser = ArgumentParsers.newFor("Processor").build()
                .description("Flink processor reading events from Kafka and different back to Kafka and to PostgreSQL");
        parser.addArgument("--bootstrap-servers").metavar("HOST:PORT")
                .setDefault("localhost:29092").help("bootstrap servers");
        parser.addArgument("--topic").metavar("TOPIC")
                .setDefault("events").help("Kafka topic name for input events data");
        parser.addArgument("--db-url").metavar("JDBCURL")
                .setDefault("jdbc:postgresql://localhost:25432/db").help("JDBC URL of output database");
        parser.addArgument("--db-username").metavar("USERNAME")
                .setDefault("user").help("username for accessing output database");
        parser.addArgument("--db-password").metavar("PASSWORD")
                .setDefault("user").help("password for accessing output database");
        parser.addArgument("--ui-port").metavar("PORT").type(Integer.class).setDefault(8081)
                .help("enables Flink UI at specified port when running standalone (mini-cluster mode)");
        parser.addArgument("--rewind").action(Arguments.storeTrue())
                .help("whether to (re)process input events from the beginning");
        parser.addArgument("--dry-run").action(Arguments.storeTrue())
                .help("print to stdout instead of writing to DB/Kafka");
        Namespace cmd;
        try {
            cmd = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return;
        }
        // Read options.
        String bootstrapServers = cmd.getString("bootstrap_servers");
        String inputTopic = cmd.getString("topic");
        String dbUrl = cmd.getString("db_url");
        String dbUsername = cmd.getString("db_username");
        String dbPassword = cmd.getString("db_password");
        int uiPort = cmd.getInt("ui_port");
        boolean rewind = cmd.getBoolean("rewind");
        boolean dryRun = cmd.getBoolean("dry_run");
        // Await input Kafka topic.
        KafkaUtil.waitForTopics(bootstrapServers, inputTopic);
        // Obtain and configure Flink environments.
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, uiPort);
        conf.setInteger("table.exec.source.idle-timeout", 1000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env,
                new EnvironmentSettings.Builder().withConfiguration(conf).build());
        // Define Kafka source.
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("processor")
                .setStartingOffsets(rewind ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // Parse into a stream of events.
        DataStream<GithubEvent> eventStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(jsonString -> {
                    ObjectMapper mapper = new ObjectMapper();
                    GithubEvent event = mapper.readValue(jsonString, GithubEvent.class);
                    return event;
                }, TypeInformation.of(GithubEvent.class))
                .returns(Types.POJO(GithubEvent.class)) // <--- This is the key fix
                // We assume events can be up to 10 seconds late, but otherwise in-order.
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<GithubEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.getCreatedAt().toEpochMilli()));
        Table eventsTable = tenv.fromDataStream(eventStream,
                Schema.newBuilder()
                        .column("eventType", DataTypes.STRING())
                        .column("createdAt", DataTypes.TIMESTAMP_LTZ(3))
                        .column("username", DataTypes.STRING())
                        .column("reponame", DataTypes.STRING())
                        .watermark("createdAt", "SOURCE_WATERMARK()")
                        .build());
        DataStream<Row> output = tenv.toChangelogStream(eventsTable);
        if (dryRun) {
            output.print().setParallelism(1);
        }
        // Execute all statements as a single job
        LOGGER.info("Submitting Flink job");
        env.execute();
    }
}

// private GithubEventType eventType;
// private Instant createdAt;
// private String username;
// private String reponame;
