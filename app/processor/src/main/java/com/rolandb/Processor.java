package com.rolandb;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rolandb.AbstractTable.TableBuilder;
import com.rolandb.tables.CountsHistoryTable;
import com.rolandb.tables.CountsLiveTable;
import com.rolandb.tables.CountsRankingTable;
import com.rolandb.tables.GithubEventsTable;
import com.rolandb.tables.ReposHistoryTable;
import com.rolandb.tables.ReposLiveTable;
import com.rolandb.tables.ReposRankingTable;
import com.rolandb.tables.RepositoriesTable;
import com.rolandb.tables.StarsHistoryTable;
import com.rolandb.tables.StarsLiveTable;
import com.rolandb.tables.StarsRankingTable;
import com.rolandb.tables.TrendingLiveTable;
import com.rolandb.tables.TrendingRankingTable;
import com.rolandb.tables.UsersHistoryTable;
import com.rolandb.tables.UsersLiveTable;
import com.rolandb.tables.UsersRankingTable;
import com.rolandb.tables.UsersTable;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * This is the main class for submitting the Flink job processing all of the
 * events and outputting both to Kafka topics and PostgreSQL tables.
 */
public class Processor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

    /** This class is only for static methods. You should not instantiate it. */
    private Processor() {
    }

    /**
     * Submit the Flink job configured based on the given arguments.
     * 
     * @param args
     *            The arguments with which to configure the job.
     * @throws Exception
     *             In case of errors.
     */
    public static void main(String[] args) throws Exception {
        // Parse command line.
        ArgumentParser parser = ArgumentParsers.newFor("Processor").build()
                .description("Flink processor reading events from Kafka and different back to Kafka and to PostgreSQL");
        parser.addArgument("--bootstrap-servers").metavar("HOST:PORT")
                .setDefault("localhost:29092").help("bootstrap servers");
        parser.addArgument("--group-id").metavar("ID")
                .setDefault("processor").help("group id when consuming edits");
        parser.addArgument("--topic").metavar("TOPIC")
                .setDefault("raw_events").help("Kafka topic name for input events data");
        parser.addArgument("--db-url").metavar("JDBCURL")
                .setDefault("jdbc:postgresql://localhost:25432/db").help("JDBC URL of output database");
        parser.addArgument("--db-username").metavar("USERNAME")
                .setDefault("user").help("username for accessing output database");
        parser.addArgument("--db-password").metavar("PASSWORD")
                .setDefault("user").help("password for accessing output database");
        parser.addArgument("--parallelism").metavar("TASKS").type(Integer.class)
                .setDefault(1).help("set the desired level of parallelism");
        parser.addArgument("--ui-port").metavar("PORT").type(Integer.class).setDefault(8081)
                .help("enables Flink UI at specified port when running standalone (mini-cluster mode)");
        parser.addArgument("--num-partitions").metavar("PARTITIONS").type(Integer.class)
                .setDefault(4).help("# partitions for Kafka topics");
        parser.addArgument("--replication-factor").metavar("REPLICATION").type(Integer.class)
                .setDefault(1).help("replication factor for Kafka topics");
        // The default retention for these topics is significantly lower than the
        // default 7 days of Kafka. This is because these topics are mainly only
        // for live updates in the frontend, and the persistent data is in the
        // PostgreSQL DB.
        parser.addArgument("--retention-ms").metavar("MILLIS").type(Long.class)
                .setDefault(1000L * 60L * 15L).help("retention time for the Kafka topics");
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
        String groupId = cmd.getString("group_id");
        String inputTopic = cmd.getString("topic");
        String dbUrl = cmd.getString("db_url");
        String dbUsername = cmd.getString("db_username");
        String dbPassword = cmd.getString("db_password");
        int parallelism = cmd.getInt("parallelism");
        int uiPort = cmd.getInt("ui_port");
        int numPartitions = cmd.getInt("num_partitions");
        int replicationFactor = cmd.getInt("replication_factor");
        long retentionMs = cmd.getLong("retention_ms");
        boolean rewind = cmd.getBoolean("rewind");
        boolean dryRun = cmd.getBoolean("dry_run");
        // Await input Kafka topic.
        KafkaUtil.waitForTopics(bootstrapServers, inputTopic);
        // Obtain and configure Flink environments.
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, uiPort);
        // Increase memory size a bit.
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(2048));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(512));
        // Set RocksDB as state backend to allow large and more durable state.
        conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(PipelineOptions.AUTO_GENERATE_UIDS, false);
        // Configure parallelism.
        conf.set(JobManagerOptions.SCHEDULER, SchedulerType.Adaptive);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        // Enable checkpointing. At-least-once semantics are fine because we basically
        // always use upserts with a primary key.
        env.enableCheckpointing(60_000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig()
                .setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Define Kafka source.
        @SuppressWarnings("deprecation") // Ignoring the warning because there seems to be no alternative.
        KafkaSource<JsonNode> kafkaSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setStartingOffsets(rewind ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new DeserializationSchema<JsonNode>() {
                    ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public TypeInformation<JsonNode> getProducedType() {
                        return TypeInformation.of(JsonNode.class);
                    }

                    @Override
                    public JsonNode deserialize(byte[] message) throws IOException {
                        return objectMapper.readTree(message);
                    }

                    @Override
                    public boolean isEndOfStream(JsonNode nextElement) {
                        return false;
                    }
                })
                .build();
        // Parse into a stream of events.
        DataStream<JsonNode> rawEventsStream = env
                .fromSource(kafkaSource,
                        // We assume events can be up to 10 seconds late, but otherwise in-order.
                        WatermarkStrategy
                                .<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> Instant
                                        .parse(event.at("/created_at").asText())
                                        .toEpochMilli()),
                        "Kafka Source")
                .uid("kafka-source-01")
                // We can not use more parallelism for the Kafka source than we have partitions,
                // because each subtask is assigned a partition and there is no point in having
                // idle subtasks, also because they would hold back watermarks.
                .setParallelism(KafkaUtil.partitionsForTopic(bootstrapServers, inputTopic))
                .rebalance();
        // Setup parameters for table builder.
        TableBuilder builder = (new TableBuilder())
                .setEnv(env)
                .addStream("rawEvents", rawEventsStream)
                .setJdbcOptions(new JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withUsername(dbUsername)
                        .withPassword(dbPassword)
                        .withDriverName("org.postgresql.Driver")
                        .withProperty("stringtype", "unspecified")
                        .withProperty("reWriteBatchedInserts", "true")
                        .withProperty("ApplicationName", "Flink Processor")
                        .build())
                .setBootstrapServers(bootstrapServers)
                .setDryRun(dryRun)
                .setNumPartitions(numPartitions)
                .setReplicationFactor(replicationFactor)
                .setRetentionMs(retentionMs);
        // Actually setup table computations.
        // Table of all events
        builder.build("events", GithubEventsTable.class);
        // Aggregated user and repository details
        builder.build("users", UsersTable.class);
        builder.build("repos", RepositoriesTable.class);
        // Per-kind event counts
        builder.build("counts_history", CountsHistoryTable.class);
        builder.get("counts_history_fine", CountsHistoryTable.class)
                .setWindowSize(Duration.ofSeconds(10))
                .build();
        builder.build("counts_live", CountsLiveTable.class);
        builder.build("counts_ranking", CountsRankingTable.class);
        // Per-user event counts
        builder.build("users_history", UsersHistoryTable.class);
        builder.get("users_history_fine", UsersHistoryTable.class)
                .setWindowSize(Duration.ofSeconds(10))
                .build();
        builder.build("users_live", UsersLiveTable.class);
        builder.build("users_ranking", UsersRankingTable.class);
        // Per-repository event counts
        builder.build("repos_history", ReposHistoryTable.class);
        builder.get("repos_history_fine", ReposHistoryTable.class)
                .setWindowSize(Duration.ofSeconds(10))
                .build();
        builder.build("repos_live", ReposLiveTable.class);
        builder.build("repos_ranking", ReposRankingTable.class);
        // Trending repository detection
        builder.build("stars_history", StarsHistoryTable.class);
        builder.get("stars_history_fine", StarsHistoryTable.class)
                .setWindowSize(Duration.ofSeconds(10))
                .build();
        builder.build("stars_live", StarsLiveTable.class);
        builder.build("stars_ranking", StarsRankingTable.class);
        builder.build("trending_live", TrendingLiveTable.class);
        builder.build("trending_ranking", TrendingRankingTable.class);
        // Adjusting parallelism. We do this here because I want to limit the
        // parallelism of every operator to the one given in the command line.
        StreamGraph graph = env.getStreamGraph();
        graph.getStreamNodes().forEach(node -> {
            if (node.getParallelism() > parallelism) {
                node.setParallelism(parallelism);
            }
        });
        graph.setJobName("GitHub Event Analysis");
        // Execute all statements as a single job
        LOGGER.info("Submitting Flink job");
        env.execute(graph);
    }
}
