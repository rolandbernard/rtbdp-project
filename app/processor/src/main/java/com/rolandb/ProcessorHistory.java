package com.rolandb;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.rolandb.AbstractTable.TableBuilder;
import com.rolandb.tables.CountsHistoryTable;
import com.rolandb.tables.ReposHistoryTable;
import com.rolandb.tables.RepositoriesTable;
import com.rolandb.tables.StarsHistoryTable;
import com.rolandb.tables.UsersHistoryTable;
import com.rolandb.tables.UsersTable;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * This is the main class for submitting the Flink job processing in batch mode
 * historical events retrieved from GhArchive. This job does not populate Kafka,
 * instead only writing to tables in PostgreSQL. Further, the job only computes
 * a subset of tables, that is those that are retained for a longer time.
 */
public class ProcessorHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

    /** This class is only for static methods. You should not instantiate it. */
    private ProcessorHistory() {
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
                .description("Flink processor reading events from GitHub Archive and writing results to PostgreSQL");
        parser.addArgument("--archive-url").metavar("URL").setDefault("https://data.gharchive.org")
                .help("use this url to access the GH Archive");
        parser.addArgument("--start-date").metavar("DATE").required(true)
                .help("the first date to replay");
        parser.addArgument("--end-date").metavar("DATE").required(true)
                .help("the last date to replay");
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
        parser.addArgument("--dry-run").action(Arguments.storeTrue())
                .help("print to stdout instead of writing to DB");
        Namespace cmd;
        try {
            cmd = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return;
        }
        // Read options.
        String dbUrl = cmd.getString("db_url");
        String dbUsername = cmd.getString("db_username");
        String dbPassword = cmd.getString("db_password");
        String archiveUrl = cmd.getString("archive_url");
        String startDate = cmd.getString("start_date");
        String endDate = cmd.getString("end_date");
        int parallelism = cmd.getInt("parallelism");
        int uiPort = cmd.getInt("ui_port");
        boolean dryRun = cmd.getBoolean("dry_run");
        // Obtain and configure Flink environments.
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, uiPort);
        // Increase memory size a bit.
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(2048));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(512));
        conf.set(PipelineOptions.AUTO_GENERATE_UIDS, false);
        // Configure parallelism.
        conf.set(JobManagerOptions.SCHEDULER, SchedulerType.AdaptiveBatch);
        // Set RocksDB as state backend to allow large and more durable state.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);
        // Define Kafka source.
        GhArchiveSource ghArchiveSource = new GhArchiveSource(archiveUrl, startDate, endDate);
        // Parse into a stream of events.
        DataStream<JsonNode> rawEventsStream = env
                .fromSource(ghArchiveSource,
                        // We assume events can be up to 10 seconds late, but otherwise
                        // in-order.
                        WatermarkStrategy
                                .<JsonNode>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> Instant
                                        .parse(event.at("/created_at").asText())
                                        .toEpochMilli()),
                        "GhArchive Source")
                .uid("gharchive-source-01");
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
                        .build())
                .setDryRun(dryRun);
        // Actually setup table computations.
        // Aggregated user and repository details
        builder.build("users", UsersTable.class);
        builder.build("repos", RepositoriesTable.class);
        // Per-kind event counts
        builder.build("counts_history", CountsHistoryTable.class);
        // Per-user event counts
        builder.build("users_history", UsersHistoryTable.class);
        // Per-repository event counts
        builder.build("repos_history", ReposHistoryTable.class);
        // Trending repository detection
        builder.build("stars_history", StarsHistoryTable.class);
        // Adjusting parallelism. We do this here because I want to limit the
        // parallelism of every operator to the one given in the command line.
        StreamGraph graph = env.getStreamGraph();
        graph.getStreamNodes().forEach(node -> {
            if (node.getParallelism() > parallelism) {
                node.setParallelism(parallelism);
            }
        });
        graph.setJobName("GitHub Historical Event Analysis");
        // Execute all statements as a single job
        LOGGER.info("Submitting Flink job");
        env.execute(graph);
    }
}
