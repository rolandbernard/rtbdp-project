package com.rolandb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * This is the frontend server for the application. It largely has two
 * different ways to interact with it by the client:
 * 1. It statically serves the resources in `com.rolandb.static` under `/`. Note
 * that this contains a single page Preact+HTM+MUI+RxJS+Charts.js app.
 * 2. It provides a WebSocket interface for the client to get data. The client
 * will be able to subscribe to different tables and types of events, and
 * then receive updates
 */
public class Frontend {
    private static final Logger LOGGER = LoggerFactory.getLogger(Frontend.class);

    private final int httpPort;
    private final int wsPort;
    private final String bootstrapServer;
    private final String groupId;
    private final String jdbcUrl;
    private HttpServer httpServer;
    private WebSocketServer webSocketServer;

    /**
     * Create a new server.
     *
     * @param httpPort
     *            The port the server should listen on.
     * @throws IOException
     *             In case the events data can not be loaded.
     */
    public Frontend(int httpPort, int wsPort, String bootstrapServer, String groupId, String jdbcUrl)
            throws IOException {
        this.httpPort = httpPort;
        this.wsPort = wsPort;
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Start listening on the port specified in the constructor and answer to
     * client requests. Starts both the HTTP and the WebSocket servers.
     *
     * @throws IOException
     *             In case the server can not be started.
     */
    public void startListen() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/", new StaticFileHandler());
        httpServer.start();
        LOGGER.info("Server started on port {}. Access it at http://localhost:{}/", httpPort, httpPort);
        webSocketServer = new SocketApiServer(new InetSocketAddress(wsPort), bootstrapServer, groupId, jdbcUrl);
        webSocketServer.start();
        LOGGER.info("Server started on port {}. Access it at ws://localhost:{}/", wsPort, wsPort);
    }

    /**
     * Stop the server from running. Stops both the HTTP and the WebSocket
     * servers.
     */
    public void stopListen() {
        httpServer.stop((int) Duration.ofSeconds(1).toSeconds());
        httpServer = null;
        LOGGER.info("Server on port {} stopped", httpPort);
        try {
            webSocketServer.stop();
        } catch (InterruptedException e) {
            // We are now shutting down anyways.
            LOGGER.warn("Failed to properly shutdown WebSocket server", e);
        }
        webSocketServer = null;
        LOGGER.info("Server on port {} stopped", wsPort);
    }

    /**
     * Run the GitHub Events API server.
     *
     * @param args
     *            Arguments to configure the server.
     */
    public static void main(String[] args) {
        // Parse command line
        ArgumentParser parser = ArgumentParsers.newFor("Frontend").build()
                .description("server for serving a the frontend code and WebSocket API.");
        parser.addArgument("--port").metavar("PORT").type(Integer.class)
                .setDefault(8888).help("the HTTP port for the exposed HTTP server");
        parser.addArgument("--ws-port").metavar("PORT").type(Integer.class)
                .setDefault(8887).help("the HTTP port for the exposed WebSocket server");
        parser.addArgument("--bootstrap-servers").metavar("SERVERS")
                .setDefault("localhost:29092").help("bootstrap servers");
        parser.addArgument("--group-id").metavar("ID")
                .setDefault("frontend").help("group id when consuming edits");
        parser.addArgument("--db-url").metavar("JDBCURL")
                .setDefault("jdbc:postgresql://localhost:25432/db").help("JDBC URL of database");
        parser.addArgument("--db-username").metavar("USERNAME")
                .setDefault("user").help("username for accessing database");
        parser.addArgument("--db-password").metavar("PASSWORD")
                .setDefault("user").help("password for accessing database");
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
        int httpPort = cmd.getInt("port");
        int wsPort = cmd.getInt("ws_port");
        String bootstrapServer = cmd.getString("bootstrap_servers");
        String groupId = cmd.getString("group_id");
        String dbUrl = cmd.getString("db_url");
        String dbUsername = cmd.getString("db_username");
        String dbPassword = cmd.getString("db_password");
        String logLevel = cmd.getString("log_level");
        // Configures logging
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(logLevel));
        // Create and start HTTP server
        try {
            Frontend server = new Frontend(
                    httpPort, wsPort, bootstrapServer, groupId,
                    dbUrl + "?user=" + dbUsername + "&password=" + dbPassword);
            // Add a shutdown hook to ensure a clean exit.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down servers");
                server.stopListen();
            }));
            server.startListen();
        } catch (IOException e) {
            LOGGER.error("Failed to start server", e);
        }
    }
}
