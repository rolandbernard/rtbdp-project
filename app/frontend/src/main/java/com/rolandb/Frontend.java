package com.rolandb;

import java.io.IOException;
import java.net.InetSocketAddress;

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
 * 1. It statically serves the resources in `com.rolandb.public` under `/`. Note
 *    that this contains a single page Preact+HTM+MUI+RxJS+Charts.js app.
 * 2. It provides a WebSocket interface for the client to get data. The client
 *    will be able to subscribe to different tables and types of events, and
 *    then receive updates
 */
public class Frontend {
    private static final Logger LOGGER = LoggerFactory.getLogger(Frontend.class);

    private final int port;
    private HttpServer server;

    /**
     * Create a new server.
     * 
     * @param port The port the server should listen on.
     * @throws IOException In case the events data can not be loaded.
     */
    public Frontend(int port) throws IOException {
        this.port = port;
    }

    /**
     * Start listening on the port specified in the constructor and answer to
     * client requests.
     * 
     * @throws IOException In case the server can not be started.
     */
    public void startListen() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.start();
        LOGGER.info("Server started on port {}. Access it at http://localhost:{}/", port, port);
    }

    /**
     * Run the GitHub Events API server.
     *
     * @param args
     *             Arguments to configure the server.
     */
    public static void main(String[] args) {
        // Parse command line
        ArgumentParser parser = ArgumentParsers.newFor("Frontend").build()
                .description("server for serving a the frontend code and WebSocket API.");
        parser.addArgument("--port").metavar("PORT").type(Integer.class)
                .setDefault(8888).help("the HTTP port for the exposed HTTP server");
        parser.addArgument("--ws-port").metavar("PORT").type(Integer.class)
                .setDefault(8887).help("the HTTP port for the exposed WebSocket server");
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
        int port = cmd.getInt("port");
        String logLevel = cmd.getString("log_level");
        // Configures logging
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(logLevel));
        // Create and start HTTP server
        try {
            Frontend server = new Frontend(port);
            server.startListen();
        } catch (IOException e) {
            LOGGER.error("Failed to start server", e);
        }
    }
}
