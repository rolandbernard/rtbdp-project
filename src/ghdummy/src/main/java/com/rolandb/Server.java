package com.rolandb;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * This is a dummy server that can be used as a replacement for the official
 * `api.github.com` server for testing the rest of the application. This avoids
 * the need for a GitHub API access token and also avoids running into rate
 * limits.
 * The data that is served has been collected from the real GitHub API on the
 * 17th of August 2025.
 */
public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        // Parse command line
        ArgumentParser parser = ArgumentParsers.newFor("Server").build()
                .description("Dummy server for serving a fake GitHub Events API. For testing purposes.");
        parser.addArgument("--port").metavar("PORT").type(Integer.class)
                .setDefault(8889).help("the HTTP port for the exposed HTTP server");
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
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/events", exchange -> handleDataRequest(exchange));
            server.start();
            LOGGER.info("Server started on port {}. Access it at http://localhost:{}/", port, port);
        } catch (IOException e) {
            LOGGER.error("Failed to start server", e);
        }
    }

    private static void handleDataRequest(HttpExchange exchange) {
        try {
            LOGGER.info("Handling data request from {}", exchange.getRemoteAddress());

            // // Compute or reuse tag cloud data
            // TagCloudData data;
            // synchronized (recentNews) {
            // if (cachedTagCloudData == null) {
            // cachedTagCloudData = new TagCloudData(recentNews);
            // }
            // data = cachedTagCloudData;
            // }

            // Serialize into JSON binary string
            byte[] bytes = "{\"hello\": \"test\"}".getBytes();

            // Send response
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } catch (IOException ex) {
            LOGGER.error("Failed to handle request", ex);
        }
    }
}
