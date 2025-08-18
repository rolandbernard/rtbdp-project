package com.rolandb;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class DummyServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DummyServer.class);

    private final DummyData data;
    private final int port;
    private HttpServer server;

    private void handleDataRequest(HttpExchange exchange) {
        try {
            LOGGER.info("Handling data request from {}", exchange.getRemoteAddress());
            // Parse the query parameters.
            int page = 1;
            int perPage = 30;
            String query = exchange.getRequestURI().getQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] entry = param.split("=");
                    String key = URLDecoder.decode(entry[0], "UTF8");
                    String value = entry.length > 1 ? URLDecoder.decode(entry[1], "UTF8") : "";
                    try {
                        if (key.equals("page")) {
                            page = Integer.parseInt(value);
                        } else if (key.equals("per_page")) {
                            perPage = Integer.parseInt(value);
                        }
                    } catch (NumberFormatException ex) {
                        // Simply ignore the parameter
                    }
                }
            }
            if (perPage > 100) {
                perPage = 100;
            }
            // Read events from dummy data
            List<JsonNode> events = data.getEvents(page, perPage);
            // Serialize into JSON binary string
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] bytes = objectMapper.writeValueAsBytes(events);
            // Send response
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } catch (RuntimeException | IOException ex) {
            LOGGER.error("Failed to handle request", ex);
        }
    }

    public DummyServer(int port) throws IOException {
        this.port = port;
        data = new DummyData();
    }

    public void startListen() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/events", this::handleDataRequest);
        server.start();
        LOGGER.info("Server started on port {}. Access it at http://localhost:{}/", port, port);
    }

    /**
     * Run the dummy GitHub Events API server.
     *
     * @param args
     *            Arguments to configure the server.
     */
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
        try {
            DummyServer server = new DummyServer(port);
            server.startListen();
        } catch (IOException e) {
            LOGGER.error("Failed to start server", e);
        }
    }
}
