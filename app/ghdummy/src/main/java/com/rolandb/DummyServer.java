package com.rolandb;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rolandb.DummyData.Event;
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
 * {@code api.github.com} server for testing the rest of the application. This
 * avoids the need for a GitHub API access token and also avoids running into
 * rate limits.
 * The data that is served has been collected from the real GitHub API on the
 * 17th of August 2025.
 */
public class DummyServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DummyServer.class);

    private final DummyData data;
    private final int port;
    private HttpServer server;

    /**
     * Handle a given request by returning simple {@code OK}.
     *
     * @param exchange
     *            The HTTP request to handle.
     * @throws IOException
     */
    private void handleStatusRequest(HttpExchange exchange) throws IOException {
        String response = "OK";
        exchange.getResponseHeaders().set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Handle a given request by returning a list of events.
     *
     * @param exchange
     *            The HTTP request to handle.
     * @throws IOException
     */
    private void handleDataRequest(HttpExchange exchange) throws IOException {
        try {
            LOGGER.info("Handling data request from {}", exchange.getRemoteAddress());
            // Parse the query parameters.
            int page = 1;
            int perPage = 30;
            String query = exchange.getRequestURI().getQuery();
            String token = exchange.getRequestHeaders().getFirst("Authorization");
            if (token == null || !token.startsWith("token github_")) {
                // Make sure we actually send the token correctly.
                String response = "403 Forbidden";
                exchange.sendResponseHeaders(403, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                return;
            }
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
            List<Event> events = data.getEvents(page, perPage);
            // Serialize into JSON byte string
            byte[] bytes = events.stream().map(e -> e.json).collect(Collectors.joining(",", "[", "]"))
                    .getBytes(StandardCharsets.UTF_8);
            // Send response
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } catch (RuntimeException | IOException ex) {
            LOGGER.error("Failed to handle request", ex);
            // Write a HTTP 500 server error response.
            String response = "500 Internal Server Error";
            exchange.sendResponseHeaders(500, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    /**
     * Create a new dummy server.
     *
     * @param port
     *            The port the server should listen on.
     * @param data
     *            The object used for retrieving the data.
     * @throws IOException
     *             In case the dummy events data can not be loaded.
     */
    public DummyServer(int port, DummyData data) throws IOException {
        this.port = port;
        this.data = data;
    }

    /**
     * Start listening on the port specified in the constructor and answer to
     * client requests.
     *
     * @throws IOException
     *             In case the server can not be started.
     */
    public void startListen() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/ok", this::handleStatusRequest);
        server.createContext("/events", this::handleDataRequest);
        server.start();
        data.startWorker();
        LOGGER.info("Server on port {} started. Access it at http://localhost:{}/", port, port);
    }

    /**
     * Stop the server from running.
     * 
     * @throws InterruptedException If interrupted.
     */
    public void stopListen() throws InterruptedException {
        data.stopWorker();
        server.stop((int) Duration.ofSeconds(1).toSeconds());
        server = null;
        LOGGER.info("Server on port {} stopped", port);
    }

    /**
     * Run the dummy GitHub Events API server.
     *
     * @param args
     *            Arguments to configure the server.
     */
    public static void main(String[] args) {
        // Parse command line
        ArgumentParser parser = ArgumentParsers.newFor("DummyServer").build()
                .description("Dummy server for serving a fake GitHub Events API. For testing purposes.");
        parser.addArgument("--port").metavar("PORT").type(Integer.class)
                .setDefault(8889).help("the HTTP port for the exposed HTTP server");
        parser.addArgument("--speed-up").metavar("MULTIPLE").type(Float.class)
                .setDefault(1.0f).help("speed up serving of events by the following multiple");
        parser.addArgument("--data-dir").metavar("DIR")
                .help("use this directory to cache data retrieved from the archive");
        parser.addArgument("--archive-url").metavar("URL").setDefault("https://data.gharchive.org")
                .help("use this url to access the GH Archive");
        parser.addArgument("--archive-delay").metavar("HOURS").setDefault(24)
                .help("the initial delay, in hours, between the real time and the served events");
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
        float speedUp = cmd.getFloat("speed_up");
        String dataDir = cmd.getString("data_dir");
        if (dataDir != null && dataDir.isBlank()) {
            dataDir = null;
        }
        String archiveUrl = cmd.getString("archive_url");
        if (archiveUrl != null && archiveUrl.isBlank()) {
            archiveUrl = null;
        }
        Duration archiveDelay = Duration.ofHours(cmd.getInt("archive_delay"));
        String logLevel = cmd.getString("log_level");
        // Configures logging
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.toLevel(logLevel));
        loggerContext.getLogger("com.rolandb").setLevel(Level.toLevel(logLevel));
        // Create and start HTTP server
        try {
            DummyData data = new DummyData(speedUp, archiveDelay, archiveUrl, dataDir);
            DummyServer server = new DummyServer(port, data);
            // Add a shutdown hook to ensure a clean exit.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down HTTP server");
                try {
                    server.stopListen();
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while stopping the server", e);
                }
            }));
            server.startListen();
        } catch (IOException e) {
            LOGGER.error("Failed to start server", e);
        }
    }
}
