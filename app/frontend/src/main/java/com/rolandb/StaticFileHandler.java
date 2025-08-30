package com.rolandb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

/**
 * A simple file server that serves static files from the resources location. It
 * will read the file and respond with those contents. It also includes cache
 * control headers to allow for browsers to cache the files.
 */
public class StaticFileHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StaticFileHandler.class);
    /**
     * The data format to be used in `Last-Modified` and `If-Modified-Since` HTTP
     * headers.
     */
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
    private static final String RESOURCE_NAME = "static";
    /**
     * This is a mapping from file extension to MIME types. This is so that we
     * can give the correct Content-Type header. This is not really required by
     * browsers, but is a nice to have.
     */
    private static final Map<String, String> MIME_TYPES = new HashMap<>();

    static {
        MIME_TYPES.put("html", "text/html");
        MIME_TYPES.put("css", "text/css");
        MIME_TYPES.put("js", "application/javascript");
        MIME_TYPES.put("png", "image/png");
        MIME_TYPES.put("jpg", "image/jpeg");
        MIME_TYPES.put("ico", "image/x-icon");
        MIME_TYPES.put("svg", "image/svg+xml");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String uriPath = exchange.getRequestURI().getPath();
            if (uriPath.equals("/")) {
                // For the root just serve `index.html`.
                uriPath = "/index.html";
            }
            LOGGER.info("Handling data request from {} for {}", exchange.getRemoteAddress(), uriPath);
            // Normalize the path to avoid directory traversal vulnerabilities.
            // The root will swallow all excess `..', and only then do we add the
            // resource folder name.
            assert uriPath.charAt(1) == '/';
            String normalizedPath = Path.of(uriPath).normalize().toString();
            String resourcePath = RESOURCE_NAME + normalizedPath;
            URL url = StaticFileHandler.class.getResource(resourcePath);
            if (url == null) {
                sendNotFound(exchange);
                return;
            } else {
                URLConnection connection = url.openConnection();
                // Check for If-Modified-Since header
                long lastModifiedMillis = connection.getLastModified();
                String clientModified = exchange.getRequestHeaders().getFirst("If-Modified-Since");
                if (clientModified != null) {
                    try {
                        Date clientDate = DATE_FORMAT.parse(clientModified);
                        if (clientDate.getTime() >= lastModifiedMillis) {
                            // Return HTTP 304 in case of no modification
                            exchange.sendResponseHeaders(304, -1);
                            return;
                        }
                    } catch (ParseException e) {
                        // Invalid date, ignore the header
                    }
                }
                byte[] content = connection.getInputStream().readAllBytes();
                // Check for If-None-Match header
                String eTag = generateEtag(content);
                String clientETag = exchange.getRequestHeaders().getFirst("If-None-Match");
                if (clientETag != null && clientETag.equals(eTag)) {
                    // Return HTTP 304 in case of matching tags
                    exchange.sendResponseHeaders(304, -1);
                    return;
                }
                // Add some cache control headers.
                exchange.getResponseHeaders().set("Cache-Control", "public, max-age=86400");
                if (lastModifiedMillis != 0) {
                    String lastModified = DATE_FORMAT.format(Date.from(Instant.ofEpochMilli(lastModifiedMillis)));
                    exchange.getResponseHeaders().set("Last-Modified", lastModified);
                }
                exchange.getResponseHeaders().set("ETag", eTag);
                // Determine MIME type based on file extension or fallback to octet-stream.
                String fileExtension = getFileExtension(uriPath);
                String mimeType = MIME_TYPES.getOrDefault(fileExtension, "application/octet-stream");
                exchange.getResponseHeaders().set("Content-Type", mimeType);
                exchange.sendResponseHeaders(200, content.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(content);
                }
            }
        } catch (RuntimeException | IOException ex) {
            LOGGER.error("Failed to handle request", ex);
            sendInternalServerError(exchange);
        }
    }

    private void sendNotFound(HttpExchange exchange) throws IOException {
        String response = "404 Not Found";
        exchange.sendResponseHeaders(404, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    private void sendInternalServerError(HttpExchange exchange) throws IOException {
        String response = "500 Internal Server Error";
        exchange.sendResponseHeaders(500, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    /**
     * A simple function to extract from a string the part after the last
     * occurrence of '.', yielding what we would normally call the file extension.
     * If there is no '.' after the last '/', then an empty string will be returned.
     * 
     * @param fileName
     *            The filename
     * @return The extension of the filename
     */
    private static String getFileExtension(String fileName) {
        int slashIndex = fileName.lastIndexOf('/');
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > slashIndex && dotIndex < fileName.length() - 1) {
            return fileName.substring(dotIndex + 1).toLowerCase();
        } else {
            return "";
        }
    }

    /**
     * Generate a MD5 hash value value for the HTTP `ETag` header. This is for
     * better caching on the browser side.
     * 
     * @param content
     *            The content of the file we want to get an ETag for.
     * @return The `ETag` header value.
     */
    private static String generateEtag(byte[] content) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(content);
            return "\"" + Base64.getEncoder().encodeToString(digest) + "\"";
        } catch (NoSuchAlgorithmException e) {
            // This should really never happen
            throw new IllegalStateException("Can't load MD5 algorithm", e);
        }
    }
}
