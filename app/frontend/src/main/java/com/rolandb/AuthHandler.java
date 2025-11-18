package com.rolandb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * A simple file server that serves static files from the resources location. It
 * will read the file and respond with those contents. It also includes cache
 * control headers to allow for browsers to cache the files.
 */
public class AuthHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthHandler.class);

    private final String password;
    private final String secret;
    private final HttpHandler wrapped;

    public AuthHandler(String password, HttpHandler wrapped) {
        this.password = password.trim();
        this.wrapped = wrapped;
        if (password != null && !password.isEmpty()) {
            try {
                byte[] salt = new byte[64];
                SecureRandom.getInstanceStrong().nextBytes(salt);
                MessageDigest md = MessageDigest.getInstance("SHA-512");
                md.update(password.getBytes(StandardCharsets.UTF_8));
                md.update(salt);
                byte[] digest = md.digest();
                this.secret = Base64.getUrlEncoder().encodeToString(digest);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Failed to generate hashed secret", e);
            }
        } else {
            secret = null;
        }
    }

    public String getSecret() {
        return secret;
    }

    private String getLoginPageTemplate() throws IOException {
        return new String(
                AuthHandler.class.getResourceAsStream("login.html").readAllBytes(),
                StandardCharsets.UTF_8);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/login")) {
                String message = "";
                if (exchange.getRequestMethod().equals("POST")) {
                    byte[] bytes = exchange.getRequestBody().readAllBytes();
                    String body = new String(bytes, StandardCharsets.UTF_8);
                    String decoded = URLDecoder.decode(body, "UTF-8");
                    if (decoded.equals("secret=" + password)) {
                        message = "correct password";
                    } else {
                        message = "wrong password";
                    }
                }
                if (secret == null || message.equals("correct password")) {
                    String url = "/";
                    String query = exchange.getRequestURI().getQuery();
                    if (query != null) {
                        for (String param : query.split("&")) {
                            String[] entry = param.split("=");
                            if (URLDecoder.decode(entry[0], "UTF-8").equals("url") && entry.length > 1) {
                                url = URLDecoder.decode(entry[1], "UTF-8");
                            }
                        }
                    }
                    exchange.getResponseHeaders().add("Location", url.isEmpty() ? "/" : url);
                    exchange.getResponseHeaders().add("Set-Cookie",
                            "rtgh_secret=" + secret + "; Secure; HTTPOnly; SameSite=Lax");
                    String response = "303 See Other";
                    exchange.sendResponseHeaders(303, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                    return;
                }
                String response = getLoginPageTemplate().replace("{{message}}", message);
                exchange.getResponseHeaders().set("Content-Type", "text/html");
                exchange.sendResponseHeaders(401, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                return;
            } else if (secret != null) {
                boolean hasSecret = false;
                for (String cookie : exchange.getRequestHeaders().getOrDefault("Cookie", List.of())) {
                    if (cookie.contains(secret)) {
                        hasSecret = true;
                    }
                }
                if (!hasSecret) {
                    exchange.getResponseHeaders().add("Location", "/login?url=" + URLEncoder.encode(path, "UTF-8"));
                    String response = "303 See Other";
                    exchange.sendResponseHeaders(303, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                    return;
                }
            }
        } catch (RuntimeException | IOException ex) {
            LOGGER.error("Failed to handle request", ex);
        }
        wrapped.handle(exchange);
    }
}
