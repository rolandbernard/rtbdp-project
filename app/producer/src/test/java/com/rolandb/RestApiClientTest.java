package com.rolandb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class RestApiClientTest {
    private HttpServer server;
    private boolean limited = false;
    private String lastQuery = null;
    private String lastToken = null;
    private String dummyContent = "[{\"id\":\"123456789\",\"type\":\"PushEvent\",\"actor\":{\"login\":\"test\",\"id\":12345}}]";

    private void handleEventsRequest(HttpExchange exchange) throws IOException {
        lastQuery = exchange.getRequestURI().getQuery();
        lastToken = exchange.getRequestHeaders().getFirst("Authorization");
        if (limited) {
            exchange.getResponseHeaders().set("x-ratelimit-remaining", "0");
            exchange.getResponseHeaders().set("x-ratelimit-reset", "0");
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(429, 0);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(new byte[0]);
            }
        } else {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, dummyContent.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(dummyContent.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(12345), 0);
        server.createContext("/events", this::handleEventsRequest);
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(10);
    }

    @Test
    void testGetEvents() throws IOException, InterruptedException {
        RestApiClient client = new RestApiClient("http://localhost:12345", "github_dummy");
        List<GithubEvent> events = client.getEvents(1, 10);
        assertEquals(1, events.size());
        assertEquals("123456789", events.get(0).getId());
        assertEquals("page=1&per_page=10", lastQuery);
        assertEquals("token github_dummy", lastToken);
    }

    @Test
    void testRateLimit() throws IOException, InterruptedException {
        RestApiClient client = new RestApiClient("http://localhost:12345", "github_dummy");
        limited = true;
        RateLimitException e = assertThrows(RateLimitException.class, () -> {
            client.getEvents(1, 10);
        });
        assertEquals(Instant.EPOCH, e.getRetryAfter());
    }
}
