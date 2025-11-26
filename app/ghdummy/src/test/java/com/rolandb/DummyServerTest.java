package com.rolandb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class DummyServerTest {
    private DummyData dummyData;
    private DummyServer server;

    @BeforeEach
    public void setUp() throws Exception {
        dummyData = new DummyData(1.0f, Duration.ofMinutes(1), null, null);
        server = new DummyServer(12345, dummyData);
        server.startListen();
    }

    @AfterEach
    public void tearDown() throws Exception {
        server.stopListen();
    }

    @Test
    void testHealthCheck() throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:12345/ok"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.body());
    }

    @Test
    void testEventsNoToken() throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:12345/events"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(403, response.statusCode());
    }

    @Test
    void testEventsWithToken() throws IOException, InterruptedException {
        dummyData.loadCurrentData();
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:12345/events"))
                .GET()
                .header("Authorization", "token github_dummy")
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertDoesNotThrow(() -> {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode node = objectMapper.readTree(response.body());
            assertTrue(node.isArray());
            assertTrue(node.get(0).isObject());
            assertTrue(node.get(0).get("id").isTextual());
        });
    }
}
