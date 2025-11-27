package com.rolandb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventPollServiceTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private MockRestApiClient mockRestApiClient;
    private EventPollService eventPollService;

    static class MockRestApiClient extends RestApiClient {
        private final List<List<GithubEvent>> responses = new ArrayList<>();
        private int callCount = 0;

        public MockRestApiClient() {
            super("", "");
        }

        @Override
        public List<GithubEvent> getEvents(int page, int perPage) throws IOException, InterruptedException {
            assertEquals(page, 1);
            assertEquals(perPage, 10);
            if (callCount < responses.size()) {
                return responses.get(callCount++);
            }
            return new ArrayList<>();
        }

        public void addResponse(List<GithubEvent> response) {
            responses.add(new ArrayList<>(response));
        }

        public int getCallCount() {
            return callCount;
        }
    }

    @BeforeEach
    void setUp() {
        mockRestApiClient = new MockRestApiClient();
        eventPollService = new EventPollService(mockRestApiClient, 10, 10);
    }

    private GithubEvent createGithubEvent(String id, String type) {
        JsonNode rawEvent = objectMapper.valueToTree(Map.of("id", id, "type", type));
        return new GithubEvent(rawEvent);
    }

    @Test
    void testPollingAndEventEmission() throws InterruptedException {
        GithubEvent event1 = createGithubEvent("1", "PushEvent");
        GithubEvent event2 = createGithubEvent("2", "PullRequestEvent");
        mockRestApiClient.addResponse(List.of(event2, event1));
        mockRestApiClient.addResponse(List.of(event2, event1));
        List<GithubEvent> receivedEvents = new ArrayList<>();
        eventPollService.getEventsStream().subscribe(receivedEvents::add);
        eventPollService.startPolling();
        Thread.sleep(100);
        assertEquals(2, receivedEvents.size());
        assertEquals("1", receivedEvents.get(0).getId());
        assertEquals("2", receivedEvents.get(1).getId());
        assertTrue(mockRestApiClient.getCallCount() >= 2);
        eventPollService.stopPolling();
    }

    @Test
    void testDeDuplication() throws InterruptedException {
        GithubEvent event1 = createGithubEvent("1", "PushEvent");
        GithubEvent event2 = createGithubEvent("2", "PullRequestEvent");
        GithubEvent event3 = createGithubEvent("3", "ForkEvent");
        mockRestApiClient.addResponse(List.of(event2, event1));
        mockRestApiClient.addResponse(List.of(event2, event1));
        mockRestApiClient.addResponse(List.of(event3, event2, event1));
        List<GithubEvent> receivedEvents = new ArrayList<>();
        eventPollService.getEventsStream().subscribe(receivedEvents::add);
        eventPollService.startPolling();
        Thread.sleep(100);
        assertEquals(3, receivedEvents.size());
        assertEquals("1", receivedEvents.get(0).getId());
        assertEquals("2", receivedEvents.get(1).getId());
        assertEquals("3", receivedEvents.get(2).getId());
        eventPollService.stopPolling();
    }

    @Test
    void testUnmarkEvent() throws InterruptedException {
        GithubEvent event1 = createGithubEvent("1", "PushEvent");
        mockRestApiClient.addResponse(List.of(event1));
        mockRestApiClient.addResponse(List.of(event1));
        List<GithubEvent> receivedEvents = new ArrayList<>();
        eventPollService.getEventsStream().subscribe(receivedEvents::add);
        eventPollService.startPolling();
        Thread.sleep(100);
        assertEquals(1, receivedEvents.size());
        eventPollService.unmarkEvent(receivedEvents.get(0));
        mockRestApiClient.addResponse(List.of(event1));
        Thread.sleep(100);
        assertEquals(2, receivedEvents.size());
        assertEquals("1", receivedEvents.get(0).getId());
        assertEquals("1", receivedEvents.get(1).getId());
        eventPollService.stopPolling();
    }
}
