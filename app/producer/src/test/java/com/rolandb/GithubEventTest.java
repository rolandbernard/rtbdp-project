package com.rolandb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GithubEventTest {

    @Test
    void testConstructor() throws JsonMappingException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String dummyEvent = "{\"id\":\"123456789\",\"type\":\"PushEvent\",\"actor\":{\"login\":\"test\",\"id\":12345}}";
        GithubEvent event = mapper.readValue(dummyEvent, GithubEvent.class);
        assertEquals("123456789", event.getId());
        assertEquals(mapper.readTree(dummyEvent), event.getRawEvent());
    }

    @Test
    void testConstructorWithNullJson() {
        assertThrows(NullPointerException.class, () -> {
            new GithubEvent(null);
        });
    }

    @Test
    void testConstructorWithMissingId() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        json.put("type", "IssueCommentEvent");
        GithubEvent event = new GithubEvent(json);
        assertEquals("", event.getId());
    }
}
