package com.rolandb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A Java object representing an event from the GitHub Events REST API. This
 * class stores the entire JSON payload for all events, extracting only the
 * unique {@code id} field for each, as it is required for deduplication. We
 * don't parse the complete structure of the events since it is not required.
 */
public class GithubEvent {
    private final String id;
    private final JsonNode rawEvent;
    // A sequence number that is guaranteed to not appear twice in the stream of
    // events and is in process time order. This is currently generated from the
    // current timestamp, but should not be interpreted that way. This is used
    // in later processing to ensure ordering of updates.
    public long seqNum;

    /**
     * Constructor to create a new event from the raw event data. This constructor
     * is marked with {@code @JsonCreator} so that it will be used automatically
     * for deserialization.
     *
     * @param rawEvent
     *            The raw event JSON object.
     */
    @JsonCreator
    public GithubEvent(JsonNode rawEvent) {
        this.rawEvent = rawEvent;
        id = rawEvent.get("id").asText();
    }

    /**
     * Get the ID of this event.
     *
     * @return The event id.
     */
    public String getId() {
        return id;
    }

    /**
     * Get the raw event JSON object.
     *
     * @return The raw event data.
     */
    public JsonNode getRawEvent() {
        return rawEvent;
    }
}
