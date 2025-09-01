package com.rolandb;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A Java object representing an event from the GitHub Events REST API. This
 * class stores only those fields that we actually need for processing, not any
 * additional information that we don't need.
 */
public class GithubEvent {
    // These are all public so that this is a POJO.
    @JsonProperty("kind")
    public final String eventType;
    @JsonProperty("created_at")
    public final Instant createdAt;
    @JsonProperty("user_id")
    public final long userId;
    @JsonProperty("repo_id")
    public final long repoId;

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
        GithubEventType type = null;
        switch (rawEvent.get("type").asText()) {
            case "CommitCommentEvent":
                type = GithubEventType.COMMIT_COMMENT;
                break;
            case "CreateEvent":
                switch (rawEvent.get("payload").get("ref_type").asText()) {
                    case "branch":
                        type = GithubEventType.CREATE_BRANCH;
                        break;
                    case "tag":
                        type = GithubEventType.CREATE_TAG;
                        break;
                    case "repository":
                        type = GithubEventType.CREATE_REPO;
                        break;
                }
                break;
            case "ForkEvent":
                type = GithubEventType.FORK;
                break;
            case "GollumEvent":
                type = GithubEventType.WIKI;
                break;
            case "IssueCommentEvent":
                if (rawEvent.get("payload").get("action").asText().equals("created")) {
                    if (rawEvent.get("payload").get("issue").has("pull_request")) {
                        type = GithubEventType.PULL_COMMENT;
                    } else {
                        type = GithubEventType.ISSUE_COMMENT;
                    }
                }
                break;
            case "IssuesEvent":
                switch (rawEvent.get("payload").get("action").asText()) {
                    case "opened":
                    case "reopened":
                        type = GithubEventType.ISSUE_OPEN;
                        break;
                    case "closed":
                        type = GithubEventType.ISSUE_CLOSE;
                        break;
                }
                break;
            case "PullRequestEvent":
                switch (rawEvent.get("payload").get("action").asText()) {
                    case "opened":
                    case "reopened":
                        type = GithubEventType.PULL_OPEN;
                        break;
                    case "closed":
                        type = GithubEventType.PULL_CLOSE;
                        break;
                }
                break;
            case "PullRequestReviewCommentEvent":
                if (rawEvent.get("payload").get("action").asText().equals("created")) {
                    type = GithubEventType.PULL_COMMENT;
                }
                break;
            case "PushEvent":
                type = GithubEventType.PUSH;
                break;
            case "WatchEvent":
                type = GithubEventType.WATCH;
                break;
        }
        if (type == null) {
            type = GithubEventType.OTHER;
        }
        eventType = type.toString();
        createdAt = Instant.parse(rawEvent.get("created_at").asText());
        userId = rawEvent.get("actor").get("id").asLong();
        repoId = rawEvent.get("repo").get("id").asLong();
    }

    /**
     * Get the {@link GithubEventType} of this event.
     *
     * @return The event type.
     */
    public GithubEventType getType() {
        return GithubEventType.fromString(eventType);
    }
}
