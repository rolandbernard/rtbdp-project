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
public class GithubEvent extends SequencedRow {
    // These are all public so that this is a POJO.
    @JsonProperty("kind")
    public final GithubEventType eventType;
    @JsonProperty("created_at")
    public final Instant createdAt;
    @JsonProperty("user_id")
    public final long userId;
    @JsonProperty("repo_id")
    public final long repoId;

    public GithubEvent(GithubEventType eventType, Instant createdAt, long userId, long repoId, long seqNum) {
        this.seqNum = seqNum;
        this.eventType = eventType;
        this.createdAt = createdAt;
        this.userId = userId;
        this.repoId = repoId;
    }

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
        // Steal the sequence number from the raw event.
        seqNum = rawEvent.get("seq_num").asLong();
        switch (rawEvent.get("type").asText()) {
            case "CommitCommentEvent":
                eventType = GithubEventType.COMMIT_COMMENT;
                break;
            case "CreateEvent":
                switch (rawEvent.get("payload").get("ref_type").asText()) {
                    case "branch":
                        eventType = GithubEventType.CREATE_BRANCH;
                        break;
                    case "tag":
                        eventType = GithubEventType.CREATE_TAG;
                        break;
                    case "repository":
                        eventType = GithubEventType.CREATE_REPO;
                        break;
                    default:
                        eventType = GithubEventType.OTHER;
                        break;
                }
                break;
            case "ForkEvent":
                eventType = GithubEventType.FORK;
                break;
            case "GollumEvent":
                eventType = GithubEventType.WIKI;
                break;
            case "IssueCommentEvent":
                if (rawEvent.get("payload").get("action").asText().equals("created")) {
                    if (rawEvent.get("payload").get("issue").has("pull_request")) {
                        eventType = GithubEventType.PULL_COMMENT;
                    } else {
                        eventType = GithubEventType.ISSUE_COMMENT;
                    }
                } else {
                    eventType = GithubEventType.OTHER;
                }
                break;
            case "IssuesEvent":
                switch (rawEvent.get("payload").get("action").asText()) {
                    case "opened":
                    case "reopened":
                        eventType = GithubEventType.ISSUE_OPEN;
                        break;
                    case "closed":
                        eventType = GithubEventType.ISSUE_CLOSE;
                        break;
                    default:
                        eventType = GithubEventType.OTHER;
                        break;
                }
                break;
            case "PullRequestEvent":
                switch (rawEvent.get("payload").get("action").asText()) {
                    case "opened":
                    case "reopened":
                        eventType = GithubEventType.PULL_OPEN;
                        break;
                    case "closed":
                        eventType = GithubEventType.PULL_CLOSE;
                        break;
                    default:
                        eventType = GithubEventType.OTHER;
                        break;
                }
                break;
            case "PullRequestReviewCommentEvent":
                if (rawEvent.get("payload").get("action").asText().equals("created")) {
                    eventType = GithubEventType.PULL_COMMENT;
                } else {
                    eventType = GithubEventType.OTHER;
                }
                break;
            case "PushEvent":
                eventType = GithubEventType.PUSH;
                break;
            case "WatchEvent":
                eventType = GithubEventType.WATCH;
                break;
            default:
                eventType = GithubEventType.OTHER;
                break;
        }
        createdAt = Instant.parse(rawEvent.get("created_at").asText());
        userId = rawEvent.get("actor").get("id").asLong();
        repoId = rawEvent.get("repo").get("id").asLong();
    }
}
