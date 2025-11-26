package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class GithubEventTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testDirectConstructor() {
        Instant now = Instant.now();
        GithubEvent event = new GithubEvent(GithubEventType.PUSH, now, 123L, 456L, 1L);
        assertEquals(GithubEventType.PUSH, event.eventType);
        assertEquals(now, event.createdAt);
        assertEquals(123L, event.userId);
        assertEquals(456L, event.repoId);
        assertEquals(1L, event.seqNum);
    }

    @Test
    public void testJsonCreatorCommitCommentEvent() throws Exception {
        String json = "{\"seq_num\": 1, \"type\": \"CommitCommentEvent\", \"created_at\": \"2023-01-01T10:00:00Z\", \"actor\": {\"id\": 123}, \"repo\": {\"id\": 456}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.COMMIT_COMMENT, event.eventType);
        assertEquals(Instant.parse("2023-01-01T10:00:00Z"), event.createdAt);
        assertEquals(123L, event.userId);
        assertEquals(456L, event.repoId);
        assertEquals(1L, event.seqNum);
    }

    @Test
    public void testJsonCreatorCreateEventRepo() throws Exception {
        String json = "{\"seq_num\": 2, \"type\": \"CreateEvent\", \"created_at\": \"2023-01-01T11:00:00Z\", \"actor\": {\"id\": 124}, \"repo\": {\"id\": 457}, \"payload\": {\"ref_type\": \"repository\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.CREATE_REPO, event.eventType);

        json = "{\"seq_num\": 3, \"type\": \"CreateEvent\", \"created_at\": \"2023-01-01T11:00:00Z\", \"actor\": {\"id\": 124}, \"repo\": {\"id\": 457}, \"payload\": {\"ref_type\": \"branch\", \"ref\": \"master\", \"master_branch\": \"master\"}}";
        event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.CREATE_REPO, event.eventType);
    }

    @Test
    public void testJsonCreatorCreateEventBranch() throws Exception {
        String json = "{\"seq_num\": 3, \"type\": \"CreateEvent\", \"created_at\": \"2023-01-01T11:00:00Z\", \"actor\": {\"id\": 124}, \"repo\": {\"id\": 457}, \"payload\": {\"ref_type\": \"branch\", \"ref\": \"feature\", \"master_branch\": \"master\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.CREATE_BRANCH, event.eventType);
    }

    @Test
    public void testJsonCreatorDeleteEventBranch() throws Exception {
        String json = "{\"seq_num\": 4, \"type\": \"DeleteEvent\", \"created_at\": \"2023-01-01T12:00:00Z\", \"actor\": {\"id\": 125}, \"repo\": {\"id\": 458}, \"payload\": {\"ref_type\": \"branch\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.DELETE_BRANCH, event.eventType);
    }

    @Test
    public void testJsonCreatorForkEvent() throws Exception {
        String json = "{\"seq_num\": 5, \"type\": \"ForkEvent\", \"created_at\": \"2023-01-01T13:00:00Z\", \"actor\": {\"id\": 126}, \"repo\": {\"id\": 459}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.FORK, event.eventType);
    }

    @Test
    public void testJsonCreatorGollumEvent() throws Exception {
        String json = "{\"seq_num\": 6, \"type\": \"GollumEvent\", \"created_at\": \"2023-01-01T14:00:00Z\", \"actor\": {\"id\": 127}, \"repo\": {\"id\": 460}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.WIKI, event.eventType);
    }

    @Test
    public void testJsonCreatorIssueCommentEventPull() throws Exception {
        String json = "{\"seq_num\": 7, \"type\": \"IssueCommentEvent\", \"created_at\": \"2023-01-01T15:00:00Z\", \"actor\": {\"id\": 128}, \"repo\": {\"id\": 461}, \"payload\": {\"action\": \"created\", \"issue\": {\"pull_request\": {}}}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.PULL_COMMENT, event.eventType);
    }

    @Test
    public void testJsonCreatorIssueCommentEventIssue() throws Exception {
        String json = "{\"seq_num\": 8, \"type\": \"IssueCommentEvent\", \"created_at\": \"2023-01-01T16:00:00Z\", \"actor\": {\"id\": 129}, \"repo\": {\"id\": 462}, \"payload\": {\"action\": \"created\", \"issue\": {}}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.ISSUE_COMMENT, event.eventType);
    }

    @Test
    public void testJsonCreatorIssuesEventOpened() throws Exception {
        String json = "{\"seq_num\": 9, \"type\": \"IssuesEvent\", \"created_at\": \"2023-01-01T17:00:00Z\", \"actor\": {\"id\": 130}, \"repo\": {\"id\": 463}, \"payload\": {\"action\": \"opened\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.ISSUE_OPEN, event.eventType);
    }

    @Test
    public void testJsonCreatorIssuesEventClosed() throws Exception {
        String json = "{\"seq_num\": 10, \"type\": \"IssuesEvent\", \"created_at\": \"2023-01-01T18:00:00Z\", \"actor\": {\"id\": 131}, \"repo\": {\"id\": 464}, \"payload\": {\"action\": \"closed\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.ISSUE_CLOSE, event.eventType);
    }

    @Test
    public void testJsonCreatorPullRequestEventOpened() throws Exception {
        String json = "{\"seq_num\": 11, \"type\": \"PullRequestEvent\", \"created_at\": \"2023-01-01T19:00:00Z\", \"actor\": {\"id\": 132}, \"repo\": {\"id\": 465}, \"payload\": {\"action\": \"opened\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.PULL_OPEN, event.eventType);
    }

    @Test
    public void testJsonCreatorPullRequestEventClosed() throws Exception {
        String json = "{\"seq_num\": 12, \"type\": \"PullRequestEvent\", \"created_at\": \"2023-01-01T20:00:00Z\", \"actor\": {\"id\": 133}, \"repo\": {\"id\": 466}, \"payload\": {\"action\": \"closed\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.PULL_CLOSE, event.eventType);
    }

    @Test
    public void testJsonCreatorPullRequestReviewCommentEvent() throws Exception {
        String json = "{\"seq_num\": 13, \"type\": \"PullRequestReviewCommentEvent\", \"created_at\": \"2023-01-01T21:00:00Z\", \"actor\": {\"id\": 134}, \"repo\": {\"id\": 467}, \"payload\": {\"action\": \"created\"}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.PULL_COMMENT, event.eventType);
    }

    @Test
    public void testJsonCreatorPushEvent() throws Exception {
        String json = "{\"seq_num\": 14, \"type\": \"PushEvent\", \"created_at\": \"2023-01-01T22:00:00Z\", \"actor\": {\"id\": 135}, \"repo\": {\"id\": 468}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.PUSH, event.eventType);
    }

    @Test
    public void testJsonCreatorWatchEvent() throws Exception {
        String json = "{\"seq_num\": 15, \"type\": \"WatchEvent\", \"created_at\": \"2023-01-01T23:00:00Z\", \"actor\": {\"id\": 136}, \"repo\": {\"id\": 469}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.WATCH, event.eventType);
    }

    @Test
    public void testJsonCreatorOtherEvent() throws Exception {
        String json = "{\"seq_num\": 16, \"type\": \"SomeOtherEvent\", \"created_at\": \"2023-01-02T00:00:00Z\", \"actor\": {\"id\": 137}, \"repo\": {\"id\": 470}}";
        GithubEvent event = objectMapper.readValue(json, GithubEvent.class);
        assertEquals(GithubEventType.OTHER, event.eventType);
    }
}
