package com.rolandb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GithubEventTypeTest {

    @Test
    void testEnumValues() {
        GithubEventType[] types = GithubEventType.values();
        assertEquals(16, types.length);
    }

    @Test
    void testToString() {
        assertEquals("all", GithubEventType.ALL.toString());
        assertEquals("push", GithubEventType.PUSH.toString());
        assertEquals("watch", GithubEventType.WATCH.toString());
        assertEquals("create_repo", GithubEventType.CREATE_REPO.toString());
        assertEquals("create_branch", GithubEventType.CREATE_BRANCH.toString());
        assertEquals("delete_branch", GithubEventType.DELETE_BRANCH.toString());
        assertEquals("fork", GithubEventType.FORK.toString());
        assertEquals("wiki", GithubEventType.WIKI.toString());
        assertEquals("issue_open", GithubEventType.ISSUE_OPEN.toString());
        assertEquals("issue_close", GithubEventType.ISSUE_CLOSE.toString());
        assertEquals("pull_open", GithubEventType.PULL_OPEN.toString());
        assertEquals("pull_close", GithubEventType.PULL_CLOSE.toString());
        assertEquals("commit_comment", GithubEventType.COMMIT_COMMENT.toString());
        assertEquals("issue_comment", GithubEventType.ISSUE_COMMENT.toString());
        assertEquals("pull_comment", GithubEventType.PULL_COMMENT.toString());
        assertEquals("other", GithubEventType.OTHER.toString());
    }

    @Test
    void testFromStringValid() {
        assertEquals(GithubEventType.ALL, GithubEventType.fromString("all"));
        assertEquals(GithubEventType.PUSH, GithubEventType.fromString("push"));
        assertEquals(GithubEventType.WATCH, GithubEventType.fromString("watch"));
        assertEquals(GithubEventType.CREATE_REPO, GithubEventType.fromString("create_repo"));
        // .. Some skipped
        assertEquals(GithubEventType.OTHER, GithubEventType.fromString("other"));
    }

    @Test
    void testFromStringInvalid() {
        assertThrows(IllegalArgumentException.class, () -> {
            GithubEventType.fromString("invalid_type");
        });
    }
}
