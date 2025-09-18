package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.rolandb.AbstractUpdateTable;
import com.rolandb.SequencedRow;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

public class UsersTable extends AbstractUpdateTable<UsersTable.UserUpdateEvent> {
    public static class UserUpdateEvent extends SequencedRow {
        @TableEventKey
        @JsonProperty("id")
        public final long id;
        @JsonProperty("username")
        public final String username;
        @JsonProperty("avatar_url")
        public final String avatarUrl;
        @JsonProperty("html_url")
        public final String htmlUrl;
        @JsonProperty("user_type")
        public final String userType;

        public UserUpdateEvent(long id, String username, String avatarUrl, String htmlUrl, String userType) {
            this.id = id;
            this.username = username;
            this.avatarUrl = avatarUrl;
            this.htmlUrl = htmlUrl;
            this.userType = userType;
        }

        private static void readFromUserObject(JsonNode rawUser, List<UserUpdateEvent> events) {
            if (rawUser != null && rawUser.isObject() && rawUser.has("id")) {
                long userId = rawUser.get("id").asLong();
                String username = null;
                String avatarUrl = null;
                String htmlUrl = null;
                String userType = null;
                if (rawUser.has("login")) {
                    username = rawUser.get("login").asText();
                } else if (rawUser.has("display_login")) {
                    username = rawUser.get("display_login").asText();
                }
                if (rawUser.has("avatar_url")) {
                    avatarUrl = rawUser.get("avatar_url").asText();
                }
                if (rawUser.has("html_url")) {
                    htmlUrl = rawUser.get("html_url").asText();
                }
                if (rawUser.has("type")) {
                    userType = rawUser.get("type").asText();
                }
                events.add(new UserUpdateEvent(userId, username, avatarUrl, htmlUrl, userType));
            }
        }

        private static void readFromRepoObject(JsonNode rawRepo, List<UserUpdateEvent> events) {
            if (rawRepo != null) {
                readFromUserObject(rawRepo.get("owner"), events);
            }
        }

        private static void readFromBranchObject(JsonNode rawHead, List<UserUpdateEvent> events) {
            if (rawHead != null) {
                readFromUserObject(rawHead.get("user"), events);
                readFromRepoObject(rawHead.get("repo"), events);
            }
        }

        private static void readFromIssueObject(JsonNode rawIssue, List<UserUpdateEvent> events) {
            if (rawIssue != null) {
                readFromUserObject(rawIssue.get("user"), events);
                readFromUserObject(rawIssue.get("assignee"), events);
                JsonNode assignees = rawIssue.get("assignees");
                if (assignees != null && assignees.isArray()) {
                    assignees.valueStream().forEach(a -> {
                        readFromUserObject(a, events);
                    });
                }
                JsonNode reviewers = rawIssue.get("requested_reviewers");
                if (reviewers != null && reviewers.isArray()) {
                    reviewers.valueStream().forEach(a -> {
                        readFromUserObject(a, events);
                    });
                }
                readFromBranchObject(rawIssue.get("head"), events);
                readFromBranchObject(rawIssue.get("base"), events);
                readFromUserObject(rawIssue.get("merged_by"), events);
            }
        }

        /**
         * Constructor to create a number of user update events. A single raw event may
         * contain more than a single users information, so this returns a list of
         * updates.
         *
         * @param rawEvent
         *            The raw event JSON object.
         * @return A list of user updates that can be learned from the raw event.
         */
        public static List<UserUpdateEvent> readFromRawEvent(JsonNode rawEvent) {
            List<UserUpdateEvent> events = new ArrayList<>();
            readFromUserObject(rawEvent.get("actor"), events);
            readFromUserObject(rawEvent.get("org"), events);
            JsonNode payload = rawEvent.get("payload");
            if (payload != null) {
                JsonNode comment = payload.get("comment");
                if (comment != null) {
                    readFromUserObject(comment.get("user"), events);
                    JsonNode app = payload.get("performed_via_github_app");
                    if (app != null) {
                        readFromUserObject(app.get("owner"), events);
                    }
                }
                JsonNode review = payload.get("review");
                if (review != null) {
                    readFromUserObject(review.get("user"), events);
                }
                JsonNode release = payload.get("release");
                if (release != null) {
                    readFromUserObject(release.get("author"), events);
                }
                readFromIssueObject(payload.get("issue"), events);
                readFromIssueObject(payload.get("pull_request"), events);
                readFromRepoObject(payload.get("forkee"), events);
                readFromUserObject(payload.get("member"), events);
            }
            return events;
        }
    }

    @Override
    protected DataStream<UserUpdateEvent> computeTable() {
        return getRawEventStream()
                .<UserUpdateEvent>flatMap((jsonNode, out) -> {
                    for (UserUpdateEvent event : UserUpdateEvent.readFromRawEvent(jsonNode)) {
                        out.collect(event);
                    }
                })
                .returns(UserUpdateEvent.class)
                .name("User Update Stream");
    }

    @Override
    protected Class<UserUpdateEvent> getOutputType() {
        return UserUpdateEvent.class;
    }
}
