package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.rolandb.AbstractUpdateTable;
import com.rolandb.AbstractUpdateTable.UpdateSeqRow;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

public class UsersTable extends AbstractUpdateTable<UsersTable.UserUpdateEvent> {
    public static class UserUpdateEvent extends UpdateSeqRow {
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
            if (rawUser.isObject() && rawUser.has("id")) {
                long userId = rawUser.at("/id").asLong();
                String username = null;
                String avatarUrl = null;
                String htmlUrl = null;
                String userType = null;
                if (rawUser.has("login")) {
                    username = rawUser.at("/login").asText();
                } else if (rawUser.has("display_login")) {
                    username = rawUser.at("/display_login").asText();
                }
                if (rawUser.has("avatar_url")) {
                    avatarUrl = rawUser.at("/avatar_url").asText();
                }
                if (rawUser.has("html_url")) {
                    htmlUrl = rawUser.at("/html_url").asText();
                }
                if (rawUser.has("type")) {
                    userType = rawUser.at("/type").asText();
                }
                events.add(new UserUpdateEvent(userId, username, avatarUrl, htmlUrl, userType));
            }
        }

        private static void readFromRepoObject(JsonNode rawRepo, List<UserUpdateEvent> events) {
            if (rawRepo.isObject()) {
                readFromUserObject(rawRepo.at("/owner"), events);
            }
        }

        private static void readFromBranchObject(JsonNode rawHead, List<UserUpdateEvent> events) {
            if (rawHead.isObject()) {
                readFromUserObject(rawHead.at("/user"), events);
                readFromRepoObject(rawHead.at("/repo"), events);
            }
        }

        private static void readFromIssueObject(JsonNode rawIssue, List<UserUpdateEvent> events) {
            if (rawIssue.isObject()) {
                readFromUserObject(rawIssue.at("/user"), events);
                readFromUserObject(rawIssue.at("/assignee"), events);
                JsonNode assignees = rawIssue.at("/assignees");
                if (assignees.isArray()) {
                    assignees.valueStream().forEach(a -> {
                        readFromUserObject(a, events);
                    });
                }
                JsonNode reviewers = rawIssue.at("/requested_reviewers");
                if (reviewers.isArray()) {
                    reviewers.valueStream().forEach(a -> {
                        readFromUserObject(a, events);
                    });
                }
                readFromBranchObject(rawIssue.at("/head"), events);
                readFromBranchObject(rawIssue.at("/base"), events);
                readFromUserObject(rawIssue.at("/merged_by"), events);
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
            readFromUserObject(rawEvent.at("/actor"), events);
            readFromUserObject(rawEvent.at("/org"), events);
            JsonNode payload = rawEvent.at("/payload");
            if (payload.isObject()) {
                JsonNode comment = payload.at("/comment");
                if (comment.isObject()) {
                    readFromUserObject(comment.at("/user"), events);
                    JsonNode app = payload.at("/performed_via_github_app");
                    if (app.isObject()) {
                        readFromUserObject(app.at("/owner"), events);
                    }
                }
                JsonNode review = payload.at("/review");
                if (review.isObject()) {
                    readFromUserObject(review.at("/user"), events);
                }
                JsonNode release = payload.at("/release");
                if (release.isObject()) {
                    readFromUserObject(release.at("/author"), events);
                }
                readFromIssueObject(payload.at("/issue"), events);
                readFromIssueObject(payload.at("/pull_request"), events);
                readFromRepoObject(payload.at("/forkee"), events);
                readFromUserObject(payload.at("/member"), events);
            }
            return events;
        }
    }

    @Override
    protected DataStream<UserUpdateEvent> computeTable() {
        return getRawEventStream()
                .<UserUpdateEvent>flatMap((rawEvent, out) -> {
                    long seqNum = rawEvent.at("/seq_num").asLong();
                    for (UserUpdateEvent event : UserUpdateEvent.readFromRawEvent(rawEvent)) {
                        event.seqNum = seqNum;
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
