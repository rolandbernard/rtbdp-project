package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.rolandb.AbstractUpdateTable;
import com.rolandb.SequencedRow;

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
            // TODO
            return List.of();
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
