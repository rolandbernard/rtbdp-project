package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

public class UsersHistoryTable extends AbstractTable<UsersHistoryTable.UserEventCounts> {
    public static class UserEventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("user_id")
        public final long userId;
        @TableEventKey
        @JsonProperty("ts_start")
        public final Instant winStart;
        @TableEventKey
        @JsonProperty("ts_end")
        public final Instant winEnd;
        @JsonProperty("num_events")
        public final long numEvents;

        public UserEventCounts(Instant winStart, Instant winEnd, long userId, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.userId = userId;
            this.numEvents = numEvents;
        }
    }

    private Duration window = Duration.ofMinutes(5);

    public UsersHistoryTable setWindowSize(Duration window) {
        this.window = window;
        return this;
    }

    @Override
    protected DataStream<UserEventCounts> computeTable() {
        return getEventsByUserStream()
                .window(TumblingEventTimeWindows.of(window))
                // Here we can afford to allow more lateness and retroactively
                // upsert with a new value.
                .allowedLateness(window.multipliedBy(10))
                .<Long, Long, UserEventCounts>aggregate(new CountAggregation<>(),
                        (key, window, elements, out) -> {
                            long count = elements.iterator().next();
                            out.collect(new UserEventCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, count));
                        })
                .returns(UserEventCounts.class)
                .uid("historical-user-counts-01-" + window.getSeconds())
                .name("Historical Counts by User");
    }

    @Override
    protected Class<UserEventCounts> getOutputType() {
        return UserEventCounts.class;
    }
}
