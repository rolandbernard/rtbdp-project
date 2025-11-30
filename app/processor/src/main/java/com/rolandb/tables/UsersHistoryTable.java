package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

/**
 * This table computes windowed event counts per user using tumbling windows of
 * fixed size.
 */
public class UsersHistoryTable extends AbstractTable<UsersHistoryTable.UserEventCounts> {
    /** Type of event for this table. */
    public static class UserEventCounts extends SequencedRow {
        /** The user id. */
        @TableEventKey
        @JsonProperty("user_id")
        public long userId;
        /** The start of the window. */
        @TableEventKey
        @JsonProperty("ts_start")
        public Instant winStart;
        /** The end of the window. */
        @TableEventKey
        @JsonProperty("ts_end")
        public Instant winEnd;
        /** The number if events. */
        @JsonProperty("num_events")
        public long numEvents;

        /**
         * Create a new event instance-
         * 
         * @param winStart
         *            The window start.
         * @param winEnd
         *            The window end.
         * @param userId
         *            The user id.
         * @param numEvents
         *            The number of events for that user in that window.
         */
        public UserEventCounts(Instant winStart, Instant winEnd, long userId, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.userId = userId;
            this.numEvents = numEvents;
        }
    }

    private Duration window;

    /**
     * Create a new table with default values.
     */
    public UsersHistoryTable() {
        super();
        window = Duration.ofMinutes(5);
    }

    /**
     * Set the window size to use for aggregating the events.
     * 
     * @param window
     *            The window size.
     * @return {@code this}
     */
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
