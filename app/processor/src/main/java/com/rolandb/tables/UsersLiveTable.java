package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This is a table that computes the number of events in the preceding windows
 * on a per-user granularity.
 */
public class UsersLiveTable extends AbstractTable<UsersLiveTable.UserEventCounts> {
    /** Type of event for this table. */
    public static class UserEventCounts extends SequencedRow {
        /** The user id. */
        @TableEventKey
        @JsonProperty("user_id")
        public long userId;
        /** Thw window size. */
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The number of events. */
        @JsonProperty("num_events")
        public long numEvents;

        /**
         * Create a new instance.
         * 
         * @param userId
         *            The user id.
         * @param windowSize
         *            The considered window size.
         * @param numEvents
         *            The number of events for that used in the latest window of the
         *            given size.
         */
        public UserEventCounts(long userId, WindowSize windowSize, long numEvents) {
            this.userId = userId;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    /**
     * Create a new table with default values.
     */
    public UsersLiveTable() {
        super();
    }

    @Override
    protected KeySelector<UserEventCounts, ?> tableOrderingKeySelector() {
        return row -> row.windowSize.toString();
    }

    @Override
    protected int tableParallelism() {
        return WindowSize.values().length;
    }

    @Override
    protected DataStream<UserEventCounts> computeTable() {
        return getEventsByUserStream()
                .process(new MultiSlidingBuckets<>(Duration.ofSeconds(1),
                        List.of(
                                WindowSize.MINUTES_5,
                                WindowSize.HOURS_1,
                                WindowSize.HOURS_6,
                                WindowSize.HOURS_24),
                        (windowStart, windowEnd, key, winSpec, count) -> {
                            return new UserEventCounts(key, winSpec, count);
                        }))
                .returns(UserEventCounts.class)
                .uid("live-user-counts-01")
                .name("Live per User Counts");
    }

    @Override
    protected Class<UserEventCounts> getOutputType() {
        return UserEventCounts.class;
    }
}
