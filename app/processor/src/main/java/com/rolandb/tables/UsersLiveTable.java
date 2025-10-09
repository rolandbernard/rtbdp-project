package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

public class UsersLiveTable extends AbstractTable<UsersLiveTable.UserEventCounts> {
    public static class UserEventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("user_id")
        public final long userId;
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("num_events")
        public final long numEvents;

        public UserEventCounts(long userId, WindowSize windowSize, long numEvents) {
            this.userId = userId;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
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
                .name("Live per User Counts");
    }

    @Override
    protected Class<UserEventCounts> getOutputType() {
        return UserEventCounts.class;
    }
}
