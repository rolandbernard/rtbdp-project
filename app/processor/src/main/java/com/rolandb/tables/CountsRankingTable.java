package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.DynamicRanking;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.MultiSlidingBuckets.WindowSpec;

import java.time.Duration;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsRankingTable extends AbstractTableBuilder {
    public static class EventCounts {
        public final String eventType;
        public final String windowSize;
        public final int numEvents;

        public EventCounts(String eventType, String windowSize, int numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    public static class CountsRank {
        @TableEventKey
        @JsonProperty("kind")
        public final String eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public final String windowSize;
        @JsonProperty("row_number")
        public final int rowNumber;
        @JsonProperty("rank")
        public final int rank;
        @JsonProperty("num_events")
        public final int numEvents;

        public CountsRank(String eventType, String windowSize, int rowNumber, int rank, int numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.rowNumber = rowNumber;
            this.rank = rank;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<CountsRank> computeTable() {
        return getEventsByTypeStream()
                .process(new MultiSlidingBuckets<>(Duration.ofSeconds(1),
                        List.of(
                                new WindowSpec("5m", Duration.ofMinutes(5)),
                                new WindowSpec("1h", Duration.ofHours(1)),
                                new WindowSpec("6h", Duration.ofHours(6)),
                                new WindowSpec("24h", Duration.ofHours(24))),
                        (windowStart, windowEnd, key, winSpec, count) -> {
                            return new EventCounts(key, winSpec.name, count.intValue());
                        }))
                .returns(EventCounts.class)
                .name("Live Event Counts")
                .keyBy(e -> e.windowSize)
                .process(
                        new DynamicRanking<>(
                                0, Duration.ofMillis(50), e -> e.eventType, e -> e.numEvents,
                                (w, k, v, row, rank) -> {
                                    return new CountsRank(k, w, row, rank, v);
                                },
                                String.class, Integer.class))
                .returns(CountsRank.class)
                .name("Event Count Rankings");
    }

    @Override
    protected Class<?> getOutputType() {
        return CountsRank.class;
    }
}
