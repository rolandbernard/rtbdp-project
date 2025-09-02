package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.EventCounts;

import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsRankingTable extends AbstractTableBuilder {
    public static class CountsRank {
        @JsonProperty("ts_start")
        public final Instant winStart;
        @JsonProperty("ts_end")
        public final Instant winEnd;
        @JsonProperty("kind")
        public final String eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public final String windowSize;
        @TableEventKey
        @JsonProperty("row_number")
        public final int rowNumber;
        @JsonProperty("rank")
        public final int rank;
        @JsonProperty("num_events")
        public final int numEvents;

        public CountsRank(
                Instant winStart, Instant winEnd, String eventType, String windowSize, int rowNumber, int rank,
                int numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.rowNumber = rowNumber;
            this.rank = rank;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<CountsRank> computeTable() {
        return getCountsLiveStream()
                .filter(e -> e.windowSize.equals("5m"))
                .keyBy(e -> e.windowSize)
                .process(
                        new DynamicRanking<>(
                                0, e -> e.eventType, e -> e.numEvents, (a, b) -> a.winEnd != b.winEnd,
                                (e, k, v, row, rank) -> {
                                    return new CountsRank(e.winStart, e.winEnd, k, e.windowSize, row, rank, v);
                                },
                                String.class, Integer.class, EventCounts.class))
                .returns(CountsRank.class);
    }

    @Override
    protected Class<?> getOutputType() {
        return CountsRank.class;
    }
}
