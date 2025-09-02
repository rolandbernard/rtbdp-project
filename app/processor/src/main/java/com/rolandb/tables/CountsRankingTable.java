package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.EventCounts;

import java.time.Duration;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsRankingTable extends AbstractTableBuilder {
    public static class CountsRank {
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

        public CountsRank(String eventType, String windowSize, int rowNumber, int rank) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.rowNumber = rowNumber;
            this.rank = rank;
        }
    }

    @Override
    protected DataStream<CountsRank> computeTable() {
        return getCountsLiveStream()
                .keyBy(e -> e.windowSize)
                .process(
                        new DynamicRanking<>(
                                0, Duration.ofMillis(250), e -> e.eventType, e -> e.numEvents,
                                (e, k, v, row, rank) -> {
                                    return new CountsRank(k, e.windowSize, row, rank);
                                },
                                String.class, Integer.class, EventCounts.class))
                .returns(CountsRank.class)
                .name("Event Count Rankings");
    }

    @Override
    protected Class<?> getOutputType() {
        return CountsRank.class;
    }
}
