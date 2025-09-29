package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.DynamicRanking;
import com.rolandb.GithubEventType;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsRankingTable extends AbstractTable<CountsRankingTable.CountsRank> {
    public static class CountsRank extends SequencedRow {
        @JsonProperty("kind")
        public final GithubEventType eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @TableEventKey
        @JsonProperty("row_number")
        public final int rowNumber;
        @JsonProperty("rank")
        public final int rank;

        public CountsRank(GithubEventType eventType, WindowSize windowSize, int rowNumber, int rank) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.rowNumber = rowNumber;
            this.rank = rank;
        }
    }

    @Override
    protected DataStream<CountsRank> computeTable() {
        return getLiveEventCounts()
                .keyBy(e -> e.windowSize.toString())
                .process(
                        new DynamicRanking<>(
                                0L, Duration.ofSeconds(1), e -> e.eventType, e -> e.numEvents,
                                (w, k, v, row, rank, ts) -> {
                                    // We output a new ranking at discrete timestamps, and only
                                    // output once per timestamp. This means the timestamp could
                                    // be used as a sequence number.
                                    return new CountsRank(k, WindowSize.fromString(w), row, rank);
                                },
                                GithubEventType.class, Long.class))
                .setParallelism(4)
                .returns(CountsRank.class)
                .name("Event Count Rankings");
    }

    @Override
    protected Class<CountsRank> getOutputType() {
        return CountsRank.class;
    }
}
