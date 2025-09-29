package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.DynamicRanking;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;

import org.apache.flink.streaming.api.datastream.DataStream;

public class ReposRankingTable extends AbstractTable<ReposRankingTable.RepoCountsRank> {
    public static class RepoCountsRank extends SequencedRow {
        @JsonProperty("repo_id")
        public final long repoId;
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @TableEventKey
        @JsonProperty("row_number")
        public final int rowNumber;
        @JsonProperty("rank")
        public final int rank;

        public RepoCountsRank(long repoId, WindowSize windowSize, int rowNumber, int rank) {
            this.repoId = repoId;
            this.windowSize = windowSize;
            this.rowNumber = rowNumber;
            this.rank = rank;
        }
    }

    @Override
    protected DataStream<RepoCountsRank> computeTable() {
        return getLivePreRepoCounts()
                .keyBy(e -> e.windowSize.toString())
                .process(
                        new DynamicRanking<>(
                                0L, Duration.ofSeconds(1), e -> e.repoId, e -> e.numEvents,
                                (w, k, v, row, rank, ts) -> {
                                    return new RepoCountsRank(k, WindowSize.fromString(w), row, rank);
                                },
                                Long.class, Long.class))
                .setParallelism(4)
                .returns(RepoCountsRank.class)
                .name("Per Repo Count Rankings");
    }

    @Override
    protected Class<RepoCountsRank> getOutputType() {
        return RepoCountsRank.class;
    }
}
