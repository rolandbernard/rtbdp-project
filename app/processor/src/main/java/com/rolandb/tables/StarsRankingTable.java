package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.StarsLiveTable.RepoStarCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

public class StarsRankingTable extends AbstractRankingTable<StarsRankingTable.RepoStarsRank> {
    public static class RepoStarsRank extends RankingSeqRow {
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("repo_id")
        public final long repoId;
        @JsonProperty("num_stars")
        public final Long numStars;

        public RepoStarsRank(
                WindowSize windowSize, long repoId, Long numEvents, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.windowSize = windowSize;
            this.repoId = repoId;
            this.numStars = numEvents;
        }
    }

    @Override
    protected DataStream<RepoStarsRank> computeTable() {
        return this.<DataStream<RepoStarCounts>>getStream("[table]stars_live")
                .keyBy(e -> e.windowSize.toString())
                .process(
                        new DynamicRanking<>(
                                0L, e -> e.repoId, e -> e.numStars,
                                (e, w, k, v, row, rank, maxRank, oldRow, oldRank, oldMaxRank) -> {
                                    RepoStarsRank event = new RepoStarsRank(
                                            WindowSize.fromString(w), k, v, row, rank, maxRank,
                                            oldRow, oldRank, oldMaxRank);
                                    event.seqNum = e.seqNum;
                                    return event;
                                },
                                Long.class, Long.class))
                .setParallelism(Integer.min(4, env.getParallelism()))
                .returns(RepoStarsRank.class)
                .name("Per Repo Stars Rankings");
    }

    @Override
    protected Class<RepoStarsRank> getOutputType() {
        return RepoStarsRank.class;
    }
}
