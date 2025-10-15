package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.TrendingLiveTable.RepoTrendingScore;

import org.apache.flink.streaming.api.datastream.DataStream;

public class TrendingRankingTable extends AbstractRankingTable<TrendingRankingTable.RepoTrendingRank> {
    public static class RepoTrendingRank extends RankingSeqRow {
        @JsonProperty("repo_id")
        public final long repoId;
        @JsonProperty("trending_score")
        public final Long trendingScore;

        public RepoTrendingRank(
                long repoId, Long trendingScore, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.repoId = repoId;
            this.trendingScore = trendingScore;
        }
    }

    @Override
    protected DataStream<RepoTrendingRank> computeTable() {
        return this.<DataStream<RepoTrendingScore>>getStream("[table]trending_live")
                .keyBy(e -> "dummyKey")
                .process(
                        new DynamicRanking<>(
                                0L, e -> e.repoId, e -> e.trendingScore,
                                (e, w, k, v, row, rank, maxRank, oldRow, oldRank, oldMaxRank) -> {
                                    RepoTrendingRank event = new RepoTrendingRank(
                                            k, v, row, rank, maxRank,
                                            oldRow, oldRank, oldMaxRank);
                                    event.seqNum = e.seqNum;
                                    return event;
                                },
                                Long.class, Long.class))
                // We only have one trending ranking, so no parallelism make sense here.
                .setParallelism(1)
                .returns(RepoTrendingRank.class)
                .name("Per Repo Trending Rankings");
    }

    @Override
    protected Class<RepoTrendingRank> getOutputType() {
        return RepoTrendingRank.class;
    }
}
