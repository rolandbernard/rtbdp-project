package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.TrendingLiveTable.RepoTrendingScore;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This table compute ranking updates based on the live trending score computed
 * by the {@link TrendingLiveTable} table.
 */
public class TrendingRankingTable extends AbstractRankingTable<TrendingRankingTable.RepoTrendingRank> {
    /** Type of event for this table. */
    public static class RepoTrendingRank extends RankingSeqRow {
        /** The repository id. */
        @JsonProperty("repo_id")
        public long repoId;
        /** The trending score. */
        @JsonProperty("trending_score")
        public Long trendingScore;

        /**
         * Create a new instance of the event.
         * 
         * @param repoId
         *            The repository id.
         * @param trendingScore
         *            The trending score of the repository.
         * @param rowNumber
         *            The row number in the ranking.
         * @param rank
         *            The rank in the ranking.
         * @param maxRank
         *            The max rank in the ranking.
         * @param oldRow
         *            The previous row number.
         * @param oldRank
         *            The previous rank.
         * @param oldMaxRank
         *            The previous max rank.
         */
        public RepoTrendingRank(
                long repoId, Long trendingScore, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.repoId = repoId;
            this.trendingScore = trendingScore;
        }
    }

    /**
     * Create a new table with default values.
     */
    public TrendingRankingTable() {
        super();
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
                .uid("ranking-trending-01")
                .name("Per Repo Trending Rankings");
    }

    @Override
    protected Class<RepoTrendingRank> getOutputType() {
        return RepoTrendingRank.class;
    }
}
