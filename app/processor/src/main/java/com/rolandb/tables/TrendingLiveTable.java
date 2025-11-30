package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.SequencedRow;
import com.rolandb.TrendingScoreFunction;
import com.rolandb.tables.StarsLiveTable.RepoStarCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * A live computation of a simple trending score. The trending score is based on
 * the linear compilation of the number of new stars in the last 5 minutes,
 * hour, 6 hours, and day.
 */
public class TrendingLiveTable extends AbstractTable<TrendingLiveTable.RepoTrendingScore> {
    /** Type of event for this table. */
    public static class RepoTrendingScore extends SequencedRow {
        /** The repository id. */
        @TableEventKey
        @JsonProperty("repo_id")
        public long repoId;
        /** The trending score. */
        @JsonProperty("trending_score")
        public long trendingScore;

        /**
         * Create a new event instance.
         * 
         * @param repoId
         *            The considered repository id.
         * @param trendingScore
         *            The latest trending score.
         */
        public RepoTrendingScore(long repoId, long trendingScore) {
            this.repoId = repoId;
            this.trendingScore = trendingScore;
        }

        @Override
        public boolean equals(Object other) {
            if (other.getClass() == RepoTrendingScore.class) {
                RepoTrendingScore o = (RepoTrendingScore) other;
                return repoId == o.repoId && trendingScore == o.trendingScore;
            } else {
                return false;
            }
        }
    }

    /**
     * Create a new table with default values.
     */
    public TrendingLiveTable() {
        super();
    }

    @Override
    protected DataStream<RepoTrendingScore> computeTable() {
        return this.<DataStream<RepoStarCounts>>getStream("liveStarCounts")
                .keyBy(e -> e.repoId)
                .map(new TrendingScoreFunction())
                .returns(RepoTrendingScore.class)
                .uid("live-trending-01")
                .name("Live per Repo Trending Score");
    }

    @Override
    protected Class<RepoTrendingScore> getOutputType() {
        return RepoTrendingScore.class;
    }
}
