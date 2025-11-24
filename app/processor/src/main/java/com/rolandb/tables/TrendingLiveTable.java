package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.SequencedRow;
import com.rolandb.TrendingScoreFunction;
import com.rolandb.tables.StarsLiveTable.RepoStarCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

public class TrendingLiveTable extends AbstractTable<TrendingLiveTable.RepoTrendingScore> {
    public static class RepoTrendingScore extends SequencedRow {
        @TableEventKey
        @JsonProperty("repo_id")
        public final long repoId;
        @JsonProperty("trending_score")
        public final long trendingScore;

        public RepoTrendingScore(long repoId, long trendingScore) {
            this.repoId = repoId;
            this.trendingScore = trendingScore;
        }
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
