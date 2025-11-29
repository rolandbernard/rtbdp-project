package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.ReposLiveTable.RepoEventCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

public class ReposRankingTable extends AbstractRankingTable<ReposRankingTable.RepoCountsRank> {
    public static class RepoCountsRank extends RankingSeqRow {
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        @JsonProperty("repo_id")
        public long repoId;
        @JsonProperty("num_events")
        public Long numEvents;

        public RepoCountsRank(
                WindowSize windowSize, long repoId, Long numEvents, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.windowSize = windowSize;
            this.repoId = repoId;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<RepoCountsRank> computeTable() {
        return this.<DataStream<RepoEventCounts>>getStream("[table]repos_live")
                .keyBy(e -> e.windowSize.toString())
                .process(
                        new DynamicRanking<>(
                                0L, e -> e.repoId, e -> e.numEvents,
                                (e, w, k, v, row, rank, maxRank, oldRow, oldRank, oldMaxRank) -> {
                                    RepoCountsRank event = new RepoCountsRank(
                                            WindowSize.fromString(w), k, v, row, rank, maxRank,
                                            oldRow, oldRank, oldMaxRank);
                                    event.seqNum = e.seqNum;
                                    return event;
                                },
                                Long.class, Long.class))
                .setParallelism(Integer.min(4, env.getParallelism()))
                .returns(RepoCountsRank.class)
                .uid("ranking-repo-counts-01")
                .name("Per Repo Count Rankings");
    }

    @Override
    protected Class<RepoCountsRank> getOutputType() {
        return RepoCountsRank.class;
    }
}
