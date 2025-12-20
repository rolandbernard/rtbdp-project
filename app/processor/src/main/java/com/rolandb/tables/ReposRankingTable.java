package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.ReposLiveTable.RepoEventCounts;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This table compute ranking updates based on the live event counts computed by
 * the {@link ReposLiveTable} table.
 */
public class ReposRankingTable extends AbstractRankingTable<ReposRankingTable.RepoCountsRank> {
    /** Type of event for this table. */
    public static class RepoCountsRank extends RankingSeqRow {
        /** The window size. */
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The repository id. */
        @JsonProperty("repo_id")
        public long repoId;
        /** Ths number of events. */
        @JsonProperty("num_events")
        public Long numEvents;

        /**
         * Create a new instance of the event.
         * 
         * @param windowSize
         *            The size of the window.
         * @param repoId
         *            The repository id.
         * @param numEvents
         *            The number of events.
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
    protected KeySelector<RepoCountsRank, ?> tableOrderingKeySelector() {
        return row -> row.windowSize.toString();
    }

    @Override
    protected int tableParallelism() {
        return WindowSize.values().length;
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
                .setParallelism(tableParallelism() * 2)
                .returns(RepoCountsRank.class)
                .uid("ranking-repo-counts-01")
                .name("Per Repo Count Rankings");
    }

    /**
     * Create a new table with default values.
     */
    public ReposRankingTable() {
        super();
    }

    @Override
    protected Class<RepoCountsRank> getOutputType() {
        return RepoCountsRank.class;
    }
}
