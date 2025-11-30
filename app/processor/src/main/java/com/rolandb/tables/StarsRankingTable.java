package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.StarsLiveTable.RepoStarCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This table compute ranking updates based on the live event counts computed by
 * the {@link StarsLiveTable} table.
 */
public class StarsRankingTable extends AbstractRankingTable<StarsRankingTable.RepoStarsRank> {
    /** Type of event for this table. */
    public static class RepoStarsRank extends RankingSeqRow {
        /** The window size considered. */
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The repository id. */
        @JsonProperty("repo_id")
        public long repoId;
        /** The number of new stars. */
        @JsonProperty("num_stars")
        public Long numStars;

        /**
         * Create a new instance of the event.
         * 
         * @param windowSize
         *            The size of the window.
         * @param repoId
         *            The repository id.
         * @param numStars
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
        public RepoStarsRank(
                WindowSize windowSize, long repoId, Long numStars, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.windowSize = windowSize;
            this.repoId = repoId;
            this.numStars = numStars;
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
                .uid("ranking-stars-01")
                .name("Per Repo Stars Rankings");
    }

    /**
     * Create a new table with default values.
     */
    public StarsRankingTable() {
        super();
    }

    @Override
    protected Class<RepoStarsRank> getOutputType() {
        return RepoStarsRank.class;
    }
}
