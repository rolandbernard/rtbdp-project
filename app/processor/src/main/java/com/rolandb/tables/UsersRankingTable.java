package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.UsersLiveTable.UserEventCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This table compute ranking updates based on the live event counts computed by
 * the {@link UsersLiveTable} table.
 */
public class UsersRankingTable extends AbstractRankingTable<UsersRankingTable.UserCountsRank> {
    /** Type of event for this table. */
    public static class UserCountsRank extends RankingSeqRow {
        /** The window size. */
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The user id. */
        @JsonProperty("user_id")
        public long userId;
        /** The number of events. */
        @JsonProperty("num_events")
        public Long numEvents;

        /**
         * Create a new instance of the event.
         * 
         * @param windowSize
         *            The size of the window.
         * @param userId
         *            The user id.
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
        public UserCountsRank(
                WindowSize windowSize, long userId, Long numEvents, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.windowSize = windowSize;
            this.userId = userId;
            this.numEvents = numEvents;
        }
    }

    /**
     * Create a new table with default values.
     */
    public UsersRankingTable() {
        super();
    }

    @Override
    protected DataStream<UserCountsRank> computeTable() {
        return this.<DataStream<UserEventCounts>>getStream("[table]users_live")
                .keyBy(e -> e.windowSize.toString())
                .process(
                        new DynamicRanking<>(
                                0L, e -> e.userId, e -> e.numEvents,
                                (e, w, k, v, row, rank, maxRank, oldRow, oldRank, oldMaxRank) -> {
                                    UserCountsRank event = new UserCountsRank(
                                            WindowSize.fromString(w), k, v, row, rank, maxRank,
                                            oldRow, oldRank, oldMaxRank);
                                    event.seqNum = e.seqNum;
                                    return event;
                                },
                                Long.class, Long.class))
                .setParallelism(4)
                .returns(UserCountsRank.class)
                .uid("ranking-user-counts-01")
                .name("Per User Count Rankings");
    }

    @Override
    protected Class<UserCountsRank> getOutputType() {
        return UserCountsRank.class;
    }
}
