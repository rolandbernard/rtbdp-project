package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.DynamicRanking;
import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.UsersLiveTable.UserEventCounts;

import org.apache.flink.streaming.api.datastream.DataStream;

public class UsersRankingTable extends AbstractRankingTable<UsersRankingTable.UserCountsRank> {
    public static class UserCountsRank extends RankingSeqRow {
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("user_id")
        public final long userId;
        @JsonProperty("num_events")
        public final Long numEvents;

        public UserCountsRank(
                WindowSize windowSize, long userId, Long numEvents, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.windowSize = windowSize;
            this.userId = userId;
            this.numEvents = numEvents;
        }
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
                .name("Per User Count Rankings");
    }

    @Override
    protected Class<UserCountsRank> getOutputType() {
        return UserCountsRank.class;
    }
}
