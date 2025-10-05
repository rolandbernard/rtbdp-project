package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractRankingTable;
import com.rolandb.DynamicRanking;
import com.rolandb.GithubEventType;
import com.rolandb.AbstractRankingTable.RankingSeqRow;
import com.rolandb.tables.CountsLiveTable.EventCounts;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsRankingTable extends AbstractRankingTable<CountsRankingTable.CountsRank> {
    public static class CountsRank extends RankingSeqRow {
        // We put only the window size as the key, so that we put all for the same
        // ranking into the same Kafka partition, ensuring in-order delivery to the
        // frontend client.
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("kind")
        public final GithubEventType eventType;
        @JsonProperty("num_events")
        public final Long numEvents;

        public CountsRank(
                WindowSize windowSize, GithubEventType eventType, Long numEvents, Integer rowNumber, Integer rank,
                Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            super(rowNumber, rank, maxRank, oldRow, oldRank, oldMaxRank);
            this.windowSize = windowSize;
            this.eventType = eventType;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<CountsRank> computeTable() {
        return this.<DataStream<EventCounts>>getStream("[table]counts_live")
                .keyBy(e -> e.windowSize.toString())
                .process(
                        new DynamicRanking<>(
                                0L, e -> e.eventType, e -> e.numEvents,
                                (e, w, k, v, row, rank, maxRank, oldRow, oldRank, oldMaxRank) -> {
                                    CountsRank event = new CountsRank(
                                            WindowSize.fromString(w), k, v, row, rank, maxRank,
                                            oldRow, oldRank, oldMaxRank);
                                    // We should use these as sequence numbers, so that the client
                                    // can compare the once from the PostgreSQL view to know whether
                                    // these are already accounted for.
                                    event.seqNum = e.seqNum;
                                    return event;
                                },
                                GithubEventType.class, Long.class))
                .setParallelism(4)
                .returns(CountsRank.class)
                .name("Event Count Rankings");
    }

    @Override
    protected Class<CountsRank> getOutputType() {
        return CountsRank.class;
    }
}
