package com.rolandb;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a variation of the table builder that is intended for the ranking
 * tables. These are special in that they are defined in PostgreSQL as a view,
 * and therefore we only transmit the data to Kafka. Further, they are specially
 * handled in the frontend to correctly apply all of the updates inferred by
 * the events.
 */
public abstract class AbstractRankingTable<E extends AbstractRankingTable.RankingSeqRow> extends AbstractTable<E> {
    public static abstract class RankingSeqRow extends SequencedRow {
        @JsonProperty("row_number")
        public final Integer rowNumber;
        @JsonProperty("rank")
        public final Integer rank;
        @JsonProperty("max_rank")
        public final Integer maxRank;
        @JsonProperty("old_row_number")
        public final Integer oldRow;
        @JsonProperty("old_rank")
        public final Integer oldRank;
        @JsonProperty("old_max_rank")
        public final Integer oldMaxRank;

        public RankingSeqRow(
                Integer rowNumber, Integer rank, Integer maxRank, Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            this.rowNumber = rowNumber;
            this.rank = rank;
            this.maxRank = maxRank;
            this.oldRow = oldRow;
            this.oldRank = oldRank;
            this.oldMaxRank = oldMaxRank;
        }
    }

    @Override
    protected DataStream<E> sinkToPostgres(DataStream<E> stream) {
        // We don't want to emit these to PostgreSQL, because these tables are
        // defined only as views, with Flink only needing to generate the updates.
        return stream;
    }
}
