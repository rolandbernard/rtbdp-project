package com.rolandb;

import java.util.concurrent.ExecutionException;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a variation of the table builder that is intended for the ranking
 * tables. These are special in that they are defined in PostgreSQL as a view,
 * and therefore we only transmit the data to Kafka. Further, they are specially
 * handled in the frontend to correctly apply all of the updates inferred by
 * the events.
 * 
 * @param <E>
 *            The type of event this table outputs.
 */
public abstract class AbstractRankingTable<E extends AbstractRankingTable.RankingSeqRow> extends AbstractTable<E> {
    /**
     * The superclass of all rows for ranking tables. These always include some
     * specific fields.
     */
    public static abstract class RankingSeqRow extends SequencedRow {
        /** The row number of the ranked object. */
        @JsonProperty("row_number")
        public final Integer rowNumber;
        /** The rank of the ranked object. */
        @JsonProperty("rank")
        public final Integer rank;
        /** The row number of first object of higher rank. */
        @JsonProperty("max_rank")
        public final Integer maxRank;
        /** The row number of the object before this event. */
        @JsonProperty("old_row_number")
        public final Integer oldRow;
        /** The rank of the object before this event. */
        @JsonProperty("old_rank")
        public final Integer oldRank;
        /** The max rank of the object before this event. */
        @JsonProperty("old_max_rank")
        public final Integer oldMaxRank;

        /**
         * Create a new ranking row.
         * 
         * @param rowNumber
         *            The row number.
         * @param rank
         *            The rank.
         * @param maxRank
         *            The max rank.
         * @param oldRow
         *            The old row number.
         * @param oldRank
         *            The old rank.
         * @param oldMaxRank
         *            The old max rank.
         */
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

    /**
     * Create a new table with default values.
     */
    public AbstractRankingTable() {
        super();
    }

    @Override
    protected DataStream<E> sinkToPostgres(DataStream<E> stream) {
        // We don't want to emit these to PostgreSQL, because these tables are
        // defined only as views, with Flink only needing to generate the updates.
        return stream;
    }

    @Override
    protected void sinkToKafka(DataStream<E> stream) throws ExecutionException, InterruptedException {
        KafkaUtil.setupTopic(tableName, bootstrapServers, numPartitions, replicationFactor, retentionMs);
        // Keying here is important so that we ensure per-key in-order delivery of
        // the events. This is not strictly necessary for the other tables because
        // there we can just always take the latest state based on sequence numbers.
        // However, for the ranking, it is important because applying the updates
        // out-of-order would result in a different overall ranking.
        DataStreamSink<E> sink = stream
                .keyBy(tableOrderingKeySelector())
                .sinkTo(buildKafkaSink())
                .uid("kafka-sink-01-" + tableName)
                .name("Kafka Sink");
        int tableP = tableParallelism();
        if (tableP != -1) {
            sink.setParallelism(tableP <= 1 ? 1 : 2 * tableP);
        }
    }
}
