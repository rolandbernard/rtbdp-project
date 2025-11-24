package com.rolandb;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.java.functions.KeySelector;
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

    @Override
    protected void sinkToKafka(DataStream<E> stream) throws ExecutionException, InterruptedException {
        KafkaUtil.setupTopic(tableName, bootstrapServers, numPartitions, replicationFactor, retentionMs);
        // Keying here is important so that we ensure per-key in-order delivery of
        // the events. This is not strictly necessary for the other tables because
        // there we can just always take the latest state based on sequence numbers.
        // However, for the ranking, it is important because applying the updates
        // out-of-order would result in a different overall ranking.
        stream
                .keyBy(new KeySelector<E, List<Object>>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public List<Object> getKey(E event) throws Exception {
                        return (List<Object>) event.getKey();
                    }
                })
                .sinkTo(buildKafkaSink())
                .uid("kafka-sink-01-" + tableName)
                .name("Kafka Sink");
    }
}
