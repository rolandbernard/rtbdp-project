package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

/**
 * This table computes windowed star event counts per repository using tumbling
 * windows of fixed size.
 */
public class StarsHistoryTable extends AbstractTable<StarsHistoryTable.RepoStarCounts> {
    /** Type of event for this table. */
    public static class RepoStarCounts extends SequencedRow {
        /** The repository id. */
        @TableEventKey
        @JsonProperty("repo_id")
        public long repoId;
        /** The window start. */
        @TableEventKey
        @JsonProperty("ts_start")
        public Instant winStart;
        /** The window end. */
        @TableEventKey
        @JsonProperty("ts_end")
        public Instant winEnd;
        /** The number of star events. */
        @JsonProperty("num_stars")
        public long numStars;

        /**
         * Create a new event instance.
         * 
         * @param winStart
         *            The start of the window.
         * @param winEnd
         *            The end of the window.
         * @param repoId
         *            The id of the considered repository.
         * @param numStars
         *            The number of starring events for that repository in the given
         *            window.
         */
        public RepoStarCounts(Instant winStart, Instant winEnd, long repoId, long numStars) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.repoId = repoId;
            this.numStars = numStars;
        }
    }

    private Duration window;

    /**
     * Create a new table with default values.
     */
    public StarsHistoryTable() {
        super();
        window = Duration.ofMinutes(5);
    }

    @Override
    protected KeySelector<RepoStarCounts, ?> tableOrderingKeySelector() {
        return row -> List.of(row.winStart, row.repoId);
    }

    /**
     * Set the window size with which to aggregate the events.
     * 
     * @param window
     *            The window size.
     * @return {@code this}
     */
    public StarsHistoryTable setWindowSize(Duration window) {
        this.window = window;
        return this;
    }

    @Override
    protected DataStream<RepoStarCounts> computeTable() {
        return getStarEventsByRepoStream()
                .window(TumblingEventTimeWindows.of(window))
                // Here we can afford to allow more lateness and retroactively
                // upsert with a new value.
                .allowedLateness(window.multipliedBy(10))
                .<Long, Long, RepoStarCounts>aggregate(new CountAggregation<>(),
                        (key, window, elements, out) -> {
                            long count = elements.iterator().next();
                            out.collect(new RepoStarCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, count));
                        })
                .returns(RepoStarCounts.class)
                .uid("historical-star-counts-01-" + window.getSeconds())
                .name("Historical Stars by Repository");
    }

    @Override
    protected Class<RepoStarCounts> getOutputType() {
        return RepoStarCounts.class;
    }
}
