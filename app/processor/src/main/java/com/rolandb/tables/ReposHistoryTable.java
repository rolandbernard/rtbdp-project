package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

/**
 * This table computes windowed event counts per repository using tumbling
 * windows of fixed size.
 */
public class ReposHistoryTable extends AbstractTable<ReposHistoryTable.RepoEventCounts> {
    /** Type of event for this table. */
    public static class RepoEventCounts extends SequencedRow {
        /** The repository id. */
        @TableEventKey
        @JsonProperty("repo_id")
        public long repoId;
        /** The window start timestamp. */
        @TableEventKey
        @JsonProperty("ts_start")
        public Instant winStart;
        /** The window end timestamp. */
        @TableEventKey
        @JsonProperty("ts_end")
        public Instant winEnd;
        /** The number of events. */
        @JsonProperty("num_events")
        public long numEvents;

        /**
         * Create a new event instance.
         * 
         * @param winStart
         *            The start of the window.
         * @param winEnd
         *            The end of the window (exclusive):
         * @param repoId
         *            The id of the considered repository.
         * @param numEvents
         *            The number of events for the repository in the given window.
         */
        public RepoEventCounts(Instant winStart, Instant winEnd, long repoId, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.repoId = repoId;
            this.numEvents = numEvents;
        }
    }

    private Duration window;

    /**
     * Create a new table with default values.
     */
    public ReposHistoryTable() {
        super();
        window = Duration.ofMinutes(5);
    }

    /**
     * Set the window size to be used for the aggregation.
     * 
     * @param window
     *            The window size.
     * @return {@code this}
     */
    public ReposHistoryTable setWindowSize(Duration window) {
        this.window = window;
        return this;
    }

    @Override
    protected DataStream<RepoEventCounts> computeTable() {
        return getEventsByRepoStream()
                .window(TumblingEventTimeWindows.of(window))
                // Here we can afford to allow more lateness and retroactively
                // upsert with a new value.
                .allowedLateness(window.multipliedBy(10))
                .<Long, Long, RepoEventCounts>aggregate(new CountAggregation<>(),
                        (key, window, elements, out) -> {
                            long count = elements.iterator().next();
                            out.collect(new RepoEventCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, count));
                        })
                .returns(RepoEventCounts.class)
                .uid("historical-repo-counts-01-" + window.getSeconds())
                .name("Historical Counts by Repository");
    }

    @Override
    protected Class<RepoEventCounts> getOutputType() {
        return RepoEventCounts.class;
    }
}
