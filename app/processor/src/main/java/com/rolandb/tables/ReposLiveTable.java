package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This is a table that computes the number of events in the preceding windows
 * on a per-repository granularity.
 */
public class ReposLiveTable extends AbstractTable<ReposLiveTable.RepoEventCounts> {
    /** Type of event for this table. */
    public static class RepoEventCounts extends SequencedRow {
        /** The repository id. */
        @TableEventKey
        @JsonProperty("repo_id")
        public long repoId;
        /** The considered window size. */
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The number of events. */
        @JsonProperty("num_events")
        public long numEvents;

        /**
         * Create a new event instance.
         * 
         * @param repoId
         *            The id of the considered repository.
         * @param windowSize
         *            The considered window size.
         * @param numEvents
         *            The number of events for the repository in that window.
         */
        public RepoEventCounts(long repoId, WindowSize windowSize, long numEvents) {
            this.repoId = repoId;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    /**
     * Create a new table with default values.
     */
    public ReposLiveTable() {
        super();
    }

    @Override
    protected KeySelector<RepoEventCounts, ?> tableOrderingKeySelector() {
        return row -> row.windowSize.toString();
    }

    @Override
    protected int tableParallelism() {
        return WindowSize.values().length;
    }

    @Override
    protected DataStream<RepoEventCounts> computeTable() {
        return getEventsByRepoStream()
                .process(new MultiSlidingBuckets<>(Duration.ofSeconds(1),
                        List.of(
                                WindowSize.MINUTES_5,
                                WindowSize.HOURS_1,
                                WindowSize.HOURS_6,
                                WindowSize.HOURS_24),
                        (windowStart, windowEnd, key, winSpec, count) -> {
                            return new RepoEventCounts(key, winSpec, count);
                        }))
                .returns(RepoEventCounts.class)
                .uid("live-repo-counts-01")
                .name("Live per Repo Counts");
    }

    @Override
    protected Class<RepoEventCounts> getOutputType() {
        return RepoEventCounts.class;
    }
}
