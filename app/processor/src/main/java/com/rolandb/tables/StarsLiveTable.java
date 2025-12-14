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
 * This is a table that computes the number of star events in the preceding
 * windows on a per-repository granularity.
 */
public class StarsLiveTable extends AbstractTable<StarsLiveTable.RepoStarCounts> {
    /** Type of event for this table. */
    public static class RepoStarCounts extends SequencedRow {
        /** Repository id. */
        @TableEventKey
        @JsonProperty("repo_id")
        public long repoId;
        /** The considered window size. */
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The number od stars. */
        @JsonProperty("num_stars")
        public long numStars;

        /**
         * Instantiate a new event.
         * 
         * @param repoId
         *            The repository id.
         * @param windowSize
         *            The considered window size.
         * @param numStars
         *            The number of new starts that the repository received in the last
         *            window of given size.
         */
        public RepoStarCounts(long repoId, WindowSize windowSize, long numStars) {
            this.repoId = repoId;
            this.windowSize = windowSize;
            this.numStars = numStars;
        }
    }

    /**
     * Create a new table with default values.
     */
    public StarsLiveTable() {
        super();
    }

    @Override
    protected KeySelector<RepoStarCounts, ?> tableOrderingKeySelector() {
        return row -> row.windowSize.toString();
    }

    @Override
    protected int tableParallelism() {
        return WindowSize.values().length;
    }

    @Override
    protected DataStream<RepoStarCounts> computeTable() {
        DataStream<RepoStarCounts> stream = getStarEventsByRepoStream()
                .process(new MultiSlidingBuckets<>(Duration.ofSeconds(1),
                        List.of(
                                WindowSize.MINUTES_5,
                                WindowSize.HOURS_1,
                                WindowSize.HOURS_6,
                                WindowSize.HOURS_24),
                        (windowStart, windowEnd, key, winSpec, count) -> {
                            return new RepoStarCounts(key, winSpec, count);
                        }))
                .returns(RepoStarCounts.class)
                .uid("live-star-counts-01")
                .name("Live per Repo Stars");
        streams.put("liveStarCounts", stream);
        return stream;
    }

    @Override
    protected Class<RepoStarCounts> getOutputType() {
        return RepoStarCounts.class;
    }
}
