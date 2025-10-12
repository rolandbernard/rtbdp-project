package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

public class StarsLiveTable extends AbstractTable<StarsLiveTable.RepoStarCounts> {
    public static class RepoStarCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("repo_id")
        public final long repoId;
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("num_stars")
        public final long numStars;

        public RepoStarCounts(long repoId, WindowSize windowSize, long numStars) {
            this.repoId = repoId;
            this.windowSize = windowSize;
            this.numStars = numStars;
        }
    }

    @Override
    protected DataStream<RepoStarCounts> computeTable() {
        return getStarEventsByRepoStream()
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
                .name("Live per Repo Stars");
    }

    @Override
    protected Class<RepoStarCounts> getOutputType() {
        return RepoStarCounts.class;
    }
}
