package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

import java.time.Duration;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

public class ReposLiveTable extends AbstractTable<ReposLiveTable.RepoEventCounts> {
    public static class RepoEventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("repo_id")
        public final long repoId;
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("num_events")
        public final long numEvents;

        public RepoEventCounts(long repoId, WindowSize windowSize, long numEvents) {
            this.repoId = repoId;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
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
