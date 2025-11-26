package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

public class ReposHistoryTable extends AbstractTable<ReposHistoryTable.RepoEventCounts> {
    public static class RepoEventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("repo_id")
        public final long repoId;
        @TableEventKey
        @JsonProperty("ts_start")
        public final Instant winStart;
        @TableEventKey
        @JsonProperty("ts_end")
        public final Instant winEnd;
        @JsonProperty("num_events")
        public final long numEvents;

        public RepoEventCounts(Instant winStart, Instant winEnd, long repoId, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.repoId = repoId;
            this.numEvents = numEvents;
        }
    }

    private Duration window = Duration.ofMinutes(5);

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
