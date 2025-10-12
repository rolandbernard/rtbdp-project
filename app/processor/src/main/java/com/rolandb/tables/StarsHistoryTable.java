package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

public class StarsHistoryTable extends AbstractTable<StarsHistoryTable.RepoStarCounts> {
    public static class RepoStarCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("repo_id")
        public final long repoId;
        @TableEventKey
        @JsonProperty("ts_start")
        public final Instant winStart;
        @TableEventKey
        @JsonProperty("ts_end")
        public final Instant winEnd;
        @JsonProperty("num_stars")
        public final long numStars;

        public RepoStarCounts(Instant winStart, Instant winEnd, long repoId, long numStars) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.repoId = repoId;
            this.numStars = numStars;
        }
    }

    @Override
    protected DataStream<RepoStarCounts> computeTable() {
        return getStarEventsByRepoStream()
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                // Here we can afford to allow more lateness and retroactively
                // upsert with a new value.
                .allowedLateness(Duration.ofMinutes(30))
                .<Long, Long, RepoStarCounts>aggregate(new CountAggregation<>(),
                        (key, window, elements, out) -> {
                            long count = elements.iterator().next();
                            out.collect(new RepoStarCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, count));
                        })
                .returns(RepoStarCounts.class)
                .name("Historical Stars by Repository");
    }

    @Override
    protected Class<RepoStarCounts> getOutputType() {
        return RepoStarCounts.class;
    }
}
