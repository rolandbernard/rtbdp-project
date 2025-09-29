package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.SequencedRow;
import com.rolandb.tables.CountsLiveTable.WindowSize;

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
        return getLivePreRepoCounts();
    }

    @Override
    protected Class<RepoEventCounts> getOutputType() {
        return RepoEventCounts.class;
    }
}
