package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.SequencedRow;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsLiveTable extends AbstractTableBuilder {
    public static class EventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("kind")
        public final String eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public final String windowSize;
        @JsonProperty("num_events")
        public final long numEvents;

        public EventCounts(String eventType, String windowSize, long numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getLiveEventCounts();
    }

    @Override
    protected Class<EventCounts> getOutputType() {
        return EventCounts.class;
    }
}
