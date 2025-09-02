package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsLiveTable extends AbstractTableBuilder {
    public static class EventCounts {
        @TableEventKey
        @JsonProperty("kind")
        public final String eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public final String windowSize;
        @JsonProperty("num_events")
        public final int numEvents;

        public EventCounts(String eventType, String windowSize, int numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getCountsLiveStream();
    }

    @Override
    protected Class<?> getOutputType() {
        return EventCounts.class;
    }
}
