package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.CountAggregation;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class CountsLiveTable extends AbstractTableBuilder {
    public static class EventCounts {
        @JsonProperty("ts_start")
        public Instant winStart;
        @JsonProperty("ts_end")
        public Instant winEnd;
        @TableEventKey
        @JsonProperty("kind")
        public String eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public String windowSize;
        @JsonProperty("num_events")
        public int numEvents;

        public EventCounts(Instant winStart, Instant winEnd, String eventType, String windowSize, int numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getEventStream()
                .keyBy(event -> event.getType().toString())
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(15)))
                .<Integer, Integer, EventCounts>aggregate(new CountAggregation(),
                        (key, window, elements, out) -> {
                            out.collect(new EventCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, "5m", elements.iterator().next()));
                        })
                .returns(EventCounts.class);
    }

    @Override
    protected Class<?> getOutputType() {
        return EventCounts.class;
    }
}
