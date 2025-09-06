package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.CountAggregation;

public class CountsHistoryTable extends AbstractTableBuilder {
    public static class EventCounts {
        @TableEventKey
        @JsonProperty("ts_start")
        public final Instant winStart;
        @TableEventKey
        @JsonProperty("ts_end")
        public final Instant winEnd;
        @TableEventKey
        @JsonProperty("kind")
        public final String eventType;
        @JsonProperty("num_events")
        public final long numEvents;

        public EventCounts(Instant winStart, Instant winEnd, String eventType, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.eventType = eventType;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getEventsByTypeStream()
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                // Here we can afford to allow more lateness and retroactively
                // upsert with a new value.
                .allowedLateness(Duration.ofMinutes(30))
                .<Long, Long, EventCounts>aggregate(new CountAggregation<>(),
                        (key, window, elements, out) -> {
                            out.collect(new EventCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, elements.iterator().next()));
                        })
                .returns(EventCounts.class)
                .name("Historical Event Counts");
    }

    @Override
    protected Class<?> getOutputType() {
        return EventCounts.class;
    }
}
