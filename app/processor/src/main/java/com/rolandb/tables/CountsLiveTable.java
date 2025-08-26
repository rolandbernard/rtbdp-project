package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.SlidingCountWithZeros;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

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
                .process(new SlidingCountWithZeros<>(
                        // 20 = 5m, 240 = 1h, 1440 = 6h, 5760 = 24h
                        List.of(20, 240, 1440, 5760), Duration.ofSeconds(15),
                        (windowStart, windowEnd, key, window_size, count) -> {
                            return new EventCounts(
                                    windowStart, windowEnd, key,
                                    window_size == 20 ? "5m"
                                            : window_size == 240 ? "1h"
                                                    : window_size == 1440 ? "6h" : "24h",
                                    count);
                        }))
                .returns(EventCounts.class);
    }

    @Override
    protected Class<?> getOutputType() {
        return EventCounts.class;
    }
}
