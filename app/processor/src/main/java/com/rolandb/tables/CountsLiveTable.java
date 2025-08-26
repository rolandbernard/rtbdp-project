package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.MultiSlidingBuckets.WindowSpec;

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
                .process(new MultiSlidingBuckets<>(Duration.ofSeconds(15),
                        List.of(
                                new WindowSpec("5m", Duration.ofMinutes(5)),
                                new WindowSpec("1h", Duration.ofHours(1)),
                                new WindowSpec("6h", Duration.ofHours(6)),
                                new WindowSpec("24h", Duration.ofHours(24))),
                        (windowStart, windowEnd, key, winSpec, count) -> {
                            return new EventCounts(windowStart, windowEnd, key, winSpec.name, count.intValue());
                        }))
                .returns(EventCounts.class);
    }

    @Override
    protected Class<?> getOutputType() {
        return EventCounts.class;
    }
}
