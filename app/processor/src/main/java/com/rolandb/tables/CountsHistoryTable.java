package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTableBuilder;
import com.rolandb.CountAggregation;
import com.rolandb.SequencedRow;

public class CountsHistoryTable extends AbstractTableBuilder {
    public static class EventCounts extends SequencedRow {
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
                            // The count could function as a sequence number, since
                            // it must be monotonic only within a single key, and
                            // within a window (which defines the key), the count
                            // is only ever increased.
                            long count = elements.iterator().next();
                            out.collect(new EventCounts(
                                    Instant.ofEpochMilli(window.getStart()),
                                    Instant.ofEpochMilli(window.getEnd()),
                                    key, count));
                        })
                .returns(EventCounts.class)
                .name("Historical Event Counts");
    }

    @Override
    protected Class<EventCounts> getOutputType() {
        return EventCounts.class;
    }
}
