package com.rolandb.tables;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable;
import com.rolandb.CountAggregation;
import com.rolandb.GithubEventType;
import com.rolandb.SequencedRow;

public class CountsHistoryTable extends AbstractTable<CountsHistoryTable.EventCounts> {
    public static class EventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("ts_start")
        public Instant winStart;
        @TableEventKey
        @JsonProperty("ts_end")
        public Instant winEnd;
        @TableEventKey
        @JsonProperty("kind")
        public GithubEventType eventType;
        @JsonProperty("num_events")
        public long numEvents;

        public EventCounts(Instant winStart, Instant winEnd, GithubEventType eventType, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.eventType = eventType;
            this.numEvents = numEvents;
        }
    }

    private Duration window = Duration.ofMinutes(5);

    public CountsHistoryTable setWindowSize(Duration window) {
        this.window = window;
        return this;
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getEventsByTypeStream()
                .window(TumblingEventTimeWindows.of(window))
                // Here we can afford to allow more lateness and retroactively
                // upsert with a new value.
                .allowedLateness(window.multipliedBy(10))
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
                                    GithubEventType.fromString(key), count));
                        })
                .returns(EventCounts.class)
                .uid("historical-event-counts-01-" + window.getSeconds())
                .name("Historical Event Counts");
    }

    @Override
    protected Class<EventCounts> getOutputType() {
        return EventCounts.class;
    }
}
