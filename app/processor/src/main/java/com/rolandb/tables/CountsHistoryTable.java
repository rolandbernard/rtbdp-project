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

/**
 * A table that counts the events by type in tumbling windows.
 */
public class CountsHistoryTable extends AbstractTable<CountsHistoryTable.EventCounts> {
    /** Type of event for this table. */
    public static class EventCounts extends SequencedRow {
        /** Start of the window. */
        @TableEventKey
        @JsonProperty("ts_start")
        public Instant winStart;
        /** End of the window. */
        @TableEventKey
        @JsonProperty("ts_end")
        public Instant winEnd;
        /** Event type considered. */
        @TableEventKey
        @JsonProperty("kind")
        public GithubEventType eventType;
        /** Number of events in the window. */
        @JsonProperty("num_events")
        public long numEvents;

        /**
         * Construct a new instance of the event counts.
         * 
         * @param winStart
         *            Window start.
         * @param winEnd
         *            Window end.
         * @param eventType
         *            Type of event.
         * @param numEvents
         *            Number of events in that window of the given type.
         */
        public EventCounts(Instant winStart, Instant winEnd, GithubEventType eventType, long numEvents) {
            this.winStart = winStart;
            this.winEnd = winEnd;
            this.eventType = eventType;
            this.numEvents = numEvents;
        }
    }

    private Duration window;

    /**
     * Create a new table with default values.
     */
    public CountsHistoryTable() {
        super();
        window = Duration.ofMinutes(5);
    }

    /**
     * Modify the window size to use for this table.
     * 
     * @param window
     *            The new window size.
     * @return {@code this}
     */
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
