package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.rolandb.AbstractTable;
import com.rolandb.GithubEventType;
import com.rolandb.MultiSlidingBuckets;
import com.rolandb.SequencedRow;
import com.rolandb.MultiSlidingBuckets.WindowSpec;

import java.time.Duration;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * A table that contains the live counts for the preceding window.
 */
public class CountsLiveTable extends AbstractTable<CountsLiveTable.EventCounts> {
    /** The type of window size used. */
    public static enum WindowSize implements WindowSpec {
        // The below should be in sync with the ones in the frontend and in
        // postgres database.
        /** Five minutes. */
        MINUTES_5("5m", Duration.ofMinutes(5)),
        /** One hour. */
        HOURS_1("1h", Duration.ofHours(1)),
        /** Six hours. */
        HOURS_6("6h", Duration.ofHours(6)),
        /** One day. */
        HOURS_24("24h", Duration.ofHours(24));

        private final String name;
        private final long sizeMs;

        private WindowSize(String name, Duration size) {
            this.name = name;
            this.sizeMs = size.toMillis();
        }

        @JsonValue
        @Override
        public String toString() {
            return this.name;
        }

        @Override
        public long sizeInMs() {
            return sizeMs;
        }

        /**
         * Find the window size that matches the given string.
         * 
         * @param name
         *            The string to search for.
         * @return The window size with given name.
         */
        public static WindowSize fromString(String name) {
            for (WindowSize type : WindowSize.values()) {
                if (type.toString().equals(name)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("\"" + name + "\" is not a valid window size");
        }
    }

    /** Type of event for this table. */
    public static class EventCounts extends SequencedRow {
        /** The type of event considered. */
        @TableEventKey
        @JsonProperty("kind")
        public GithubEventType eventType;
        /** The window size considered */
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        /** The number of events in the window. */
        @JsonProperty("num_events")
        public long numEvents;

        /**
         * Create a new event instance.
         * 
         * @param eventType
         *            The type of event.
         * @param windowSize
         *            The size of the window.
         * @param numEvents
         *            The number of events of that type in the window.
         */
        public EventCounts(GithubEventType eventType, WindowSize windowSize, long numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    /**
     * Create a new table with default values.
     */
    public CountsLiveTable() {
        super();
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getEventsByTypeStream()
                .process(new MultiSlidingBuckets<>(Duration.ofSeconds(1),
                        List.of(
                                WindowSize.MINUTES_5,
                                WindowSize.HOURS_1,
                                WindowSize.HOURS_6,
                                WindowSize.HOURS_24),
                        (windowStart, windowEnd, key, winSpec, count) -> {
                            // The combination of windowStart and count could act as
                            // a sequence number, since we always want to override
                            // older windowStart with newer onces, and always want
                            // the latest (highest) count for that window.
                            return new EventCounts(GithubEventType.fromString(key), winSpec, count);
                        }))
                .returns(EventCounts.class)
                .uid("live-event-counts-01")
                .name("Live Event Counts");
    }

    @Override
    protected Class<EventCounts> getOutputType() {
        return EventCounts.class;
    }
}
