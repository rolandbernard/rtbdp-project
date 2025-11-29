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

public class CountsLiveTable extends AbstractTable<CountsLiveTable.EventCounts> {
    public static enum WindowSize implements WindowSpec {
        // The below should be in sync with the ones in the frontend and in
        // postgres database.
        MINUTES_5("5m", Duration.ofMinutes(5)), HOURS_1("1h", Duration.ofHours(1)),
        HOURS_6("6h", Duration.ofHours(6)), HOURS_24("24h", Duration.ofHours(24));

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

        public static WindowSize fromString(String name) {
            for (WindowSize type : WindowSize.values()) {
                if (type.toString().equals(name)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("\"" + name + "\" is not a valid window size");
        }
    }

    public static class EventCounts extends SequencedRow {
        @TableEventKey
        @JsonProperty("kind")
        public GithubEventType eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public WindowSize windowSize;
        @JsonProperty("num_events")
        public long numEvents;

        public EventCounts(GithubEventType eventType, WindowSize windowSize, long numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
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
