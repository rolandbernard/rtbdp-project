package com.rolandb.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.rolandb.AbstractTable;
import com.rolandb.GithubEventType;
import com.rolandb.SequencedRow;
import com.rolandb.MultiSlidingBuckets.WindowSpec;

import java.time.Duration;

import org.apache.flink.streaming.api.datastream.DataStream;

public class CountsLiveTable extends AbstractTable<CountsLiveTable.EventCounts> {
    public static enum WindowSize implements WindowSpec {
        // The below should be in sync with the ones in the frontend and in
        // postgres database.
        MINUTES_5("5m", Duration.ofMinutes(5)), HOURS_1("1h", Duration.ofHours(1)), HOURS_6("6h",
                Duration.ofHours(6)), HOURS_24("24h", Duration.ofHours(24));

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
        public final GithubEventType eventType;
        @TableEventKey
        @JsonProperty("window_size")
        public final WindowSize windowSize;
        @JsonProperty("num_events")
        public final long numEvents;

        public EventCounts(GithubEventType eventType, WindowSize windowSize, long numEvents) {
            this.eventType = eventType;
            this.windowSize = windowSize;
            this.numEvents = numEvents;
        }
    }

    @Override
    protected DataStream<EventCounts> computeTable() {
        return getLiveEventCounts();
    }

    @Override
    protected Class<EventCounts> getOutputType() {
        return EventCounts.class;
    }
}
