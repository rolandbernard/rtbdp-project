package com.rolandb;

import java.time.Instant;

import org.apache.flink.types.Row;

/**
 * This is a simple container that contains both a timestamp and a row. The
 * timestamp is set to the time when the object instance is constructed.
 */
public class TimedRow {
    private final Row row;
    private final Instant time;

    public TimedRow(Row row) {
        this.row = row;
        time = Instant.now();
    }

    public <T> T getFieldAs(String name) {
        return row.getFieldAs(name);
    }

    public Row getRow() {
        return row;
    }

    public Instant getTime() {
        return time;
    }

    @Override
    public String toString() {
        return row + " @ " + time;
    }
}
