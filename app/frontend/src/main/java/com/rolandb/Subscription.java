package com.rolandb;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A subscription is always for a specific table and filters the table based on
 * multiple filters. Every row that matches one of the given filters is to be
 * included in the subscription. Subscriptions also have id, so that they can
 * easily be added and removed.
 */
public class Subscription {
    public final long id;
    public final String tableName;
    private final List<TableRowFilter> filters;

    @JsonCreator
    public Subscription(
            @JsonProperty("id") long id, @JsonProperty("table") String tableName,
            @JsonProperty("filters") List<TableRowFilter> filters) {
        this.id = id;
        this.tableName = tableName;
        this.filters = filters;
    }

    /**
     * Test whether this subscription can be used with the given table.
     * 
     * @param table
     *            The table to check against.
     * @return {@code true} if the subscription can be used with the table,
     *         {@code false} otherwise.
     */
    public boolean applicableTo(Table table) {
        for (TableRowFilter filter : filters) {
            if (filter.applicableTo(table)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Test whether a given row matches the filter.
     * 
     * @param row
     *            The row to test against.
     * @return {@code true} in case we match, {@code false} otherwise.
     */
    public boolean accept(Map<String, ?> row) {
        for (TableRowFilter filter : filters) {
            if (filter.accept(row)) {
                return true;
            }
        }
        return false;
    }
}
