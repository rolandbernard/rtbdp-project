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
    private final Long limit;

    @JsonCreator
    public Subscription(
            @JsonProperty("id") long id, @JsonProperty("table") String tableName,
            @JsonProperty("filters") List<TableRowFilter> filters, @JsonProperty("limit") Long limit) {
        this.id = id;
        this.tableName = tableName == null ? "" : tableName;
        this.filters = filters;
        this.limit = limit;
    }

    /**
     * Return whether all fields used for filters are using keys.
     *
     * @param table
     *            The table to check against.
     * @return {@code true} if only keys are used.
     */
    public boolean usesOnlyKey(Table table) {
        for (TableRowFilter filter : filters) {
            if (!filter.usesOnlyKey(table)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Estimate the cardinality of rows returned by evaluating the subscription on
     * the given table.
     *
     * @param table
     *            The table to check against.
     * @return An estimate of the cardinality or {@code null} to indicate unbounded.
     */
    public Long estimateCardinality(Table table) {
        if (filters == null) {
            return null;
        }
        long estimate = 0;
        for (TableRowFilter filter : filters) {
            Long est = filter.estimateCardinality(table);
            if (est == null) {
                return null;
            }
            estimate += est;
        }
        return estimate;
    }

    /**
     * Test whether this subscription can be used with the given table.
     *
     * @param table
     *            The table to check against.
     * @return {@code true} if the subscription can be used with the table,
     *         {@code false} otherwise.
     */
    public boolean applicableTo(Table table, boolean inReplay) {
        if (inReplay && table.maxLimit != null && (limit == null || limit > table.maxLimit)) {
            Long estimate = estimateCardinality(table);
            if (estimate == null || estimate > table.maxLimit) {
                return false;
            }
        }
        if (filters == null) {
            return true;
        } else {
            for (TableRowFilter filter : filters) {
                if (!filter.applicableTo(table, inReplay)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Answer whether this type of subscription requires the results to be
     * sorted. Since we in any case don't guarantee ordering, this is only
     * relevant for queries that only request the last `N` results.
     * 
     * @return Whether to sort in the SQL query.
     */
    public boolean isSorted() {
        return limit != null;
    }

    /**
     * Test whether a given row matches the filter.
     *
     * @param row
     *            The row to test against.
     * @return {@code true} in case we match, {@code false} otherwise.
     */
    public boolean accept(Map<String, ?> row) {
        if (filters == null) {
            return true;
        } else {
            for (TableRowFilter filter : filters) {
                if (filter.accept(row)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Returns an SQL expression that can be used as the condition in a
     * select statement that in the `WHERE` clause to filter only for events
     * relevant to this subscription.
     *
     * @return The SQL expression.
     */
    public String asSqlQueryCondition() {
        if (filters == null) {
            return "TRUE";
        } else if (filters.isEmpty()) {
            return "FALSE";
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            boolean first = true;
            for (TableRowFilter filter : filters) {
                if (!first) {
                    builder.append(" OR ");
                }
                first = false;
                builder.append(filter.asSqlQueryCondition());
            }
            builder.append(")");
            return builder.toString();
        }
    }

    /**
     * Returns an SQL expression that can be used as the limit in a select
     * statement, if requested to return only the first set of results.
     *
     * @return The SQL expression.
     */
    public String asSqlQueryLimit() {
        if (limit == null) {
            return "";
        } else {
            return " LIMIT " + limit;
        }
    }
}
