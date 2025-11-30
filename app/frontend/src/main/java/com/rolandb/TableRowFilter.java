package com.rolandb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.rolandb.Table.Field;

/**
 * A filter that filters rows of a table.
 */
public class TableRowFilter {
    /** A set of conjunctive filters to apply. */
    private final Map<String, TableValueFilter<?>> filters;

    /**
     * Create a new empty instance. The instance will have no row filters, meaning
     * it accepts all rows.
     */
    public TableRowFilter() {
        filters = new HashMap<>();
    }

    /**
     * Set the filter for a given field name. A single field can only have one
     * filter and this method will override the existing one if any.
     * 
     * @param key
     *            The name of the field.
     * @param rule
     *            The filter to apply for that field.
     */
    @JsonAnySetter
    public void setKeyFilters(String key, TableValueFilter<?> rule) {
        this.filters.put(key, rule);
    }

    /**
     * Test whether this filter can be used with the given table.
     *
     * @param table
     *            The table to check against.
     * @param inReplay
     *            Whether this is for a replay of a subscription.
     * @return {@code true} if the filter can be used with the table, {@code false}
     *         otherwise.
     */
    public boolean applicableTo(Table table, boolean inReplay) {
        Map<String, Field> keys = table.fields.stream()
                .filter(e -> !inReplay || (e.canFilter() && e.inReplay))
                .collect(Collectors.toMap(e -> e.name, e -> e));
        for (Entry<String, TableValueFilter<?>> filter : filters.entrySet()) {
            Field field = keys.get(filter.getKey());
            if (field == null || !filter.getValue().applicableTo(field)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return whether all fields used for filters are using keys.
     *
     * @param table
     *            The table to check against.
     * @return {@code true} if only keys are used.
     */
    public boolean usesOnlyKey(Table table) {
        Map<String, Field> fields = table.fields.stream()
                .collect(Collectors.toMap(e -> e.name, e -> e));
        for (Entry<String, TableValueFilter<?>> filter : filters.entrySet()) {
            Field field = fields.get(filter.getKey());
            if (field == null || !field.isKey()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Estimate the maximum number of tuples that can be returned in the presence
     * of this filter. This assumes the filter is on a key, i.e., no two tuples
     * will have the same values.
     * 
     * @param table
     *            The table to use for the estimation.
     * @return The estimated cardinality
     */
    public Long estimateCardinality(Table table) {
        if (filters.isEmpty()) {
            return null;
        } else {
            Map<String, Field> fields = table.fields.stream()
                    .collect(Collectors.toMap(e -> e.name, e -> e));
            Long est = null;
            for (Entry<String, TableValueFilter<?>> filter : filters.entrySet()) {
                Field field = fields.get(filter.getKey());
                if (field == null) {
                    return null;
                }
                Long child = filter.getValue().estimateCardinality(field);
                if (child != null) {
                    est = est == null || child < est ? child : est;
                }
            }
            return est;
        }
    }

    /**
     * Test whether a given row matches the filter.
     *
     * @param row
     *            The row to test against.
     * @return {@code true} in case we match, {@code false} otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean accept(Map<String, ?> row) {
        for (Entry<String, TableValueFilter<?>> filter : filters.entrySet()) {
            Object value = row.get(filter.getKey());
            if (value == null) {
                return false;
            } else if (value instanceof Long) {
                if (!((TableValueFilter<Long>) filter.getValue()).accept((Long) value)) {
                    return false;
                }
            } else {
                if (!((TableValueFilter<String>) filter.getValue()).accept(value.toString())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns an SQL expression that can be used as the condition in a
     * select statement that in the `WHERE` clause to filter only for events
     * relevant to this filter.
     *
     * @return The SQL expression.
     */
    public String asSqlQueryCondition() {
        if (filters.isEmpty()) {
            return "TRUE";
        } else {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            builder.append("(");
            for (Entry<String, TableValueFilter<?>> filter : filters.entrySet()) {
                if (!first) {
                    builder.append(" AND ");
                }
                first = false;
                builder.append(filter.getValue().asSqlQueryCondition(filter.getKey()));
            }
            builder.append(")");
            return builder.toString();
        }
    }
}
