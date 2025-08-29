package com.rolandb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAnySetter;

/**
 * A filter that filters rows of a table.
 */
public class TableRowFilter {
    private final Map<String, TableValueFilter<?>> filters = new HashMap<>();

    @JsonAnySetter
    public void setKeyFilters(String key, TableValueFilter<?> rule) {
        this.filters.put(key, rule);
    }

    /**
     * Test whether this filter can be used with the given table.
     * 
     * @param table
     *            The table to check against.
     * @return {@code true} if the filter can be used with the table, {@code false}
     *         otherwise.
     */
    public boolean applicableTo(Table table) {
        Set<String> keys = table.fields.stream()
                .filter(e -> e.isKey)
                .map(e -> e.name)
                .collect(Collectors.toSet());
        for (String filterKey : filters.keySet()) {
            if (!keys.contains(filterKey)) {
                return false;
            }
        }
        return true;
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
            } else if (value instanceof String) {
                if (!((TableValueFilter<String>) filter.getValue()).accept((String) value)) {
                    return false;
                }
            } else {
                throw new IllegalArgumentException("Unsupported filter value type");
            }
        }
        return true;
    }
}
