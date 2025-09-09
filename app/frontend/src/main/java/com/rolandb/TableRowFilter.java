package com.rolandb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.rolandb.Table.TableField;

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
        Map<String, TableField> keys = table.fields.stream()
                .filter(e -> e.isKey)
                .collect(Collectors.toMap(e -> e.name, e -> e));
        for (Entry<String, TableValueFilter<?>> filter : filters.entrySet()) {
            TableField field = keys.get(filter.getKey());
            if (field == null || !filter.getValue().applicableTo(field)) {
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
