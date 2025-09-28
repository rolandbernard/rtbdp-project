package com.rolandb;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.rolandb.Table.Field;

/**
 * Interface used for filters that filter based on a single columns value.
 */
public interface TableValueFilter<T> {
    /**
     * A column filter that retains only a range of values. The values of start
     * or end may be omitted (set to null) to get an open interval.
     */
    public static class RangeFilter<T> implements TableValueFilter<T> {
        private final T start;
        private final T end;
        private final T substr;
        private final boolean inclusive;

        @JsonCreator
        public RangeFilter(
                @JsonProperty("start") T start, @JsonProperty("end") T end, @JsonProperty("substr") T substr,
                @JsonProperty(value = "inclusive", defaultValue = "false") boolean inclusive) {
            this.start = start;
            this.end = end;
            this.substr = substr;
            this.inclusive = inclusive;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean accept(T obj) {
            Comparable<T> cmp = (Comparable<T>) obj;
            if (start != null && (cmp == null || cmp.compareTo(start) < 0)) {
                return false;
            }
            if (end != null && (cmp == null || cmp.compareTo(end) >= 0 && (!inclusive || cmp.compareTo(end) > 0))) {
                return false;
            }
            if (substr != null && !((String) obj).contains((String) substr)) {
                return false;
            }
            return true;
        }

        @Override
        public String asSqlQueryCondition(String name) {
            if (start == null && end == null && substr == null) {
                return "TRUE";
            } else {
                StringBuilder builder = new StringBuilder();
                builder.append("(");
                if (start != null) {
                    builder.append(name);
                    builder.append(" >= ");
                    if (start instanceof Long) {
                        builder.append(end.toString());
                    } else {
                        builder.append(escapeString(start.toString()));
                    }
                }
                if (end != null) {
                    if (start != null) {
                        builder.append(" AND ");
                    }
                    builder.append(name);
                    if (inclusive) {
                        builder.append(" <= ");
                    } else {
                        builder.append(" < ");
                    }
                    if (end instanceof Long) {
                        builder.append(end.toString());
                    } else {
                        builder.append(escapeString(end.toString()));
                    }
                }
                if (substr != null) {
                    if (start != null || end != null) {
                        builder.append(" AND ");
                    }
                    builder.append("LOWER(");
                    builder.append(name);
                    builder.append(") LIKE ");
                    builder.append(escapeString("%" + escapeLikeString(substr.toString()) + "%"));
                }
                builder.append(")");
                return builder.toString();
            }
        }

        @Override
        public boolean applicableTo(Field field) {
            if (start != null && !field.type.isInstance(start)) {
                return false;
            }
            if (end != null && !field.type.isInstance(end)) {
                return false;
            }
            if (substr != null && !field.type.isInstance(substr)) {
                return false;
            }
            if (substr != null && !(substr instanceof String)) {
                return false;
            }
            return field.canFilter();
        }

        @Override
        public Long estimateCardinality(Field field) {
            if (field.cardinality != null && start instanceof Long && end instanceof Long) {
                return Long.max(0, (Long) end - (Long) start) * field.cardinality;
            } else {
                return null;
            }
        }
    }

    /**
     * A column filter that accepts only the elements that are equal to the ones
     * it has been constructed with.
     */
    public static class InFilter<T> implements TableValueFilter<T> {
        private final List<T> options;

        @JsonCreator
        public InFilter(List<T> options) {
            this.options = options == null ? List.of() : options;
        }

        @Override
        public boolean accept(T obj) {
            return obj != null && options.contains(obj);
        }

        @Override
        public String asSqlQueryCondition(String name) {
            if (options.isEmpty()) {
                return "FALSE";
            } else {
                StringBuilder builder = new StringBuilder();
                builder.append("(");
                builder.append(name);
                builder.append(" IN (");
                boolean first = true;
                for (Object o : options) {
                    if (!first) {
                        builder.append(", ");
                    }
                    first = false;
                    if (o instanceof Long) {
                        builder.append(o.toString());
                    } else {
                        builder.append(escapeString(o.toString()));
                    }
                }
                builder.append("))");
                return builder.toString();
            }
        }

        @Override
        public boolean applicableTo(Field field) {
            for (Object o : options) {
                if (!field.type.isInstance(o)) {
                    return false;
                }
            }
            return field.canFilter();
        }

        @Override
        public Long estimateCardinality(Field field) {
            if (field.cardinality != null) {
                return (long) options.size() * field.cardinality;
            } else {
                return null;
            }
        }
    }

    @JsonCreator
    public static TableValueFilter<?> fromJsonNode(JsonNode node)
            throws JsonProcessingException, IllegalArgumentException {
        ObjectMapper mapper = new ObjectMapper();
        if (node.isObject()) {
            return mapper.treeToValue(node, RangeFilter.class);
        } else {
            return mapper.treeToValue(node, InFilter.class);
        }
    }

    /**
     * Test whether a given value matches the filter.
     *
     * @param row
     *            The row to test against.
     * @return {@code true} in case we match, {@code false} otherwise.
     */
    public abstract boolean accept(T obj);

    /**
     * Returns an SQL expression that can be used as the condition in a
     * select statement that in the `WHERE` clause to filter only for events
     * relevant to this filter.
     *
     * @param name
     *            The name of the field to check.
     * @return The SQL expression.
     */
    public abstract String asSqlQueryCondition(String name);

    /**
     * Test whether this filter can be used with the given field.
     *
     * @param field
     *            The filed to check against.
     * @return {@code true} if the filter can be used with the field, {@code false}
     *         otherwise.
     */
    public abstract boolean applicableTo(Field field);

    /**
     * Estimate the maximum number of tuples that can be returned in the presence
     * of this filter. This assumes the filter is on a key, i.e., no two tuples
     * will have the same values.
     * 
     * @return The estimated cardinality
     */
    public abstract Long estimateCardinality(Field field);

    /**
     * Escape a string for use in an SQL query.
     *
     * @param string
     *            The string to escape.
     * @return The escaped string.
     */
    public static String escapeString(String string) {
        return "'" + string
                .replace("\\", "\\\\")
                .replace("'", "''")
                .replace("\0", "\\x00") + "'";
    }

    /**
     * Escape a string for use in an SQL `LIKE` expression. This will escape all
     * occurrences of `_` and `%`.
     *
     * @param string
     *            The string to escape.
     * @return The escaped string.
     */
    public static String escapeLikeString(String string) {
        return string
                .replace("_", "\\_")
                .replace("%", "\\%");
    }
}
