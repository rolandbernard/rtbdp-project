package com.rolandb;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.rolandb.Table.Field;

/**
 * Class used for filters that filter based on a single columns value.
 * 
 * @param <T>
 *            The type of value that can be filtered with this filter.
 */
public class TableValueFilter<T> {
    /** The list of values that should pass the filter. */
    private final List<T> options;
    /** The minimum value passing the filter. */
    private final T start;
    /** The upper bound on passing values. */
    private final T end;
    /** A string that must be a subset of all passing values. */
    private final T substr;
    /** Whether the upper bound is inclusive or not. */
    private final boolean inclusive;

    /**
     * Create a new range based filer.
     * 
     * @param start
     *            The minimum value passing the filter. Or null of there is no lower
     *            bound.
     * @param end
     *            The upper bound on values passing the filter. Or null if there is
     *            no upper bound.
     * @param substr
     *            The substring that must be included in the lowercase version of
     *            the filed value. Or null to ignore it.
     * @param inclusive
     *            Whether the upper bound is inclusive or not.
     * @param options
     *            The set of values that should pass the filter.
     */
    @JsonCreator
    public TableValueFilter(
            @JsonProperty("start") T start, @JsonProperty("end") T end, @JsonProperty("substr") T substr,
            @JsonProperty(value = "inclusive", defaultValue = "false") boolean inclusive,
            @JsonProperty("opt") List<T> options) {
        this.start = start;
        this.end = end;
        this.substr = substr;
        this.inclusive = inclusive;
        this.options = options;
    }

    /**
     * Test whether a given value matches the filter.
     *
     * @param row
     *            The row to test against.
     * @return {@code true} in case we match, {@code false} otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean accept(T obj) {
        Comparable<T> cmp = (Comparable<T>) obj;
        if (options != null && !options.contains(obj)) {
            return false;
        }
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

    /**
     * Returns an SQL expression that can be used as the condition in a
     * select statement that in the `WHERE` clause to filter only for events
     * relevant to this filter.
     *
     * @param name
     *            The name of the field to check.
     * @return The SQL expression.
     */
    public String asSqlQueryCondition(String name) {
        if (start == null && end == null && substr == null && options == null) {
            return "TRUE";
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            if (options != null) {
                if (options.isEmpty()) {
                    builder.append("FALSE");
                } else if (options.size() == 1 && options.get(0) == null) {
                    builder.append("(" + name + " IS NULL)");
                } else {
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
                }
            }
            if (start != null) {
                if (options != null) {
                    builder.append(" AND ");
                }
                builder.append(name);
                builder.append(" >= ");
                if (start instanceof Long) {
                    builder.append(start.toString());
                } else {
                    builder.append(escapeString(start.toString()));
                }
            }
            if (end != null) {
                if (options != null || start != null) {
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
                if (options != null || start != null || end != null) {
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

    /**
     * Test whether this filter can be used with the given field.
     *
     * @param field
     *            The filed to check against.
     * @return {@code true} if the filter can be used with the field, {@code false}
     *         otherwise.
     */
    public boolean applicableTo(Field field) {
        if (options != null && (options.size() != 1 || options.get(0) != null)) {
            for (Object o : options) {
                if (!field.type.isInstance(o)) {
                    return false;
                }
            }
        }
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
        return true;
    }

    /**
     * Estimate the maximum number of tuples that can be returned in the presence
     * of this filter. This assumes the filter is on a key, i.e., no two tuples
     * will have the same values.
     * 
     * @param field
     *            The field the filter is applied to.
     * @return The estimated cardinality
     */
    public Long estimateCardinality(Field field) {
        if (field.cardinality != null) {
            Long minEst = null;
            if (options != null) {
                minEst = (long) options.size() * field.cardinality;
            }
            if (start instanceof Long && end instanceof Long) {
                long est = Long.max(0, (Long) end - (Long) start) * field.cardinality;
                if (minEst == null || est < minEst) {
                    minEst = est;
                }
            }
            return minEst;
        } else {
            return null;
        }
    }

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
