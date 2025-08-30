package com.rolandb;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import com.rolandb.Table.TableField;

/**
 * Interface used for filters that filter based on a single columns value.
 */
@JsonDeserialize(using = TableValueFilter.Deserializer.class)
public interface TableValueFilter<T extends Comparable<T>> {
    /**
     * A column filter that retains only a range of values. The values of start
     * or end may be omitted (set to null) to get an open interval.
     */
    public static class RangeFilter<T extends Comparable<T>> implements TableValueFilter<T> {
        private final T start;
        private final T end;
        private final boolean inclusive;

        @JsonCreator
        public RangeFilter(
                @JsonProperty("start") T start, @JsonProperty("end") T end,
                @JsonProperty(value = "inclusive", defaultValue = "false") boolean inclusive) {
            this.start = start;
            this.end = end;
            this.inclusive = inclusive;
        }

        @Override
        public boolean accept(T obj) {
            if (start != null && obj.compareTo(start) < 0) {
                return false;
            }
            if (end != null && obj.compareTo(end) >= 0 && (!inclusive || obj.compareTo(end) > 0)) {
                return false;
            }
            return true;
        }

        @Override
        public String asSqlQueryCondition(String name) {
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
            builder.append(")");
            return builder.toString();
        }

        @Override
        public boolean applicableTo(TableField field) {
            if (start != null && !field.type.isInstance(start)) {
                return false;
            }
            if (end != null && !field.type.isInstance(end)) {
                return false;
            }
            return field.isKey;
        }
    }

    /**
     * A column filter that accepts only the elements that are equal to the ones
     * it has been constructed with.
     */
    public static class InFilter<T extends Comparable<T>> implements TableValueFilter<T> {
        private final List<T> options;

        @JsonCreator
        public InFilter(List<T> options) {
            this.options = options;
        }

        @Override
        public boolean accept(T obj) {
            return options.contains(obj);
        }

        @Override
        public String asSqlQueryCondition(String name) {
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

        @Override
        public boolean applicableTo(TableField field) {
            for (Object o : options) {
                if (!field.type.isInstance(o)) {
                    return false;
                }
            }
            return field.isKey;
        }
    }

    public static class Deserializer<T extends Comparable<T>> extends JsonDeserializer<TableValueFilter<T>> {
        @Override
        public TableValueFilter<T> deserialize(JsonParser p, DeserializationContext ctx)
                throws IOException, JacksonException {
            ObjectMapper mapper = (ObjectMapper) p.getCodec();
            JsonToken token = p.getCurrentToken();
            if (token == JsonToken.START_ARRAY) {
                // Lists should deserialize as `InFilter`
                JavaType type = mapper.getTypeFactory().constructType(new TypeReference<InFilter<T>>() {
                });
                return mapper.readValue(p, type);
            } else if (token == JsonToken.START_OBJECT) {
                // Objects should deserialize as `RangeFilter`
                JavaType type = mapper.getTypeFactory().constructType(new TypeReference<RangeFilter<T>>() {
                });
                return mapper.readValue(p, type);
            } else {
                throw ctx.wrongTokenException(p, TableValueFilter.class, token, "Should be an array ot object");
            }
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
    public abstract boolean applicableTo(TableField field);

    /**
     * Escape a string for use in an SQL query.
     * 
     * @param string
     *            The string to escape.
     * @return The escaped string.
     */
    public static String escapeString(String string) {
        return "'" + string
                .replaceAll("\\", "\\\\")
                .replaceAll("'", "''")
                .replaceAll("\0", "\\x00") + "'";
    }
}
