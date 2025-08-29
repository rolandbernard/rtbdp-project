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
    abstract boolean accept(T obj);
}
