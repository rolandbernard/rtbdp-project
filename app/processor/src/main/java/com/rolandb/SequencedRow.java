package com.rolandb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.rolandb.AbstractTable.TableEventKey;

/**
 * This is a simple container that contains both a row and a sequence number.
 * The.sequence number is to be used for resolving precedence between rows for
 * the same key. This is required for the client to know whether incoming events
 * are newer than the snapshot.
 */
public class SequencedRow {
    // A single number that is used by the client (and the database) to ignore
    // updates that arrive to late and are older that what is already in the db.
    @JsonProperty("seq_num")
    public Long seqNum;

    public Object getField(Field field) {
        try {
            return field.get(this);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            // Should actually never happen.
            return null;
        }
    }

    @JsonIgnore
    public List<?> getKey() {
        return readKeyFrom(this);
    }

    @JsonIgnore
    public Object[] getValues() {
        return readValuesFrom(this);
    }

    @Override
    public String toString() {
        return getClass().getName() + " @ " + seqNum;
    }

    public static boolean hasKeyIn(Class<?> clazz) {
        for (Field field : clazz.getFields()) {
            if (field.getAnnotation(TableEventKey.class) != null) {
                return true;
            }
        }
        return false;
    }

    public static List<?> readKeyFrom(Object obj) {
        List<Object> keys = new ArrayList<>();
        for (Field field : obj.getClass().getFields()) {
            if (field.getAnnotation(TableEventKey.class) != null) {
                try {
                    keys.add(field.get(obj));
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    // Should actually never happen.
                    keys.add(null);
                }
            }
        }
        return keys;
    }

    public static Object[] readValuesFrom(Object obj) {
        Field[] fields = obj.getClass().getFields();
        Object[] values = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            try {
                values[i] = fields[i].get(obj);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                // Should actually never happen. Leave it a `null`.
            }
        }
        return values;
    }
}
