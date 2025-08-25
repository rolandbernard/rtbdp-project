package com.rolandb;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.rolandb.AbstractTableBuilder.TableEventKey;

/**
 * This is a simple container that contains both a timestamp and a row. The
 * timestamp is set to the time when the object instance is constructed.
 */
public class TimedRow {
    private final Object row;
    private final Instant time;

    public TimedRow(Object row) {
        this.row = row;
        time = Instant.now();
    }

    public Object getRow() {
        return row;
    }

    public Instant getTime() {
        return time;
    }

    public Object getField(Field field) {
        try {
            return field.get(row);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            // Should actually never happen.
            return null;
        }
    }

    public List<?> getKey() {
        return readKeyFrom(row);
    }

    public Object[] getValues() {
        return readValuesFrom(row);
    }

    @Override
    public String toString() {
        return row + " @ " + time;
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
        return keys.isEmpty() ? null : keys;
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
