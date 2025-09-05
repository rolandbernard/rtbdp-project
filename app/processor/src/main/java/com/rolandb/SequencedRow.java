package com.rolandb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.rolandb.AbstractTableBuilder.TableEventKey;

/**
 * This is a simple container that contains both a row and a sequence number.
 * The.sequence number is to be used for resolving precedence between rows for
 * the same key. This is required for the client to know whether incoming events
 * are newer than the snapshot.
 */
public class SequencedRow<T> {
    private final T row;
    private final long seqNum;

    public SequencedRow(T row, long seqNum) {
        this.row = row;
        this.seqNum = seqNum;
    }

    public T getRow() {
        return row;
    }

    public long getSeqNum() {
        return seqNum;
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
        return row + " @ " + seqNum;
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
