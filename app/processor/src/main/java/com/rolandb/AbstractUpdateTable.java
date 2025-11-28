package com.rolandb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a variation of the table builder that will gradually update a single
 * row by overwriting values in the database on a per-field level. This means
 * that we have a sequence number for every single field, allowing for updates
 * to arrive out-of-order at the client, and still be combined correctly.
 */
public abstract class AbstractUpdateTable<E extends AbstractUpdateTable.UpdateSeqRow> extends AbstractTable<E> {
    public static abstract class UpdateSeqRow extends SequencedRow {
        @Override
        @JsonIgnore
        public Object[] getValues() {
            Object[] baseValues = super.getValues();
            Map<String, Long> extra = perFieldSeqNumbers();
            Object[] allValues = new Object[baseValues.length + extra.size()];
            int i = 0;
            for (; i < baseValues.length; i++) {
                allValues[i] = baseValues[i];
            }
            for (String seqNum : UpdateSeqRow.perFieldSeqNumNames(getClass())) {
                allValues[i++] = extra.get(seqNum);
            }
            return allValues;
        }

        @JsonIgnore
        public Object[] getUpdateValues() {
            List<Field> fields = new ArrayList<>();
            for (Field field : this.getClass().getFields()) {
                if (field.getAnnotation(TableEventKey.class) == null) {
                    fields.add(field);
                }
            }
            Object[] result = new Object[fields.size() * 2];
            try {
                for (int i = 0; i < fields.size(); i++) {
                    result[2 * i] = seqNum;
                    result[2 * i + 1] = fields.get(i).get(this);
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new IllegalStateException("Failed to read field values.", e);
            }
            return result;
        }

        @JsonAnyGetter
        public Map<String, Long> perFieldSeqNumbers() {
            Map<String, Long> fields = new HashMap<>();
            for (Field field : this.getClass().getFields()) {
                if (field.getAnnotation(TableEventKey.class) == null) {
                    JsonProperty prop = field.getAnnotation(JsonProperty.class);
                    String name = prop == null ? field.getName() : prop.value();
                    if (!name.equals("seq_num")) {
                        if (getField(field) != null) {
                            fields.put(name + "_seq_num", seqNum);
                        } else {
                            fields.put(name + "_seq_num", 0L);
                        }
                    }
                }
            }
            return fields;
        }

        public static List<String> perFieldSeqNumNames(Class<?> clazz) {
            List<String> fields = new ArrayList<>();
            for (Field field : clazz.getFields()) {
                if (field.getAnnotation(TableEventKey.class) == null) {
                    JsonProperty prop = field.getAnnotation(JsonProperty.class);
                    String name = prop == null ? field.getName() : prop.value();
                    if (!name.equals("seq_num")) {
                        fields.add(name + "_seq_num");
                    }
                }
            }
            return fields;
        }
    }

    protected void buildSqlConflictResolution(String keyNames, Field[] fields, StringBuilder builder) {
        builder.append(" ON CONFLICT (");
        builder.append(keyNames);
        builder.append(") DO UPDATE SET ");
        boolean first = true;
        for (Field field : fields) {
            if (field.getAnnotation(TableEventKey.class) == null) {
                JsonProperty prop = field.getAnnotation(JsonProperty.class);
                String name = prop == null ? field.getName() : prop.value();
                if (!first) {
                    builder.append(", ");
                }
                first = false;
                if (name.equals("seq_num")) {
                    builder.append(name);
                    builder.append(" = GREATEST(EXCLUDED.");
                    builder.append(name);
                    builder.append(",");
                    builder.append(tableName);
                    builder.append(".");
                    builder.append(name);
                    builder.append(")");
                } else {
                    builder.append(name);
                    builder.append(" = CASE WHEN EXCLUDED.");
                    builder.append(name);
                    builder.append("_seq_num > ");
                    builder.append(tableName);
                    builder.append(".");
                    builder.append(name);
                    builder.append("_seq_num THEN ");
                    builder.append("EXCLUDED.");
                    builder.append(name);
                    builder.append(" ELSE ");
                    builder.append(tableName);
                    builder.append(".");
                    builder.append(name);
                    builder.append(" END, ");
                    builder.append(name + "_seq_num");
                    builder.append(" = GREATEST(EXCLUDED.");
                    builder.append(name + "_seq_num");
                    builder.append(",");
                    builder.append(tableName);
                    builder.append(".");
                    builder.append(name + "_seq_num");
                    builder.append(")");
                }
            }
        }
    }

    protected void buildSqlExtraFields(Class<E> output, StringBuilder builder) {
        for (String seqNum : UpdateSeqRow.perFieldSeqNumNames(output)) {
            builder.append(", ");
            builder.append(seqNum);
        }
    }

    protected void buildSqlExtraValues(Class<E> output, StringBuilder builder) {
        for (int i = 0; i < UpdateSeqRow.perFieldSeqNumNames(output).size(); i++) {
            builder.append(", ?");
        }
    }
}
