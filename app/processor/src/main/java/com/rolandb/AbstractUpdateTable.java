package com.rolandb;

import java.lang.reflect.Field;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a variation of the table builder that will gradually update a single
 * row by overwriting values in the database only for non-null fields in the
 * event. There exists also the option to only update values that are not
 * already non-null in the database.
 */
public abstract class AbstractUpdateTable<E extends SequencedRow> extends AbstractTable<E> {
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
                builder.append(name);
                builder.append(" = CASE WHEN ");
                builder.append(tableName);
                builder.append(".seq_num < EXCLUDED.seq_num THEN ");
                builder.append("COALESCE(");
                builder.append("EXCLUDED.");
                builder.append(name);
                builder.append(", ");
                builder.append(tableName);
                builder.append(".");
                builder.append(name);
                builder.append(")");
                builder.append(" ELSE ");
                builder.append("COALESCE(");
                builder.append(tableName);
                builder.append(".");
                builder.append(name);
                builder.append(", ");
                builder.append("EXCLUDED.");
                builder.append(name);
                builder.append(")");
                builder.append("END");
            }
        }
    }
}
