package com.rolandb;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a subclass of table with a constructor that automatically adds
 * additional fields for the per-field sequence numbers present in the users
 * and repos table.
 */
public class UpdateTable extends Table {
    private static List<Field> expandFields(List<Field> fields) {
        List<Field> expanded = new ArrayList<>();
        expanded.addAll(fields);
        for (Field f : fields) {
            if (!f.isKey() && !f.name.equals("seq_num")) {
                expanded.add(new Field(f.name + "_seq_num", Long.class));
            }
        }
        return expanded;
    }

    public UpdateTable(String name, Long maxLimit, List<Field> fields) {
        super(name, maxLimit, expandFields(fields));
    }
}
