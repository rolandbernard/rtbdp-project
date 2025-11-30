package com.rolandb;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a subclass of table with a constructor that automatically adds
 * additional fields for the per-field sequence numbers present in the users
 * and repos table.
 */
public class UpdateTable extends Table {
    /**
     * Expand the list of filed by adding field for the per-field sequence numbers
     * present in this kind of table.
     * 
     * @param fields
     *            The base fields.
     * @return The extended list of fields.
     */
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

    /**
     * Create a new update table.
     * 
     * @param name
     *            The name of the table.
     * @param maxLimit
     *            The maximum cardinality allowed for replays.
     * @param fields
     *            The field of the table, without the per-field sequence numbers.
     */
    public UpdateTable(String name, Long maxLimit, List<Field> fields) {
        super(name, maxLimit, expandFields(fields));
    }
}
