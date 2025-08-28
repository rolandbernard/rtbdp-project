package com.rolandb;

import java.util.List;

/**
 * In this application, a table consists of both a relational table in
 * PostgreSQL storing the latest state, and a Kafka topic that streams all of
 * the
 * changes to that table. This class provides an abstraction over that.
 */
public class Table {
    public static class TableField {
        public final String name;
        public final boolean isKey;
        public final boolean isNumeric;

        public TableField(String name, boolean isKey, boolean isNumeric) {
            this.name = name;
            this.isKey = isKey;
            this.isNumeric = isNumeric;
        }
    };

    private final String name;
    private final List<TableField> fields;

    public Table(String name, List<TableField> fields) {
        this.name = name;
        this.fields = fields;
    }
}
