package com.rolandb;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a subclass of table with a constructor that automatically adds
 * additional fields for the fields that must exists for all of them.
 */
public class RankingTable extends Table {
    private static List<Field> expandFields(List<Field> fields, Long rowCard) {
        List<Field> expanded = new ArrayList<>();
        expanded.addAll(fields);
        expanded.add(new Field("row_number", FieldKind.INDEXED, Long.class, rowCard));
        expanded.add(new Field("rank", Long.class));
        expanded.add(new Field("max_rank", Long.class, false));
        expanded.add(new Field("old_row_number", Long.class, false));
        expanded.add(new Field("old_rank", Long.class, false));
        expanded.add(new Field("old_max_rank", Long.class, false));
        return expanded;
    }

    public RankingTable(String name, Long maxLimit, Long rowCard, List<Field> fields) {
        super(name, maxLimit, expandFields(fields, rowCard));
    }
}
