package com.rolandb;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a subclass of table with a constructor that automatically adds
 * additional fields for the fields that must exists for all of them.
 */
public class RankingTable extends Table {
    /**
     * Expand the given fields by adding those that are present in all ranking
     * tables.
     * 
     * @param fields
     *            The table specific fields.
     * @param rowCard
     *            The cardinality to assign to the {@code row_number} field.
     * @return The table specific field plus the common ranking field.
     */
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

    /**
     * Create a new ranking table.
     * 
     * @param name
     *            The name of the table.
     * @param maxLimit
     *            The maximum cardinality to allow for replay requests.
     * @param rowCard
     *            The cardinality for a single row number.
     * @param fields
     *            The fields that are specific to this ranking table.
     */
    public RankingTable(String name, Long maxLimit, Long rowCard, List<Field> fields) {
        super(name, maxLimit, expandFields(fields, rowCard));
    }

    @Override
    protected String asSqlQueryTableName(Subscription subscription) {
        Long card = subscription.estimateCardinality(this);
        if (card != null && card < 5 && subscription.usesOnlyKey(this)) {
            return name + "_point";
        } else {
            return name;
        }
    }
}
