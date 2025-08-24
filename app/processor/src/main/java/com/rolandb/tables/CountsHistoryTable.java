package com.rolandb.tables;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;

import com.rolandb.AbstractTableBuilder;

import static org.apache.flink.table.api.Expressions.*;

public class CountsHistoryTable extends AbstractTableBuilder {
    protected Table computeTable() {
        return getEventTable()
                .window(Tumble.over(lit(5).minutes()).on($("created_at")).as("w"))
                .groupBy($("w"), $("kind"))
                .select(
                        $("w").start().as("ts_start"),
                        $("w").end().as("ts_end"),
                        $("kind"),
                        lit("5m").as("window_size"),
                        $("kind").count().as("num_events"));
    }

    protected String[] getPrimaryKeyNames() {
        return new String[] { "ts_start", "ts_end", "window_size", "kind" };
    }
}
