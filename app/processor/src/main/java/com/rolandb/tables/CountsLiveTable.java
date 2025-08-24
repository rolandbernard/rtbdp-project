package com.rolandb.tables;

import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;

import com.rolandb.AbstractTableBuilder;

import static org.apache.flink.table.api.Expressions.*;

public class CountsLiveTable extends AbstractTableBuilder {
    protected Table computeTable() {
        return getEventTable()
                .window(Slide.over(lit(5).minutes()).every(lit(15).seconds()).on($("created_at")).as("w"))
                .groupBy($("w"), $("kind"))
                .select(
                        $("w").start().as("ts_start"),
                        $("w").end().as("ts_end"),
                        $("kind"),
                        lit("5m").as("window_size"),
                        $("kind").count().as("num_events"));
    }

    protected String[] getPrimaryKeyNames() {
        return new String[] { "window_size", "kind" };
    }
}
