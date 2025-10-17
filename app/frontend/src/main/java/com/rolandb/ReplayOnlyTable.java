package com.rolandb;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.reactivex.rxjava3.core.Observable;

/**
 * This is a subclass of table that does not have any topic in Kafka, and
 * instead can only be refreshed by loading the table from PostgreSQL. Currently
 * this means the subscription will not produce any rows, but in principle we
 * could periodically reload the table.
 */
public class ReplayOnlyTable extends Table {
    public ReplayOnlyTable(String name, Long maxLimit, List<Field> fields) {
        super(name, maxLimit, fields);
    }

    @Override
    public void startLiveObservable(Properties kafkaProperties) {
        // Do nothing, we don't have a live replay.
    }

    @Override
    public void stopLiveObservable() throws InterruptedException {
        // Do nothing, we don't have a live replay.
    }

    @Override
    public Observable<Map<String, ?>> getLiveObservable() {
        // Since we don't have any live updates, just give an empty observable.
        return Observable.empty();
    }
}
