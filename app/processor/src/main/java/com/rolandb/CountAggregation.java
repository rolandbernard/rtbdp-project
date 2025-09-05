package com.rolandb;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * A simple aggregation function that simply counts the number of received
 * elements and returned that as a Long.
 */
public class CountAggregation<T> implements AggregateFunction<T, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(T value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
