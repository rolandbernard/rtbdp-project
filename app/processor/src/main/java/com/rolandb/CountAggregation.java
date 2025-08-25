package com.rolandb;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * A simple aggregation function that simply counts the number of received
 * elements and returned that as an Integer.
 */
public class CountAggregation implements AggregateFunction<GithubEvent, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(GithubEvent value, Integer accumulator) {
        return accumulator + 1;
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}
