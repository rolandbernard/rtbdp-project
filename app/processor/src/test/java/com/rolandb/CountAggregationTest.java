package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CountAggregationTest {

    private CountAggregation<Object> countAggregation;

    @BeforeEach
    public void setUp() {
        countAggregation = new CountAggregation<>();
    }

    @Test
    public void testCreateAccumulator() {
        assertEquals(0L, countAggregation.createAccumulator());
    }

    @Test
    public void testAdd() {
        Long accumulator = countAggregation.createAccumulator();
        accumulator = countAggregation.add(new Object(), accumulator);
        assertEquals(1L, accumulator);
        accumulator = countAggregation.add(new Object(), accumulator);
        assertEquals(2L, accumulator);
    }

    @Test
    public void testGetResult() {
        Long accumulator = countAggregation.createAccumulator();
        accumulator = countAggregation.add(new Object(), accumulator);
        assertEquals(1L, countAggregation.getResult(accumulator));
    }

    @Test
    public void testMerge() {
        Long accumulator1 = countAggregation.createAccumulator();
        accumulator1 = countAggregation.add(new Object(), accumulator1);
        accumulator1 = countAggregation.add(new Object(), accumulator1);

        Long accumulator2 = countAggregation.createAccumulator();
        accumulator2 = countAggregation.add(new Object(), accumulator2);
        accumulator2 = countAggregation.add(new Object(), accumulator2);
        accumulator2 = countAggregation.add(new Object(), accumulator2);

        Long mergedAccumulator = countAggregation.merge(accumulator1, accumulator2);
        assertEquals(5L, mergedAccumulator);
    }
}
