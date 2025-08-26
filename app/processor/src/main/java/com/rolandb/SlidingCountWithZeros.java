package com.rolandb;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SlidingCountWithZeros<K, T, R> extends KeyedProcessFunction<K, T, R> {
    public static interface ResultFunction<A, B, C, D, E, R> extends Serializable {
        public abstract R apply(A a, B b, C c, E e, D d);
    }

    private final List<Integer> windowSizes;
    private final long slideSize;
    private final ResultFunction<Instant, Instant, K, Integer, Integer, R> function;

    private transient MapState<Long, Integer> windowCounts;

    public SlidingCountWithZeros(List<Integer> windowSizes, Duration slideSize,
            ResultFunction<Instant, Instant, K, Integer, Integer, R> function) {
        this.windowSizes = windowSizes;
        this.slideSize = slideSize.toMillis();
        this.function = function;
    }

    @Override
    public void open(OpenContext parameters) {
        windowCounts = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("windowCounts", Long.class, Integer.class));
    }

    @Override
    public void processElement(T event, Context ctx, Collector<R> out) throws Exception {
        long eventTime = ctx.timestamp();
        // Align to the end of the first window that this element is a part of.
        long windowStart = (eventTime / slideSize + 1) * slideSize - windowSize;
        for (; windowStart <= eventTime; windowStart += slideSize) {
            long windowEnd = windowStart + windowSize;
            int current = windowCounts.contains(windowEnd) ? windowCounts.get(windowEnd) : 0;
            windowCounts.put(windowEnd, current + 1);
            // Register timer for window end.
            ctx.timerService().registerEventTimeTimer(windowEnd);
        }
        // Register one timer for the first window this element is not a part of.
        // This is so that we can emit an final `0` count, should there be no more
        // events for this key.
        ctx.timerService().registerEventTimeTimer(windowStart + windowSize);
    }

    @Override
    public void onTimer(long windowEnd, OnTimerContext ctx, Collector<R> out) throws Exception {
        Integer currentCount = windowCounts.get(windowEnd);
        if (currentCount == null) {
            currentCount = 0;
        }
        long windowStart = windowEnd - windowSize;
        out.collect(function.apply(Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowEnd),
                ctx.getCurrentKey(), currentCount));
        windowCounts.remove(windowEnd);
    }
}
