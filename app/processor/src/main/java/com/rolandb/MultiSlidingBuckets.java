package com.rolandb;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * A {@link KeyedProcessFunction} that can efficiently collect multiple sliding
 * windows of different lengths at once. All windows share the same hop distance
 * but can have a different size. Note that the sizes of the windows is
 * restricted to be a multiple of the hop duration.
 */
public class MultiSlidingBuckets<K, E, R> extends KeyedProcessFunction<K, E, R> {
    public static class WindowSpec implements Serializable {
        public final String name;
        public final long sizeMs;

        public WindowSpec(String name, Duration size) {
            this.name = name;
            this.sizeMs = size.toMillis();
        }
    }

    public static interface ResultFunction<K, R> extends Serializable {
        public abstract R apply(Instant ws, Instant we, K k, WindowSpec win, Long count);
    }

    private final long slideMs;
    private final List<WindowSpec> windows;
    private final long maxWindowMs;
    private final ResultFunction<K, R> function;

    // Buckets by Timestamp
    private transient MapState<Long, Long> bucketCounts;
    // Rolling Sum per Window
    private transient ValueState<long[]> windowTotals;
    // When the last window was closed.
    private transient ValueState<Long> lastTimer;

    public MultiSlidingBuckets(Duration slide, List<WindowSpec> windows, ResultFunction<K, R> function) {
        this.slideMs = slide.toMillis();
        this.windows = windows;
        this.maxWindowMs = windows.stream().mapToLong(w -> w.sizeMs).max().orElse(slideMs);
        this.function = function;
        windows.stream().forEach(w -> {
            // We only support exact multiples of the slide duration.
            assert w.sizeMs % slideMs == 0;
        });
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        MapStateDescriptor<Long, Long> bucketsDesc = new MapStateDescriptor<>(
                "bucketCounts", TypeInformation.of(Long.class), TypeInformation.of(Long.class));
        ValueStateDescriptor<long[]> totalsDesc = new ValueStateDescriptor<>(
                "windowTotals", TypeInformation.of(long[].class));
        ValueStateDescriptor<Long> lastTimerDesc = new ValueStateDescriptor<>(
                "lastClosing", TypeInformation.of(Long.class));
        bucketCounts = getRuntimeContext().getMapState(bucketsDesc);
        windowTotals = getRuntimeContext().getState(totalsDesc);
        lastTimer = getRuntimeContext().getState(lastTimerDesc);
    }

    @Override
    public void processElement(E event, Context ctx, Collector<R> out) throws Exception {
        long ts = ctx.timestamp();
        long bucketEnd = (ts / slideMs + 1) * slideMs;
        // Check if this bucket still has to be updated. Not that if the last
        // close will have removed the bucket at `lastClose - maxWindowsSize * slideMs`,
        // so if this event is even older we should completely discard it.
        Long lastClose = lastTimer.value();
        long maxWindowsSize = maxWindowMs / slideMs;
        if (lastClose == null || bucketEnd > lastClose - maxWindowsSize * slideMs) {
            Long current = bucketCounts.get(bucketEnd);
            bucketCounts.put(bucketEnd, current == null ? 1 : current + 1);
            // We only need to register new timers if this is a new bucket. Otherwise
            // those timers already exist.
            if (current == null) {
                // Register an event-time timer for when this bucket ends.
                if (lastClose == null || bucketEnd > lastClose) {
                    ctx.timerService().registerEventTimeTimer(bucketEnd);
                }
                // For each window size also register an event for when the bucket has
                // to be removed from the running totals.
                for (WindowSpec spec : windows) {
                    long size = spec.sizeMs / slideMs;
                    if (lastClose == null || bucketEnd + size * slideMs > lastClose) {
                        ctx.timerService().registerEventTimeTimer(bucketEnd + size * slideMs);
                    }
                }
            }
            // If this bucket has already been added to some of the window totals,
            // we need to update the value in the totals. We will unpublish a new
            // upsert event.
            if (lastClose != null && bucketEnd <= lastClose) {
                long[] totals = windowTotals.value();
                if (totals == null) {
                    totals = new long[windows.size()];
                }
                for (int i = 0; i < totals.length; i++) {
                    WindowSpec spec = windows.get(i);
                    long size = spec.sizeMs / slideMs;
                    if (bucketEnd > lastClose - size * slideMs) {
                        totals[i]++;
                        out.collect(function.apply(
                                Instant.ofEpochMilli(bucketEnd - size * slideMs),
                                Instant.ofEpochMilli(bucketEnd),
                                ctx.getCurrentKey(), spec, totals[i]));
                    }
                }
                windowTotals.update(totals);
            }
        }
    }

    @Override
    public void onTimer(long bucketEnd, OnTimerContext ctx, Collector<R> out) throws Exception {
        lastTimer.update(bucketEnd);
        Long addedCount = bucketCounts.get(bucketEnd);
        if (addedCount == null) {
            addedCount = 0L;
        }
        long[] totals = windowTotals.value();
        if (totals == null) {
            totals = new long[windows.size()];
        }
        boolean allZero = true;
        for (int i = 0; i < totals.length; i++) {
            totals[i] += addedCount;
            WindowSpec spec = windows.get(i);
            long size = spec.sizeMs / slideMs;
            long expiredBucketEnd = bucketEnd - size * slideMs;
            Long expiredCount = bucketCounts.get(expiredBucketEnd);
            if (expiredCount == null) {
                expiredCount = 0L;
            }
            totals[i] -= expiredCount;
            if (totals[i] != 0) {
                allZero = false;
            }
            if (addedCount != expiredCount) {
                out.collect(function.apply(
                        Instant.ofEpochMilli(expiredBucketEnd),
                        Instant.ofEpochMilli(bucketEnd),
                        ctx.getCurrentKey(), spec, totals[i]));
            }
        }
        if (allZero) {
            // We can just remove the state altogether.
            windowTotals.clear();
        } else {
            windowTotals.update(totals);
        }
        // We can remove now the bucket that was just expired in the longest
        // window. Clearly the shorter windows had this bucket expire before,
        // and so it is no longer needed for any window. Since in the element
        // handling we setup a timer for every window size, this will remove
        // for every bucket that we ever write to eventually.
        long maxWindowsSize = maxWindowMs / slideMs;
        bucketCounts.remove(bucketEnd - maxWindowsSize * slideMs);
    }
}
