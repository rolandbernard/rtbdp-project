package com.rolandb;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * A {@link KeyedProcessFunction} that can efficiently collect multiple sliding
 * windows of different lengths at once. All windows share the same hop distance
 * but can have a different size. Note that the sizes of the windows is
 * restricted to be a multiple of the hop duration. Note that this
 * implementation may send multiple events for the same window, updating it with
 * late arriving events. As such the output of this operator should be used as
 * an upsert with the key and window as the primary key.
 * Important: reconfiguration of the operator is not supported. If old state
 * values are loaded from a checkpoint, the results might be wrong if the window
 * sizes or slide duration are not the same. Names or collection function may
 * be changed however.
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
    private final ResultFunction<K, R> function;

    // Buckets by Timestamp
    private static final MapStateDescriptor<Long, Long> bucketsDesc = new MapStateDescriptor<>(
            "bucketCounts", TypeInformation.of(Long.class), TypeInformation.of(Long.class));
    private transient MapState<Long, Long> bucketCounts;
    // Rolling Sum per Window
    private static final ValueStateDescriptor<long[]> totalsDesc = new ValueStateDescriptor<>(
            "windowTotals", PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO);
    private transient ValueState<long[]> windowTotals;
    // When the last window was closed.
    private static final ValueStateDescriptor<Long> lastTimerDesc = new ValueStateDescriptor<>(
            "lastClosing", TypeInformation.of(Long.class));
    private transient ValueState<Long> lastTimer;

    public MultiSlidingBuckets(Duration slide, List<WindowSpec> windows, ResultFunction<K, R> function) {
        this.slideMs = slide.toMillis();
        // We sort the windows, so the first is the shortest and the last is the
        // longers. The sort is stable so we get the same result each time.
        this.windows = windows.stream().sorted((a, b) -> Long.compare(a.sizeMs, b.sizeMs)).toList();
        this.function = function;
        // We only support exact multiples of the slide duration.
        windows.stream().forEach(w -> {
            Preconditions.checkArgument(
                    w.sizeMs % slideMs == 0, "Window size must be multiple of slide duration");
        });
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        bucketCounts = getRuntimeContext().getMapState(bucketsDesc);
        windowTotals = getRuntimeContext().getState(totalsDesc);
        lastTimer = getRuntimeContext().getState(lastTimerDesc);
        // Quick sanity check. Necessary but not sufficient.
        long[] lastTotals = windowTotals.value();
        Preconditions.checkState(lastTotals == null || lastTotals.length == windows.size(),
                "The stored state size does not match the window specifications");
    }

    private void addNextBucketTimer(Context ctx, long bucketEnd, Long lastClose) throws Exception {
        // There is always one timer per bucket at one time. Some of the timers
        // of different buckets may overlap, but that is fine in Flink.
        if (lastClose == null || bucketEnd > lastClose) {
            // Register an event-time timer for when this bucket needs to be added
            // to the totals. In the onTimer for this we will add another timer.
            ctx.timerService().registerEventTimeTimer(bucketEnd);
        } else {
            // Since this bucket has already been added to the different windows
            // add instead a timer for when it gets removed from the first window.
            for (WindowSpec spec : windows) {
                long size = spec.sizeMs / slideMs;
                if (bucketEnd + size * slideMs > lastClose) {
                    ctx.timerService().registerEventTimeTimer(bucketEnd + size * slideMs);
                    return;
                }
            }
            // I we get here, can remove now the bucket since there is no more
            // any window total that needs it for updating. Note that for every
            // bucket we go through all of the steps, before removing it completely.
            bucketCounts.remove(bucketEnd);
        }
    }

    @Override
    public void processElement(E event, Context ctx, Collector<R> out) throws Exception {
        long ts = Preconditions.checkNotNull(ctx.timestamp(), "Event timestamp must be assigned");
        long bucketEnd = (ts / slideMs + 1) * slideMs;
        // Check if this bucket still has to be updated. Not that if the last
        // close will have removed the bucket at `lastClose - maxWindowsSize * slideMs`,
        // so if this event is even older we should completely discard it.
        Long lastClose = lastTimer.value();
        long maxWindowsSize = windows.get(windows.size() - 1).sizeMs / slideMs;
        if (lastClose == null || bucketEnd > lastClose - maxWindowsSize * slideMs) {
            Long current = bucketCounts.get(bucketEnd);
            bucketCounts.put(bucketEnd, current == null ? 1 : current + 1);
            // We only need to register new timers if this is a new bucket. Otherwise
            // some timer will already exist. For every bucket we will only have
            // one timer at a time.
            if (current == null) {
                addNextBucketTimer(ctx, bucketEnd, lastClose);
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
        Long lastClose = lastTimer.value();
        // Should be guaranteed by `addNextBucketTimer`.
        Preconditions.checkState(lastClose == null || lastClose < bucketEnd, "Timer must run strictly in order");
        lastTimer.update(bucketEnd);
        Long addedCount = bucketCounts.get(bucketEnd);
        if (addedCount == null) {
            addedCount = 0L;
        } else {
            // This bucket must be removed again later.
            addNextBucketTimer(ctx, bucketEnd, bucketEnd);
        }
        long[] totals = windowTotals.value();
        if (totals == null) {
            totals = new long[windows.size()];
        }
        boolean allZero = true;
        for (int i = 0; i < totals.length; i++) {
            WindowSpec spec = windows.get(i);
            long expiredBucketEnd = bucketEnd - (spec.sizeMs / slideMs) * slideMs;
            Long expiredCount = bucketCounts.get(expiredBucketEnd);
            if (expiredCount == null) {
                expiredCount = 0L;
            } else {
                // There is a bucket here. We have to make sure we have a timer
                // for the next time we use it, or remove it if we don't use it
                // in any window anymore.
                addNextBucketTimer(ctx, expiredBucketEnd, bucketEnd);
            }
            if (addedCount != expiredCount) {
                totals[i] += addedCount - expiredCount;
                out.collect(function.apply(
                        Instant.ofEpochMilli(expiredBucketEnd),
                        Instant.ofEpochMilli(bucketEnd),
                        ctx.getCurrentKey(), spec, totals[i]));
            }
            if (totals[i] != 0) {
                allZero = false;
            }
        }
        if (allZero) {
            // We can just remove the state altogether.
            windowTotals.clear();
        } else {
            windowTotals.update(totals);
        }
    }
}
