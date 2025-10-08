package com.rolandb;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
public class MultiSlidingBuckets<K, E, R, W extends MultiSlidingBuckets.WindowSpec>
        extends KeyedProcessFunction<K, E, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiSlidingBuckets.class);

    public static interface WindowSpec extends Serializable {
        public long sizeInMs();
    }

    public static interface ResultFunction<K, R, W> extends Serializable {
        public abstract R apply(Instant ws, Instant we, K k, W win, long count);
    }

    private final long slideMs;
    private final List<W> windows;
    private final ResultFunction<K, R, W> function;

    // Buckets by Timestamp
    private transient MapState<Long, Long> bucketCounts;
    // Rolling Sum per Window
    private transient ValueState<long[]> windowTotals;
    // When the last window was closed.
    private transient ValueState<Long> lastTimer;

    public MultiSlidingBuckets(Duration slide, List<W> windows, ResultFunction<K, R, W> function) {
        this.slideMs = slide.toMillis();
        // We sort the windows, so the first is the shortest and the last is the
        // longers. The sort is stable so we get the same result each time.
        this.windows = windows.stream().sorted((a, b) -> Long.compare(a.sizeInMs(), b.sizeInMs())).toList();
        this.function = function;
        // We only support exact multiples of the slide duration.
        windows.stream().forEach(w -> {
            Preconditions.checkArgument(
                    w.sizeInMs() % slideMs == 0, "Window size must be multiple of slide duration");
        });
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        MapStateDescriptor<Long, Long> bucketDesc = new MapStateDescriptor<>("bucketCounts", Long.class, Long.class);
        ValueStateDescriptor<long[]> totalsDesc = new ValueStateDescriptor<>("windowTotals", long[].class);
        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("lastClosing", Long.class);
        // As a failsafe, to avoid loosing some state and never cleaning it up, we just
        // use a TTL that is double the duration of the longest window.
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofMillis(windows.getLast().sizeInMs() * 2))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        bucketDesc.enableTimeToLive(ttlConfig);
        totalsDesc.enableTimeToLive(ttlConfig);
        timerDesc.enableTimeToLive(ttlConfig);
        bucketCounts = getRuntimeContext().getMapState(bucketDesc);
        windowTotals = getRuntimeContext().getState(totalsDesc);
        lastTimer = getRuntimeContext().getState(timerDesc);
    }

    private void addNextBucketTimer(Context ctx, long bucketEnd, Long lastClose) throws Exception {
        // There is always one timer per bucket at one time. Some of the timers
        // of different buckets may overlap, but that is fine in Flink.
        if (lastClose == null || bucketEnd > lastClose) {
            // Register an event-time timer for when this bucket needs to be added
            // to the totals. In the onTimer for this we will add another timer.
            ctx.timerService().registerEventTimeTimer(bucketEnd);
        } else {
            // Since this bucket has already been added to the different windows,
            // add instead a timer for when it gets removed from the first window.
            for (WindowSpec spec : windows) {
                long size = spec.sizeInMs() / slideMs;
                if (bucketEnd + size * slideMs > lastClose) {
                    ctx.timerService().registerEventTimeTimer(bucketEnd + size * slideMs);
                    return;
                }
            }
            // If we get here, we can remove now the bucket since there is no more
            // window totals that need it to be removed. Note that for every
            // bucket we go through all of the steps, before removing it completely.
            bucketCounts.remove(bucketEnd);
        }
    }

    /**
     * Small helper to get the current window totals, and initialize them with
     * zeros in case they don't exists yet.
     *
     * @return The window totals.
     * @throws IOException
     */
    private long[] getWindowTotals() throws IOException {
        long[] totals = windowTotals.value();
        if (totals == null) {
            totals = new long[windows.size()];
        }
        // Quick sanity check. Necessary but not sufficient.
        Preconditions.checkState(totals.length == windows.size(),
                "The stored state size does not match the window specifications");
        return totals;
    }

    @Override
    public void processElement(E event, Context ctx, Collector<R> out) throws Exception {
        long ts = Preconditions.checkNotNull(ctx.timestamp(), "Event timestamp must be assigned");
        long bucketEnd = (ts / slideMs + 1) * slideMs;
        // Check if this bucket still has to be updated. Not that if the last
        // close will have removed the bucket at `lastClose - maxWindowsSize * slideMs`,
        // so if this event is even older we should completely discard it.
        Long lastClose = lastTimer.value();
        long maxWindowsSize = windows.get(windows.size() - 1).sizeInMs() / slideMs;
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
            // we need to update the value in the totals. We will publish a new
            // upsert event.
            if (lastClose != null && bucketEnd <= lastClose) {
                long[] totals = getWindowTotals();
                for (int i = 0; i < totals.length; i++) {
                    W spec = windows.get(i);
                    long size = spec.sizeInMs() / slideMs;
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
        long[] totals = getWindowTotals();
        boolean allZero = true;
        for (int i = 0; i < totals.length; i++) {
            W spec = windows.get(i);
            long expiredBucketEnd = bucketEnd - (spec.sizeInMs() / slideMs) * slideMs;
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
                if (totals[i] < 0) {
                    // This must be an error. We try to handle it somewhat gracefully.
                    LOGGER.warn("Underflow in bucketed multi-sliding windows totals.");
                    totals[i] = 0;
                }
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
