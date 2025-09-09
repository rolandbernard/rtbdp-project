package com.rolandb;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * This is a processing function that will keep a ranking of all values based on
 * a key and value extracted from the incoming stream. All the changed rankings
 * above a certain threshold will be emitted, and updated if changed.
 */
public class DynamicRanking<K, E, R, I extends Comparable<I>, V extends Comparable<V>>
        extends KeyedProcessFunction<K, E, R> {
    private static class Tuple<I extends Comparable<I>, V extends Comparable<V>>
            implements Serializable, Comparable<Tuple<I, V>> {
        public final I key;
        public final V value;

        public Tuple(I key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int hashCode() {
            return 31 * key.hashCode() ^ value.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Tuple) {
                Tuple other = (Tuple) obj;
                return key.equals(other.key) && value.equals(other.value);
            } else {
                return false;
            }
        }

        private <C extends Comparable<C>> int compareWithNull(C a, C b) {
            return a == null ? (b == null ? 0 : -1) : (b == null ? 1 : a.compareTo(b));
        }

        @Override
        public int compareTo(Tuple<I, V> other) {
            // `null` means not in the table, meaning it is the lowest possible value.
            int valueComp = compareWithNull(value, other.value);
            if (valueComp != 0) {
                return -valueComp;
            } else {
                return compareWithNull(key, other.key);
            }
        }
    }

    public static interface KeyFunction<I, E> extends Serializable {
        public abstract I apply(E event);
    }

    public static interface ResultFunction<K, R, I, V> extends Serializable {
        public abstract R apply(K key, I i, V v, int row_number, int rank, long ts);
    }

    private final V cutoff;
    private final long discreteMs;
    private final KeyFunction<I, E> keyFunction;
    private final KeyFunction<V, E> valueFunction;
    private final ResultFunction<K, R, I, V> resultFunction;
    // These are needed for the persistent state.
    private final Class<I> keyClass;
    private final Class<V> valueClass;

    // Record the timestamp of the last flush. We will not output updates for the
    // same timestamp multiple times.
    private transient ValueState<Long> lastFlush;
    // The last value for each key.
    private transient MapState<I, V> persistentValues;
    // The set of keys that have changed with their previous positions.
    private transient MapState<Tuple<Long, I>, V> pendingChanges;

    // The current dynamic ranking of all the keys. Can be restored from
    // `persistentValues` in case of recovery from snapshot. This is basically
    // only a more efficiently organized cache of `persistentValues`.
    private transient Map<K, OrderStatisticTree<Tuple<I, V>>> rankings;

    /**
     * Create a new dynamic ranking operator.
     *
     * @param cutoff
     *            The minimal value to keep. Note that all elements with a smaller
     *            value will be dropped of the ranking. If the number of rows in
     *            the ranking shrinks, events with a key of null will be emitted
     *            to indicate that.
     * @param keyFunction
     *            The function for extracting the key.
     * @param valueFunction
     *            The function for extracting the value.
     * @param flushTest
     *            The test, between two events, about whether to flush the ranking.
     * @param resultFunction
     *            The function that generates the output events.
     * @param keyClass
     *            The class of the keys.
     * @param valueClass
     *            The class of the values.
     * @param eventClass
     *            The class of the events.
     */
    public DynamicRanking(
            V cutoff, Duration discrete, KeyFunction<I, E> keyFunction, KeyFunction<V, E> valueFunction,
            ResultFunction<K, R, I, V> resultFunction, Class<I> keyClass, Class<V> valueClass) {
        this.cutoff = cutoff;
        this.discreteMs = discrete.toMillis();
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
        this.resultFunction = resultFunction;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Long> lastFlushDesc = new ValueStateDescriptor<>("lastFlush", Long.class);
        MapStateDescriptor<I, V> persistentDesc = new MapStateDescriptor<>("persistentValues", keyClass, valueClass);
        MapStateDescriptor<Tuple<Long, I>, V> pendingDesc = new MapStateDescriptor<>("pendingChanges",
                TypeInformation.of(new TypeHint<Tuple<Long, I>>() {
                }), TypeInformation.of(valueClass));
        lastFlush = getRuntimeContext().getState(lastFlushDesc);
        persistentValues = getRuntimeContext().getMapState(persistentDesc);
        pendingChanges = getRuntimeContext().getMapState(pendingDesc);
        rankings = new HashMap<>();
    }

    /**
     * Small helper to return the rankings for the current key. If necessary, the
     * ranking is rebuild from the currently saved persistent state.
     *
     * @return The ranking for the current key.
     * @throws Exception
     */
    private OrderStatisticTree<Tuple<I, V>> getRanking(K key) throws Exception {
        OrderStatisticTree<Tuple<I, V>> ranking = rankings.get(key);
        if (ranking == null) {
            // Restore the ranking tree if restoring from a snapshot.
            ranking = new OrderStatisticTree<>();
            for (Entry<I, V> entry : persistentValues.entries()) {
                ranking.add(new Tuple<I, V>(entry.getKey(), entry.getValue()));
            }
            rankings.put(key, ranking);
        }
        return ranking;
    }

    @Override
    public void processElement(E event, Context ctx, Collector<R> out) throws Exception {
        // Append the pending change. We keep these persistent so we don't
        // loose them in case of restoring from a checkpoint.
        I key = keyFunction.apply(event);
        long flushTime = (ctx.timestamp() / discreteMs + 1) * discreteMs;
        V value = valueFunction.apply(event);
        if (key != null && value != null) {
            pendingChanges.put(new Tuple<>(flushTime, key), value);
            Long lastTime = lastFlush.value();
            if (lastTime != null && lastTime <= flushTime) {
                // We already flushed this timestamp. We will wait until the next
                // one and flush it together with those records. We have logic in
                // the flushing to only keep the latest.
                ctx.timerService().registerEventTimeTimer(lastTime + discreteMs);
            } else {
                ctx.timerService().registerEventTimeTimer(flushTime);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<R> out) throws Exception {
        // Find all pending changes before the flush timestamp.
        Map<I, Tuple<Long, V>> changes = new HashMap<>();
        for (Entry<Tuple<Long, I>, V> entry : pendingChanges.entries()) {
            I key = entry.getKey().value;
            Long time = entry.getKey().key;
            if (time <= timestamp) {
                pendingChanges.remove(entry.getKey());
                Tuple<Long, V> old = changes.get(key);
                if (old == null || old.key < time) {
                    changes.put(key, new Tuple<>(time, entry.getValue()));
                }
            }
        }
        // Figure out old values from the persistent state, and also update it.
        List<Tuple<I, V>> toAdd = new ArrayList<>();
        List<Tuple<I, V>> toRemove = new ArrayList<>();
        for (Entry<I, Tuple<Long, V>> entry : changes.entrySet()) {
            I key = entry.getKey();
            V newValue = entry.getValue().value;
            V oldValue = persistentValues.get(key);
            if (oldValue == null || newValue.compareTo(oldValue) != 0) {
                if (oldValue != null) {
                    toRemove.add(new Tuple<>(key, oldValue));
                }
                if (newValue.compareTo(cutoff) <= 0) {
                    persistentValues.remove(key);
                } else {
                    persistentValues.put(key, newValue);
                    toAdd.add(new Tuple<>(key, newValue));
                }
            }
        }
        if (!toAdd.isEmpty() || !toRemove.isEmpty()) {
            K key = ctx.getCurrentKey();
            OrderStatisticTree<Tuple<I, V>> ranking = getRanking(key);
            int lastSize = ranking.size();
            // Sort from smallest to largest. Note that the `Tuple` comparator
            // sorts largest values first, for ranking indices to be equal to
            // the output row numbers.
            toAdd.sort((a, b) -> b.compareTo(a));
            toRemove.sort((a, b) -> b.compareTo(a));
            // We walk through these one by one. This way we can emit the minimal
            // number of changes necessary. Lower values do not affect the higher
            // ones in terms of ranking.
            int lastRow = 0, lastOffset = 0;
            while (!toAdd.isEmpty() || !toRemove.isEmpty()) {
                // Get the largest values we want to change first.
                int row, offset;
                if (toRemove.isEmpty() || (!toAdd.isEmpty()
                        && toAdd.get(toAdd.size() - 1).compareTo(toRemove.get(toRemove.size() - 1)) < 0)) {
                    Tuple<I, V> entry = toAdd.remove(toAdd.size() - 1);
                    ranking.add(entry);
                    row = ranking.indexOf(entry);
                    int rank = -(ranking.indexOf(new Tuple<>(null, entry.value)) + 1);
                    out.collect(resultFunction.apply(key, entry.key, entry.value, row, rank, timestamp));
                    offset = lastOffset + 1;
                } else {
                    Tuple<I, V> entry = toRemove.remove(toRemove.size() - 1);
                    row = ranking.indexOf(entry);
                    ranking.remove(entry);
                    offset = lastOffset - 1;
                }
                if (lastOffset != 0 && lastRow != row) {
                    Iterator<Tuple<I, V>> it = ranking.indexIterator(lastRow);
                    for (int rown = lastRow; rown < row; rown++) {
                        assert it.hasNext();
                        Tuple<I, V> moved = it.next();
                        int rank = -(ranking.indexOf(new Tuple<>(null, moved.value)) + 1);
                        out.collect(resultFunction.apply(key, moved.key, moved.value, rown, rank, timestamp));
                    }
                }
                lastRow = row + (offset > lastOffset ? 1 : 0);
                lastOffset = offset;
            }
            // Clear all row numbers that are no longer used. Rows can become
            // free when a element is removed due to the cutoff.
            for (int i = ranking.size(); i < lastSize; i++) {
                out.collect(resultFunction.apply(key, null, null, i, i, timestamp));
            }
        }
        lastFlush.update(timestamp);
    }
}
