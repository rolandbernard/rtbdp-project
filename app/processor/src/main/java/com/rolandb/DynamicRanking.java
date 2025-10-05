package com.rolandb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * This is a processing function that will keep a ranking of all values based on
 * a key and value extracted from the incoming stream. For every incoming event
 * an outgoing event is created, that include information about how the given
 * events key has moved in the ranking. Note that for scalability reasons not
 * the complete ranking will be output every time, and a consumer of the
 * outgoing stream will have to reconstruct the full ranking themselves.
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

        @Override
        public int compareTo(Tuple<I, V> other) {
            int valueComp = value.compareTo(other.value);
            if (valueComp != 0) {
                return -valueComp;
            } else {
                return key.compareTo(other.key);
            }
        }
    }

    private static class KeylessTuple<I extends Comparable<I>, V extends Comparable<V>>
            implements Serializable, Comparable<Tuple<I, V>> {
        public final boolean min;
        public final V value;

        private KeylessTuple(V value, boolean min) {
            this.value = value;
            this.min = min;
        }

        public static <I extends Comparable<I>, V extends Comparable<V>> KeylessTuple<I, V> minFor(V value) {
            return new KeylessTuple<>(value, true);
        }

        public static <I extends Comparable<I>, V extends Comparable<V>> KeylessTuple<I, V> maxFor(V value) {
            return new KeylessTuple<>(value, false);
        }

        @Override
        public int compareTo(Tuple<I, V> other) {
            int valueComp = value.compareTo(other.value);
            if (valueComp != 0) {
                return -valueComp;
            } else if (min) {
                // We are smaller than all tuples with the same value.
                return -1;
            } else {
                // We are smaller larger all tuples with the same value.
                return 1;
            }
        }
    }

    public static interface KeyFunction<I, E> extends Serializable {
        public abstract I apply(E event);
    }

    public static interface ResultFunction<E, K, R, I, V> extends Serializable {
        public abstract R apply(
                E event, K key, I i, V v, Integer rowNumber, Integer minRank, Integer maxRank,
                Integer oldRowNumber, Integer oldMinRank, Integer oldMaxRank);
    }

    private final V cutoff;
    private final KeyFunction<I, E> keyFunction;
    private final KeyFunction<V, E> valueFunction;
    private final ResultFunction<E, K, R, I, V> resultFunction;
    // These are needed for the persistent state.
    private final Class<I> keyClass;
    private final Class<V> valueClass;

    // The last value for each key.
    private transient MapState<I, V> persistentValues;

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
     * @param resultFunction
     *            The function that generates the output events.
     * @param keyClass
     *            The class of the keys.
     * @param valueClass
     *            The class of the values.
     */
    public DynamicRanking(
            V cutoff, KeyFunction<I, E> keyFunction, KeyFunction<V, E> valueFunction,
            ResultFunction<E, K, R, I, V> resultFunction, Class<I> keyClass, Class<V> valueClass) {
        this.cutoff = cutoff;
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
        this.resultFunction = resultFunction;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        MapStateDescriptor<I, V> persistentDesc = new MapStateDescriptor<>("persistentValues", keyClass, valueClass);
        persistentValues = getRuntimeContext().getMapState(persistentDesc);
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
        K globalKey = ctx.getCurrentKey();
        I key = keyFunction.apply(event);
        V value = valueFunction.apply(event);
        // We only consider those events for which the key is not `null`. A value
        // of `null` is to indicate removal (like if it was below the cutoff).
        if (key != null) {
            OrderStatisticTree<Tuple<I, V>> ranking = getRanking(globalKey);
            V oldValue = persistentValues.get(key);
            if (value == null || (cutoff != null && value.compareTo(cutoff) <= 0)) {
                if (oldValue != null) {
                    // Remove the key from the ranking.
                    int oldRow = ranking.indexOf(new Tuple<>(key, oldValue));
                    int oldMinRank = -(ranking.indexOf(KeylessTuple.minFor(oldValue)) + 1);
                    int oldMaxRank = -(ranking.indexOf(KeylessTuple.maxFor(oldValue)) + 1);
                    assert oldRow >= 0 && oldMinRank >= 0 && oldMaxRank >= 0;
                    assert oldRow >= oldMinRank && oldMaxRank > oldRow;
                    ranking.remove(new Tuple<>(key, oldValue));
                    out.collect(resultFunction.apply(
                            event, globalKey, key, value, null, null, null, oldRow, oldMinRank, oldMaxRank));
                    persistentValues.remove(key);
                }
            } else {
                if (oldValue == null) {
                    // Add the key to the ranking.
                    ranking.add(new Tuple<>(key, value));
                    int newRow = ranking.indexOf(new Tuple<>(key, value));
                    int newMinRank = -(ranking.indexOf(KeylessTuple.minFor(value)) + 1);
                    int newMaxRank = -(ranking.indexOf(KeylessTuple.maxFor(value)) + 1);
                    assert newRow >= 0 && newMinRank >= 0 && newMaxRank >= 0;
                    assert newRow >= newMinRank && newMaxRank > newRow;
                    out.collect(resultFunction.apply(
                            event, globalKey, key, value, newRow, newMinRank, newMaxRank, null, null, null));
                } else {
                    // Update the value of the key in the ranking.
                    int oldRow = ranking.indexOf(new Tuple<>(key, oldValue));
                    int oldMinRank = -(ranking.indexOf(KeylessTuple.minFor(oldValue)) + 1);
                    int oldMaxRank = -(ranking.indexOf(KeylessTuple.maxFor(oldValue)) + 1);
                    assert oldRow >= 0 && oldMinRank >= 0 && oldMaxRank >= 0;
                    assert oldRow >= oldMinRank && oldMaxRank > oldRow;
                    ranking.remove(new Tuple<>(key, oldValue));
                    ranking.add(new Tuple<>(key, value));
                    int newRow = ranking.indexOf(new Tuple<>(key, value));
                    int newMinRank = -(ranking.indexOf(KeylessTuple.minFor(value)) + 1);
                    int newMaxRank = -(ranking.indexOf(KeylessTuple.maxFor(value)) + 1);
                    assert newRow >= 0 && newMinRank >= 0 && newMaxRank >= 0;
                    assert newRow >= newMinRank && newMaxRank > newRow;
                    out.collect(resultFunction.apply(
                            event, globalKey, key, value, newRow, newMinRank, newMaxRank,
                            oldRow, oldMinRank, oldMaxRank));
                }
                persistentValues.put(key, value);
            }
        }
    }
}
