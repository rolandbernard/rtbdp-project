package com.rolandb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
 * This is a keyed process function that collects all the events coming to the
 * same key into batches based on a maximum size and timeout per-batch.
 * 
 * @param <K>
 *            The key of the keyed stream.
 * @param <E>
 *            The type of event we want to batch.
 */
public class CountAndTimeBatcher<K, E extends SequencedRow> extends KeyedProcessFunction<K, E, List<E>> {
    /** The maximum delay before an event is flushed. */
    private final long batchMs;
    /** The maximum number of events before we flush them. */
    private final long batchSize;
    /**
     * Event class needed to create the state descriptor for saving the temporarily
     * buffered events.
     */
    private final Class<E> eventClass;

    /** Currently buffered events. */
    private transient MapState<List<Object>, E> buffer;
    /** Buffer size of the current buffer, to detect when to flush. */
    private transient ValueState<Long> bufferSize;
    /** Currently set timer timestamp. For deleting it in case of early flush. */
    private transient ValueState<Long> currentTimer;

    /**
     * Create a new instance of the batcher.
     * 
     * @param batchSize
     *            The batch size to use when batching the events.
     * @param batchDuration
     *            The maximum delay in processing time before flushing an event.
     * @param eventClass
     *            The class of events we want to store.
     */
    public CountAndTimeBatcher(long batchSize, Duration batchDuration, Class<E> eventClass) {
        this.batchMs = batchDuration.toMillis();
        this.batchSize = batchSize;
        this.eventClass = eventClass;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Long> currentTimerDesc = new ValueStateDescriptor<>("currentTimer", Long.class);
        ValueStateDescriptor<Long> bufferSizeDesc = new ValueStateDescriptor<>("bufferSize", Long.class);
        MapStateDescriptor<List<Object>, E> bufferDesc = new MapStateDescriptor<>("buffer",
                TypeInformation.of(new TypeHint<List<Object>>() {
                }),
                TypeInformation.of(eventClass));
        currentTimer = getRuntimeContext().getState(currentTimerDesc);
        bufferSize = getRuntimeContext().getState(bufferSizeDesc);
        buffer = getRuntimeContext().getMapState(bufferDesc);
    }

    private void flushBuffer(Context ctx, Collector<List<E>> out) throws Exception {
        List<E> batch = new ArrayList<>();
        for (E event : buffer.values()) {
            batch.add(event);
        }
        out.collect(batch);
        buffer.clear();
        bufferSize.clear();
        Long timer = currentTimer.value();
        if (timer != null) {
            ctx.timerService().deleteProcessingTimeTimer(timer);
            currentTimer.clear();
        }
    }

    @Override
    public void processElement(E event, Context ctx, Collector<List<E>> out) throws Exception {
        List<Object> key = event.getKey();
        E existing = buffer.get(key);
        if (existing == null) {
            buffer.put(key, event);
            Long size = bufferSize.value();
            if (size == null) {
                size = 0L;
            }
            bufferSize.update(size + 1);
            if (size + 1 >= batchSize) {
                flushBuffer(ctx, out);
            } else if (currentTimer.value() == null) {
                long timer = ctx.timerService().currentProcessingTime() + batchMs;
                ctx.timerService().registerProcessingTimeTimer(timer);
                currentTimer.update(timer);
            }
        } else {
            existing.mergeWith(event);
            buffer.put(key, existing);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<E>> out) throws Exception {
        currentTimer.clear();
        flushBuffer(ctx, out);
    }
}
