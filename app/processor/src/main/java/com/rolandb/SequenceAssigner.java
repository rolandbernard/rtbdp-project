package com.rolandb;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * This is a mapping function that assigns monotonically increasing sequence
 * numbers. The intended use case is to first key by some value and then it is
 * guaranteed that sequence numbers within that value are monotonically
 * increasing.
 * 
 * @param <E>
 *            The type of event to handle.
 */
public class SequenceAssigner<E extends SequencedRow> extends RichMapFunction<E, E> {
    /**
     * Last sequence number, used in case the event stream does not contain any.
     * It is to be considered undefined behavior if some events in an event stream
     * have sequence numbers, while others don't.
     */
    private transient ValueState<Long> sequenceNumber;

    /**
     * Create a new sequence assigner.
     */
    public SequenceAssigner() {
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Long> sequenceNumberDesc = new ValueStateDescriptor<>("sequenceNumber", Long.class);
        // We only really need to keep the last sequence numbers in case of very large
        // bursts. If there way nothing happening in the last five minutes, then we can
        // safely remove the state to avoid state leakage.
        Duration ttl = Duration.ofMinutes(5);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        sequenceNumberDesc.enableTimeToLive(ttlConfig);
        sequenceNumber = getRuntimeContext().getState(sequenceNumberDesc);
    }

    @Override
    public E map(E event) throws Exception {
        long timestamp = Instant.now().toEpochMilli() * 1000;
        Long lastSeq = sequenceNumber.value();
        if (lastSeq == null || lastSeq < timestamp) {
            // Use at least the current timestamp as the next sequence number.
            // This ensures that in case of a crash, the new events will
            // override the old events, avoiding issues where sequence numbers
            // are assigned in a different order for the retry.
            lastSeq = timestamp - 1;
        }
        if (event.seqNum == null) {
            // We fallback to the timestamp based sequence number only if there is no
            // inherent sequence number already defined for the event.
            lastSeq++;
            event.seqNum = lastSeq;
            sequenceNumber.update(lastSeq);
        }
        return event;
    }
}
