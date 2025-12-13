package com.rolandb;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * This is a filter function that filters out duplicate events with the same key
 * that arrive not too far apart from one another. Not that the stream must
 * already be keyed beforehand, and this operator does not care at all about the
 * contents of the events.
 * 
 * @param <T>
 *            The type of event we try to deduplicate.
 */
public class Deduplicate<T> extends RichFilterFunction<T> {
    /**
     * Stores a value if this key has been seen before, otherwise it will be empty.
     */
    private transient ValueState<Boolean> seen;

    /**
     * Instantiate a default instance of the filter function.
     */
    public Deduplicate() {
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Boolean> seenDesc = new ValueStateDescriptor<>("seen", Boolean.class);
        // We don't want to deduplicate globally, since that would lead to keeping
        // unbounded memory. Therefore we will just keep the values for thirty minutes.
        Duration ttl = Duration.ofMinutes(30);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        seenDesc.enableTimeToLive(ttlConfig);
        seen = getRuntimeContext().getState(seenDesc);
    }

    @Override
    public boolean filter(T value) throws Exception {
        Boolean exists = seen.value();
        if (exists == null) {
            seen.update(true);
            return true;
        } else {
            return false;
        }
    }
}
