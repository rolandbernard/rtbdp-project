package com.rolandb;

import java.time.Duration;
import java.util.Objects;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import com.rolandb.AbstractUpdateTable.UpdateSeqRow;

/**
 * This is a filter function that filters out update events that are redundant
 * because they contain the same or less information than the combination of the
 * preceding update events. Many users appear often and for these we always see
 * the same information, meaning this can cut down significantly on the number
 * of events emitted to the sink.
 * 
 * @param <T>
 *            The type of event we try to deduplicate.
 */
public class UpdateDeduplicate<T extends UpdateSeqRow> extends RichFilterFunction<T> {
    // Stores the last values that have already been stored. This is used to prevent
    // duplicate events that don't add any new information.
    private transient ValueState<Object[]> lastState;

    /**
     * Instantiate a default instance of the filter function.
     */
    public UpdateDeduplicate() {
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        ValueStateDescriptor<Object[]> lastStateDesc = new ValueStateDescriptor<>("lastState", Object[].class);
        // We don't want to deduplicate globally, since that would lead to keeping
        // unbounded memory. Therefore we will just keep the values for ten minutes.
        Duration ttl = Duration.ofMinutes(10);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        lastStateDesc.enableTimeToLive(ttlConfig);
        lastState = getRuntimeContext().getState(lastStateDesc);
    }

    @Override
    public boolean filter(T value) throws Exception {
        Object[] merged = lastState.value();
        Object[] newValues = value.getUpdateValues();
        if (merged == null) {
            lastState.update(newValues);
            return true;
        } else {
            boolean changed = false;
            for (int i = 0; i < merged.length; i += 2) {
                if (merged[i] == null || (newValues[i] != null && (Long) newValues[i] > (Long) merged[i])) {
                    if (!Objects.equals(merged[i + 1], newValues[i + 1])) {
                        changed = true;
                        break;
                    }
                }
            }
            if (changed) {
                for (int i = 0; i < merged.length; i += 2) {
                    if (merged[i] == null || (newValues[i] != null && (Long) newValues[i] > (Long) merged[i])) {
                        merged[i] = newValues[i];
                        merged[i + 1] = newValues[i + 1];
                    }
                }
                lastState.update(merged);
                return true;
            } else {
                return false;
            }
        }
    }
}
