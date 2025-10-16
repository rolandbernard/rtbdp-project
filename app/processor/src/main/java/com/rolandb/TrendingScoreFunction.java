package com.rolandb;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;

import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.StarsLiveTable.RepoStarCounts;
import com.rolandb.tables.TrendingLiveTable.RepoTrendingScore;

public class TrendingScoreFunction extends RichMapFunction<RepoStarCounts, RepoTrendingScore> {
    // Stores the last counts for the different window sizes. If a specific window
    // size is not included it is assumed to be zero.
    private transient MapState<WindowSize, Long> lastCounts;

    @Override
    public void open(OpenContext parameters) throws Exception {
        MapStateDescriptor<WindowSize, Long> lastCountsDesc = new MapStateDescriptor<>(
                "lastCounts", WindowSize.class, Long.class);
        // As a failsafe, to avoid loosing some state and never cleaning it up, we just
        // use a TTL that is double the duration of the longest window.
        Duration ttl = Duration.ofMillis(
                Arrays.stream(WindowSize.values()).map(e -> e.sizeInMs()).max(Comparator.naturalOrder()).get() * 2);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        lastCountsDesc.enableTimeToLive(ttlConfig);
        lastCounts = getRuntimeContext().getMapState(lastCountsDesc);
    }

    private long getWindowValue(WindowSize size) throws Exception {
        Long value = lastCounts.get(size);
        if (value == null) {
            value = 0L;
        }
        return value;
    }

    private long getTrendingScore() throws Exception {
        return 10 * getWindowValue(WindowSize.MINUTES_5)
                + 5 * getWindowValue(WindowSize.HOURS_1)
                + 2 * getWindowValue(WindowSize.HOURS_6)
                + getWindowValue(WindowSize.HOURS_24);
    }

    @Override
    public RepoTrendingScore map(RepoStarCounts event) throws Exception {
        if (event.numStars == 0) {
            lastCounts.remove(event.windowSize);
        } else {
            lastCounts.put(event.windowSize, event.numStars);
        }
        return new RepoTrendingScore(event.repoId, getTrendingScore());
    }
}
