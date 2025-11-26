package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.rolandb.tables.CountsLiveTable.WindowSize;
import com.rolandb.tables.TrendingLiveTable.RepoTrendingScore;
import com.rolandb.tables.StarsLiveTable.RepoStarCounts;

import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TrendingScoreFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<Long, RepoStarCounts, RepoTrendingScore> harness;
    private TrendingScoreFunction trendingScoreFunction;

    @BeforeEach
    public void setUp() throws Exception {
        trendingScoreFunction = new TrendingScoreFunction();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new StreamMap<>(trendingScoreFunction),
                (KeySelector<RepoStarCounts, Long>) value -> value.repoId,
                TypeInformation.of(Long.class));
        harness.open();
    }

    @Test
    public void testMapFunction() throws Exception {
        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.MINUTES_5, 0L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 0), harness.extractOutputValues().get(0));
        harness.getOutput().clear();

        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.MINUTES_5, 10L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 10 * 10), harness.extractOutputValues().get(0));
        harness.getOutput().clear();

        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.HOURS_1, 5L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 125), harness.extractOutputValues().get(0));
        harness.getOutput().clear();

        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.HOURS_6, 2L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 129), harness.extractOutputValues().get(0));
        harness.getOutput().clear();

        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.HOURS_24, 1L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 130), harness.extractOutputValues().get(0));
        harness.getOutput().clear();

        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.MINUTES_5, 0L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 30), harness.extractOutputValues().get(0));
        harness.getOutput().clear();

        harness.processElement(new StreamRecord<>(new RepoStarCounts(1L, WindowSize.HOURS_1, 0L)));
        assertEquals(1, harness.extractOutputValues().size());
        assertEquals(new RepoTrendingScore(1L, 5), harness.extractOutputValues().get(0));
        harness.getOutput().clear();
    }
}
