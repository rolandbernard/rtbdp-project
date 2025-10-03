package com.rolandb;

import java.time.Duration;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.BeforeEach;

public class DynamicRankingTest {
    static class Event {
        String id;
        Integer value;

        public Event(String id, Integer value) {
            this.id = id;
            this.value = value;
        }
    }

    static class Result {
        Integer key;
        String id;
        Integer value;
        int rowNumber;
        int rank;
        long timestamp;

        public Result(Integer key, String id, Integer value, int rowNumber, int rank, long timestamp) {
            this.key = key;
            this.id = id;
            this.value = value;
            this.rowNumber = rowNumber;
            this.rank = rank;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Results(" + this.key + ", " + this.id + ", " + this.value + ", " + this.rowNumber + "," + this.rank
                    + "," + this.timestamp + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Result other = (Result) obj;
            return key.equals(other.key) && (id == null ? other.id == null : id.equals(other.id))
                    && (value == null ? other.value == null : value.equals(other.value)) && rowNumber == other.rowNumber
                    && rank == other.rank && timestamp == other.timestamp;
        }
    }

    private DynamicRanking<Integer, Event, Result, String, Integer> operator;
    private KeyedOneInputStreamOperatorTestHarness<Integer, Event, Result> harness;

    @BeforeEach
    public void setup() throws Exception {
        operator = new DynamicRanking<>(
                100, Duration.ofMillis(100), event -> event.id, event -> event.value,
                (key, i, v, row_number, rank, ts) -> new Result(key, i, v, row_number, rank, ts),
                String.class, Integer.class);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                operator, event -> 1, TypeInformation.of(Integer.class));
        harness.open();
    }

    @Test
    void testBasicProcessAndFlush() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idA", 150), 110));
        harness.processWatermark(1000);
        List<Result> output = harness.extractOutputValues();
        Assertions.assertEquals(1, output.size());
        Assertions.assertEquals(new Result(1, "idA", 150, 0, 0, 200), output.get(0));
    }

    @Test
    void testCutoffRemoval() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idB", 50), 10));
        List<Result> output = harness.extractOutputValues();
        Assertions.assertEquals(0, output.size());
    }

    @Test
    void testRankingUpdate() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idA", 150), 10));
        harness.processWatermark(110);
        List<Result> output1 = harness.extractOutputValues();
        Assertions.assertEquals(1, output1.size());
        Assertions.assertEquals(new Result(1, "idA", 150, 0, 0, 100), output1.get(0));
        harness.processElement(new StreamRecord<>(new Event("idB", 200), 150));
        harness.processWatermark(210);
        List<Result> output2 = harness.extractOutputValues();
        Assertions.assertEquals(3, output2.size());
        Assertions.assertTrue(output2.contains(new Result(1, "idB", 200, 0, 0, 200)));
        Assertions.assertTrue(output2.contains(new Result(1, "idA", 150, 1, 1, 200)));
    }

    @Test
    void testValueUpdateAndReRanking() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idA", 150), 50));
        harness.processWatermark(100);
        List<Result> output1 = harness.extractOutputValues();
        Assertions.assertEquals(1, output1.size());
        Assertions.assertEquals(new Result(1, "idA", 150, 0, 0, 100), output1.get(0));
        harness.processElement(new StreamRecord<>(new Event("idB", 200), 110));
        harness.processElement(new StreamRecord<>(new Event("idA", 250), 150));
        harness.processWatermark(200);
        List<Result> output2 = harness.extractOutputValues();
        Assertions.assertEquals(3, output2.size());
        Assertions.assertTrue(output2.contains(new Result(1, "idA", 250, 0, 0, 200L)));
        Assertions.assertTrue(output2.contains(new Result(1, "idB", 200, 1, 1, 200L)));
    }
}
