package com.rolandb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

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

    static class Result implements Cloneable {
        Integer key;
        String id;
        Integer value;
        Integer row, rank, maxRank;
        Integer oldRow, oldRank, oldMaxRank;

        public Result(
                Integer key, String id, Integer value, Integer row, Integer rank, Integer maxRank,
                Integer oldRow, Integer oldRank, Integer oldMaxRank) {
            this.key = key;
            this.id = id;
            this.value = value;
            this.row = row;
            this.rank = rank;
            this.maxRank = maxRank;
            this.oldRow = oldRow;
            this.oldRank = oldRank;
            this.oldMaxRank = oldMaxRank;
        }

        @Override
        public String toString() {
            return "Results(" + this.key + ", " + this.id + ", " + this.value + ", " + this.oldRow + "->" + this.row
                    + ", " + this.oldRank + "->" + this.rank + ", " + this.oldMaxRank + "->" + this.maxRank
                    + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Result other = (Result) obj;
            return key.equals(other.key) && Objects.equals(id, other.id)
                    && Objects.equals(value, other.value) && Objects.equals(row, other.row)
                    && Objects.equals(rank, other.rank) && Objects.equals(maxRank, other.maxRank)
                    && Objects.equals(oldRow, other.oldRow) && Objects.equals(oldRank, other.oldRank)
                    && Objects.equals(oldMaxRank, other.oldMaxRank);
        }

        @Override
        public Result clone() {
            return new Result(key, id, value, row, rank, maxRank, oldRow, oldRank, oldMaxRank);
        }
    }

    private static void updateRanking(Result[] ranking, Result event) {
        if (event.oldRow != null) {
            // Remove from old row.
            for (int i = event.oldMaxRank; i < ranking.length; i++) {
                if (ranking[i] != null) {
                    ranking[i].rank -= 1;
                }
            }
            for (int i = event.oldRank; i < ranking.length; i++) {
                if (ranking[i] != null) {
                    ranking[i].maxRank -= 1;
                }
            }
            for (int i = event.oldRow + 1; i < ranking.length; i++) {
                if (ranking[i] != null) {
                    ranking[i].row -= 1;
                }
            }
            System.arraycopy(ranking, event.oldRow + 1, ranking, event.oldRow, ranking.length - event.oldRow - 1);
            ranking[ranking.length - 1] = null;
        }
        if (event.row != null) {
            // Insert at new row.
            System.arraycopy(ranking, event.row, ranking, event.row + 1, ranking.length - event.row - 1);
            ranking[event.row] = event.clone();
            for (int i = event.maxRank; i < ranking.length; i++) {
                if (ranking[i] != null) {
                    ranking[i].rank += 1;
                }
            }
            for (int i = event.rank; i < ranking.length; i++) {
                if (ranking[i] != null && i != event.row) {
                    ranking[i].maxRank += 1;
                }
            }
            for (int i = event.row + 1; i < ranking.length; i++) {
                if (ranking[i] != null) {
                    ranking[i].row += 1;
                }
            }
        }
    }

    private static Result[] reconstructRanking(List<Result> events, int maxN) {
        Result[] ranking = new Result[maxN];
        for (Result event : events) {
            updateRanking(ranking, event);
        }
        return ranking;
    }

    private static void sanityCheckRanking(Result[] ranking) {
        int len = 0;
        for (int i = 0; i < ranking.length; i++) {
            if (ranking[i] != null) {
                len = i + 1;
            }
        }
        Integer lastValue = Integer.MAX_VALUE;
        long lastRank = -1;
        for (int j = 0; j < len; j++) {
            assertNotNull(ranking[j]);
            assertEquals(j, (int) ranking[j].row);
            assertTrue(ranking[j].rank <= ranking[j].row);
            assertTrue(ranking[j].row < ranking[j].maxRank);
            if (lastValue == null) {
                assertNull(ranking[j].value);
            } else if (ranking[j].value != null) {
                assertTrue(lastValue >= ranking[j].value);
            }
            assertTrue(lastRank <= ranking[j].rank);
            if (lastRank == ranking[j].rank) {
                assertEquals(lastValue, ranking[j].value);
            }
            lastRank = ranking[j].rank;
            lastValue = ranking[j].value;
        }
    }

    private DynamicRanking<Integer, Event, Result, String, Integer> operator;
    private KeyedOneInputStreamOperatorTestHarness<Integer, Event, Result> harness;

    @BeforeEach
    public void setup() throws Exception {
        operator = new DynamicRanking<>(
                0, event -> event.id, event -> event.value,
                (e, k, i, v, row, rank, maxRank, oldRow, oldRank, oldMaxRank) -> new Result(k, i, v, row, rank, maxRank,
                        oldRow, oldRank, oldMaxRank),
                String.class, Integer.class);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                operator, event -> 1, TypeInformation.of(Integer.class));
        harness.open();
    }

    @Test
    void testBasicProcessAndFlush() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idA", 150), 110));
        List<Result> output = harness.extractOutputValues();
        Assertions.assertEquals(1, output.size());
        Assertions.assertEquals(new Result(1, "idA", 150, 0, 0, 1, null, null, null), output.get(0));
    }

    @Test
    void testCutoffRemoval() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idB", 0), 10));
        List<Result> output = harness.extractOutputValues();
        Assertions.assertEquals(0, output.size());
    }

    @Test
    void testRankingUpdate() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idA", 150), 10));
        List<Result> output1 = harness.extractOutputValues();
        Assertions.assertEquals(1, output1.size());
        Assertions.assertEquals(new Result(1, "idA", 150, 0, 0, 1, null, null, null), output1.get(0));
        harness.processElement(new StreamRecord<>(new Event("idB", 200), 150));
        List<Result> output2 = harness.extractOutputValues();
        Assertions.assertEquals(2, output2.size());
        Assertions.assertTrue(output2.contains(new Result(1, "idB", 200, 0, 0, 1, null, null, null)));
    }

    @Test
    void testValueUpdateAndReRanking() throws Exception {
        harness.processElement(new StreamRecord<>(new Event("idA", 150), 50));
        List<Result> output1 = harness.extractOutputValues();
        Assertions.assertEquals(1, output1.size());
        Assertions.assertEquals(new Result(1, "idA", 150, 0, 0, 1, null, null, null), output1.get(0));
        harness.processElement(new StreamRecord<>(new Event("idB", 200), 110));
        harness.processElement(new StreamRecord<>(new Event("idA", 250), 150));
        List<Result> output2 = harness.extractOutputValues();
        Assertions.assertEquals(3, output2.size());
        Assertions.assertTrue(output2.contains(new Result(1, "idB", 200, 0, 0, 1, null, null, null)));
        Assertions.assertTrue(output2.contains(new Result(1, "idA", 250, 0, 0, 1, 1, 1, 2)));
    }

    @Test
    void testLargerRanking() throws Exception {
        for (int i = 0; i < 10; i++) {
            harness.processElement(new StreamRecord<>(new Event("id" + i, 150 + i), 50));
        }
        List<Result> output = harness.extractOutputValues();
        Assertions.assertEquals(10, output.size());
        output.sort(Comparator.comparingInt(e -> -e.value));
        for (int i = 0; i < output.size(); i++) {
            assertEquals(0, (int) output.get(i).row);
            assertEquals(0, (int) output.get(i).rank);
            assertEquals(1, (int) output.get(i).maxRank);
        }
    }

    @Test
    void testLargerRankingUpdate() throws Exception {
        for (int i = 0; i < 10; i++) {
            harness.processElement(new StreamRecord<>(new Event("id" + i, 150 + i), 50));
        }
        Assertions.assertEquals(10, harness.extractOutputValues().size());
        for (int i = 9; i >= 0; i--) {
            harness.processElement(new StreamRecord<>(new Event("id" + i, 150 - i), 150));
        }
        Result[] output = reconstructRanking(harness.extractOutputValues(), 10);
        sanityCheckRanking(output);
        for (int i = 0; i < output.length; i++) {
            assertEquals(150 - i, (int) output[i].value);
        }
    }

    @Test
    void testRandomRankingUpdate0() throws Exception {
        Random rand = new Random(0);
        int lastN = 0;
        Result[] output = new Result[16];
        for (int i = 0; i <= 1_000; i++) {
            harness.processElement(new StreamRecord<>(new Event("id" + rand.nextInt(16), rand.nextInt(200)), i));
            List<Result> events = harness.extractOutputValues();
            events.stream().skip(lastN).forEach(event -> updateRanking(output, event));
            lastN = events.size();
            sanityCheckRanking(output);
        }
    }

    @Test
    void testRandomRankingUpdate() throws Exception {
        Random rand = new Random(0);
        for (int i = 0; i <= 100_000; i++) {
            harness.processElement(new StreamRecord<>(new Event("id" + rand.nextInt(16), rand.nextInt(200)), i));
            if (i % 100 == 0) {
                Result[] output = reconstructRanking(harness.extractOutputValues(), 16);
                sanityCheckRanking(output);
            }
        }
    }

    @Test
    void testRandomRankingUpdateCount() throws Exception {
        Random rand = new Random(0);
        int[] counts = new int[16];
        for (int i = 0; i <= 100_000; i++) {
            int id = rand.nextInt(16);
            harness.processElement(new StreamRecord<>(new Event("id" + id, counts[id]++), i));
            if (i % 100 == 0) {
                Result[] output = reconstructRanking(harness.extractOutputValues(), 16);
                sanityCheckRanking(output);
            }
        }
    }

    @Test
    void testRandomRankingUpdateCount2() throws Exception {
        Random rand = new Random(0);
        int[] counts = new int[16];
        for (int i = 0; i <= 1_000; i++) {
            int id = 31 - Integer.numberOfLeadingZeros(rand.nextInt(1 << 16));
            if (id >= 0) {
                harness.processElement(new StreamRecord<>(new Event("id" + id, counts[id]++), i * 50));
            }
            int id2 = 31 - Integer.numberOfLeadingZeros(rand.nextInt(1 << 16));
            if (id2 >= 0) {
                harness.processElement(new StreamRecord<>(new Event("id" + id2, counts[id2]--), i * 50));
            }
            if (i % 2 == 0) {
                Result[] output = reconstructRanking(harness.extractOutputValues(), 16);
                sanityCheckRanking(output);
            }
        }
    }
}
