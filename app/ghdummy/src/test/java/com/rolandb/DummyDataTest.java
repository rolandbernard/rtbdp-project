package com.rolandb;

import org.junit.jupiter.api.Test;

import com.rolandb.DummyData.Event;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DummyDataTest {

    @Test
    void testGetEventsBeforeLoad() throws IOException, InterruptedException {
        DummyData dummyData = new DummyData(1.0f, Duration.ofMinutes(5), null, null);
        assertNotNull(dummyData.getEvents(1, 10));
        assertEquals(0, dummyData.getEvents(1, 10).size());
    }

    @Test
    void testGetEvents() throws IOException, InterruptedException {
        DummyData dummyData = new DummyData(1.0f, Duration.ofMinutes(5), null, null);
        dummyData.loadCurrentData();
        assertNotNull(dummyData.getEvents(1, 10));
        assertEquals(10, dummyData.getEvents(1, 10).size());
    }

    @Test
    void testGetEventsMultiplePages() throws IOException, InterruptedException {
        DummyData dummyData = new DummyData(1.0f, Duration.ofMinutes(1), null, null);
        dummyData.loadCurrentData();
        List<Event> first = dummyData.getEvents(1, 20);
        List<Event> second = dummyData.getEvents(2, 20);
        assertEquals(20, first.size());
        assertEquals(20, second.size());
        assertTrue(first.stream().allMatch(e -> !second.contains(e)));
    }

    @Test
    void testStartWorker() throws IOException, InterruptedException {
        DummyData dummyData = new DummyData(2.0f, Duration.ofSeconds(0), null, null);
        assertDoesNotThrow(() -> {
            dummyData.startWorker();
            Thread.sleep(10_000);
            dummyData.stopWorker();
        });
        assertEquals(10, dummyData.getEvents(1, 10).size());
    }
}
