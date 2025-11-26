package com.rolandb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Class encapsulating the dummy data for a specific point in time. The
 * constructor takes care of loading the correct data for each timestamp.
 */
public class DummyData {
    private static final Logger LOGGER = LoggerFactory.getLogger(DummyData.class);
    private static final String RESOURCE_NAME = "dummy.json.gz";

    public static class Event {
        public final Instant timestamp;
        public final String json;

        public Event(Instant timestamp, String json) {
            this.timestamp = timestamp;
            this.json = json;
        }
    }

    private static class HourlyData {
        public final Instant last;
        public final List<Event> events;

        public HourlyData(Instant last, List<Event> events) {
            this.last = last;
            this.events = events;
        }

        private static byte[] readDataFromGhArchive(String url) throws IOException, InterruptedException {
            LOGGER.info("Fetching events from url {}", url);
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            int status = response.statusCode();
            if (status == 200) {
                return response.body();
            } else {
                LOGGER.warn("Failed to fetch events. status code {}", status);
                if (status == 403 || status == 429) {
                    // If this is an issue of hitting the rate limit, try to figure
                    // out when it will be safe again to call the API.
                    HttpHeaders headers = response.headers();
                    if (headers.map().containsKey("x-ratelimit-remaining") &&
                            headers.map().containsKey("x-ratelimit-reset")
                            && headers.map().get("x-ratelimit-remaining").stream().anyMatch(e -> e.equals("0"))) {
                        for (String val : headers.map().get("x-ratelimit-reset")) {
                            try {
                                long epoch = Long.parseLong(val);
                                throw new RateLimitException(Instant.EPOCH.plusSeconds(epoch));
                            } catch (NumberFormatException ex) {
                                // Simply ignore the header
                            }
                        }
                    }
                    if (headers.map().containsKey("retry-after")) {
                        for (String val : headers.map().get("retry-after")) {
                            try {
                                int toWait = Integer.parseInt(val);
                                throw new RateLimitException(Instant.now().plusSeconds(toWait));
                            } catch (NumberFormatException ex) {
                                // Simply ignore the header
                            }
                        }
                    }
                    if (status == 429) {
                        // We are given no information about when to resume. Simply
                        // wait for one minute before restarting.
                        throw new RateLimitException(Instant.now().plusSeconds(60));
                    }
                }
                if (status == 404) {
                    // The data is likely not yet available. Handle this like a
                    // rate limiting and retry every two minutes.
                    throw new RateLimitException(Instant.now().plusSeconds(120));
                }
                throw new IOException("Failed to fetch events. status code " + response.statusCode());
            }
        }

        private static byte[] readDataFromResource() throws IOException {
            LOGGER.info("Loading data from dummy resources file");
            URL url = DummyData.class.getResource(RESOURCE_NAME);
            if (url == null) {
                throw new IOException("Resource not found");
            } else {
                try (InputStream stream = url.openStream()) {
                    return stream.readAllBytes();
                }
            }
        }

        private static byte[] readDataForFile(String filename, String archiveUrl, String dataDir)
                throws InterruptedException {
            // Check if it maybe cached already.
            Path cachePath = Path.of(dataDir == null ? "./" : dataDir, filename);
            if (dataDir != null && Files.exists(cachePath)) {
                try {
                    return Files.readAllBytes(cachePath);
                } catch (IOException e) {
                    LOGGER.warn("Failed to load cached bytes", e);
                    // We can just continue as if we had a cache miss.
                }
            }
            if (archiveUrl != null) {
                try {
                    byte[] bytes = null;
                    do {
                        try {
                            bytes = readDataFromGhArchive(archiveUrl + "/" + filename);
                        } catch (RateLimitException ex) {
                            // In case we are hitting issues due to rate limiting,
                            // don't give up but retry after the delay.
                            Thread.sleep(ex.getRetryAfter().toEpochMilli() - Instant.now().toEpochMilli());
                            continue;
                        }
                    } while (false);
                    if (dataDir != null) {
                        try {
                            Files.createDirectories(cachePath.getParent());
                            Files.write(cachePath, bytes);
                        } catch (IOException e) {
                            LOGGER.warn("Failed to write cache.", e);
                            // Caching is optional. Simply continue.
                        }
                    }
                    return bytes;
                } catch (IOException e) {
                    LOGGER.error("Failed to read data from GH Archive", e);
                    // Fallback to loading the dummy data from the resources.
                }
            }
            try {
                return readDataFromResource();
            } catch (IOException e) {
                LOGGER.error("Failed to read dummy data from resources", e);
                throw new IllegalStateException(e);
            }
        }

        /**
         * Modify the timestamp of the given event such that ti is moved forward or
         * backwards in time to lie within one hour before the given target timestamp.
         * This is for when we use dummy data, to still have correct timestamps.
         *
         * @param event
         *            The original event to modify.
         * @param target
         *            The target timestamp to which the events should be adjusted to.
         * @return The creation time that is not in the event. Either the modified one
         *         or the original if it was already in the correct timestamp.
         */
        private static Instant modifyTimestamp(JsonNode event, Instant target) {
            long perHour = Duration.ofHours(1).toMillis();
            Instant ts = Instant.parse(event.at("/created_at").asText());
            if (!ts.isBefore(target)) {
                long diff = (ts.toEpochMilli() - target.toEpochMilli() + perHour) / perHour;
                ts = ts.minus(Duration.ofHours(diff));
            } else if (ts.isBefore(target.minus(Duration.ofHours(1)))) {
                long diff = (target.toEpochMilli() - ts.toEpochMilli() - 1) / perHour;
                ts = ts.plus(Duration.ofHours(diff));
            }
            ((ObjectNode) event).put("created_at", ts.toString());
            return ts;
        }

        public static HourlyData loadFrom(Instant ts, String archiveUrl, String dataDir)
                throws InterruptedException {
            Instant first = ts.truncatedTo(ChronoUnit.HOURS);
            Instant last = first.plus(Duration.ofHours(1));
            ZonedDateTime utcDateTime = first.atZone(ZoneOffset.UTC);
            String filename = utcDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + "-"
                    + utcDateTime.getHour()
                    + ".json.gz";
            LOGGER.info("Loading data for timestamp " + first + "(" + filename + ")");
            byte[] bytes = readDataForFile(filename, archiveUrl, dataDir);
            ObjectMapper objectMapper = new ObjectMapper();
            List<Event> events = new ArrayList<>();
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
                    GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                    InputStreamReader reader = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
                    BufferedReader bufferedReader = new BufferedReader(reader)) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    JsonNode node = objectMapper.readTree(line);
                    Instant timestamp = modifyTimestamp(node, last);
                    events.add(new Event(timestamp, line));
                }
            } catch (IOException e) {
                LOGGER.error("Failed to deserialize events data", e);
                throw new IllegalStateException(e);
            }
            events.sort((a, b) -> a.timestamp.compareTo(b.timestamp));
            return new HourlyData(last, events);
        }

        public List<Event> getLastAfter(Instant time, int n) {
            int low = 0, high = events.size();
            while (low < high) {
                int m = (low + high) / 2;
                Event event = events.get(m);
                if (event.timestamp.isAfter(time)) {
                    high = m;
                } else {
                    low = m + 1;
                }
            }
            low -= n;
            if (low < 0) {
                low = 0;
            }
            LOGGER.debug("Events for " + time + " from " + low + " to " + high);
            return events.subList(low, high);
        }
    }

    private final float speedUp;
    private final Duration delay;
    private final String archiveUrl;
    private final String dataDir;
    private final Instant start;
    private final Deque<HourlyData> cached;
    private Thread backgroundWorker;

    /**
     * Crate a new instance of the dummy data. The dummy data will loop starting
     * from the time this instance is created.
     *
     * @throws IOException
     *             In case the dummy resources can not be accessed.
     */
    public DummyData(float speedUp, Duration delay, String archiveUrl, String dataDir) throws IOException {
        this.speedUp = speedUp;
        this.delay = delay;
        this.archiveUrl = archiveUrl;
        this.dataDir = dataDir;
        start = Instant.now();
        cached = new ArrayDeque<>();
        LOGGER.info("Loaded dummy data timestamps");
    }

    public void loadCurrentData() throws InterruptedException {
        Instant now = currentTimestamp();
        Instant prev = now.minus(Duration.ofHours(1));
        Instant next = now.plus(Duration.ofHours(1));
        boolean loadNow, loadPrev, loadNext;
        synchronized (cached) {
            loadPrev = cached.isEmpty() || !cached.getLast().last.isAfter(prev);
            loadNow = cached.isEmpty() || !cached.getLast().last.isAfter(now);
            loadNext = cached.isEmpty() || !cached.getLast().last.isAfter(next);
        }
        HourlyData dataNow = null, dataPrev = null, dataNext = null;
        if (loadPrev) {
            dataPrev = HourlyData.loadFrom(prev, archiveUrl, dataDir);
        }
        if (loadNow) {
            dataNow = HourlyData.loadFrom(now, archiveUrl, dataDir);
        }
        if (loadNext) {
            dataNext = HourlyData.loadFrom(next, archiveUrl, dataDir);
        }
        synchronized (cached) {
            if (loadPrev) {
                cached.addLast(dataPrev);
            }
            if (loadNow) {
                cached.addLast(dataNow);
            }
            if (loadNext) {
                cached.addLast(dataNext);
            }
            while (cached.getFirst().last.isBefore(prev.minus(Duration.ofHours(1)))) {
                cached.removeFirst();
            }
        }
        Thread.sleep(5_000);
    }

    private void runBackgroundWorker() {
        while (!Thread.interrupted()) {
            try {
                loadCurrentData();
            } catch (InterruptedException e) {
                // We have been interrupted. We should just terminate.
                break;
            }
        }
    }

    /**
     * Start the background worker. The worker will periodically check the current
     * time, and download the previous and last events from gh archive. If a data
     * directory has been specified, the data will be cached not only in-memory,
     * but also in that specified directory.
     */
    public void startWorker() {
        backgroundWorker = new Thread(this::runBackgroundWorker);
        backgroundWorker.start();
        LOGGER.info("Started the data collection worker");
    }

    /**
     * Stop the background worker started by a call to
     * {@link DummyData#startWorker}.
     * 
     * @throws InterruptedException
     */
    public void stopWorker() throws InterruptedException {
        backgroundWorker.interrupt();
        backgroundWorker.join();
        backgroundWorker = null;
        LOGGER.info("Stopped the data collection worker");
    }

    /**
     * Get the current event in paginated form. Return only a single page, with
     * the number of events determined by {@code perPage}.
     *
     * @param page
     *            The page number to fetch. (Starting at 1)
     * @param perPage
     *            The number of elements on each page.
     * @return A subset of current events based on {@code page} and {@code perPage}.
     */
    public List<Event> getEvents(int page, int perPage) {
        return getEvents(currentTimestamp(), page, perPage);
    }

    /**
     * Get the current paginated list of events, from the given timestamp. The
     * events list at a timestamp represents the events created before that
     * timestamp in order from newest to oldest.
     * 
     * @param timestamp
     *            The timestamp for which to return the events.
     * @param page
     *            The page to fetch. (Starting at 1)
     * @param perPage
     *            The number of element on each page.
     * @return A subset of the events.
     */
    public List<Event> getEvents(Instant timestamp, int page, int perPage) {
        LOGGER.info("Loading events for page {} ({} per page) at {}", page, perPage, timestamp);
        int len = page * perPage;
        List<Event> allEvents = new ArrayList<>();
        synchronized (cached) {
            Iterator<HourlyData> it = cached.descendingIterator();
            while (it.hasNext()) {
                HourlyData d = it.next();
                List<Event> events = d.getLastAfter(timestamp, len - allEvents.size());
                for (int i = events.size() - 1; i >= 0; i--) {
                    allEvents.add(events.get(i));
                }
                if (allEvents.size() == len) {
                    break;
                }
            }
        }
        return allEvents.subList(
                Integer.min((page - 1) * perPage, allEvents.size()), Integer.min(len, allEvents.size()));
    }

    /**
     * The current timestamp to used for reading the dummy resources. This
     * timestamp will start {@code delay} behind the current time, and progress
     * with a factor {@code speedUp} faster than real-time.
     *
     * @return The current timestamp.
     */
    private Instant currentTimestamp() {
        long sinceStart = (long) (ChronoUnit.MILLIS.between(start, Instant.now()) * speedUp);
        return start.plusMillis(sinceStart).minus(delay);
    }
}
