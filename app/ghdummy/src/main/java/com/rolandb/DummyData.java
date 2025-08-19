package com.rolandb;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class encapsulating the dummy data for a specific point in time. The
 * constructor takes care of loading the correct data for each timestamp.
 */
public class DummyData {
    private static final Logger LOGGER = LoggerFactory.getLogger(DummyData.class);
    private static final String RESOURCE_NAME = "ghevents";

    private final Instant start;
    private final List<Long> timestamps;

    /**
     * Crate a new instance of the dummy data. The dummy data will loop starting
     * from the time this instance is created.
     * 
     * @throws IOException In case the dummy resources can not be accessed.
     */
    public DummyData() throws IOException {
        start = Instant.now();
        timestamps = readPossibleTimestamps();
        timestamps.sort(Long::compareTo);
        LOGGER.info("Loaded dummy data timestamps");
    }

    /**
     * Get the events for the current timestamp.
     * 
     * @return The currently present events.
     * @throws IOException In case the dummy resources can not be accessed.
     */
    public List<JsonNode> getEvents() throws IOException {
        return readTimestamp(currentTimestamp());
    }

    /**
     * Get the current event in paginated form. Return only a single page, with
     * the number of events determined by `perPage`.
     * 
     * @param page    The page number to fetch. (Starting at 1)
     * @param perPage The number of elements on each page.
     * @return A subset of current events based on `page` and `perPage`.
     * @throws IOException In case the dummy resources can not be accessed.
     */
    public List<JsonNode> getEvents(int page, int perPage) throws IOException {
        List<JsonNode> allEvents = getEvents();
        if (page < 0 || allEvents.size() < (page - 1) * perPage) {
            return List.of();
        } else {
            return allEvents.subList((page - 1) * perPage, Integer.min(allEvents.size(), page * perPage));
        }
    }

    /**
     * The current timestamp to used for reading the dummy resources. This
     * timestamp will loop around back to `0` whenever the dummy data is exhausted.
     * 
     * @return The current timestamp.
     */
    private long currentTimestamp() {
        long ideal = ChronoUnit.MILLIS.between(start, Instant.now());
        ideal %= timestamps.get(timestamps.size() - 1) + 5_000;
        int pos = Collections.binarySearch(timestamps, ideal);
        if (pos < 0) {
            pos = -pos - 2;
        }
        return timestamps.get(pos);
    }

    /**
     * Read the dummy resources for the given timestamp. Not that the timestamp
     * must actually exists in the resources, otherwise the method will throw an
     * exception.
     * 
     * @param timestamp The timestamp to load the dummy events for.
     * @return The list of events for the given timestamp.
     * @throws IOException In case the dummy resources could not be accessed.
     */
    private List<JsonNode> readTimestamp(long timestamp) throws IOException {
        LOGGER.info("Loading data for fake timestamp {}", timestamp);
        InputStream stream = DummyData.class.getResourceAsStream(RESOURCE_NAME + "/" + timestamp + ".json");
        if (stream == null) {
            throw new IOException("Resource not found");
        }
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(stream);
        assert root.isArray();
        List<JsonNode> events = new ArrayList<>();
        for (JsonNode event : root) {
            events.add(event);
        }
        return events;
    }

    /**
     * Read the set of all possible timestamp that can be loaded. The server only
     * includes a limited number of dummy events.
     * 
     * @return The list of possible timestamps.
     * @throws IOException In case the dummy resources can not be accessed.
     */
    private List<Long> readPossibleTimestamps() throws IOException {
        URL url = DummyData.class.getResource(RESOURCE_NAME);
        if (url == null) {
            throw new IOException("Resource not found");
        } else if (url.getProtocol().equals("file")) {
            // Resources are regular loose files (development environment)
            Path directory;
            try {
                directory = Paths.get(url.toURI());
            } catch (URISyntaxException ex) {
                throw new IllegalStateException("Malformed resource URL", ex);
            }
            return Files.list(directory)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(filename -> Long.valueOf(filename.substring(0, filename.lastIndexOf('.'))))
                    .collect(Collectors.toList());
        } else if (url.getProtocol().equals("jar")) {
            // Resources are inside a JAR file (production environment)
            JarURLConnection jarConnection = (JarURLConnection) url.openConnection();
            JarFile jarFile = jarConnection.getJarFile();
            Enumeration<JarEntry> entries = jarFile.entries();
            String normalizedPath = jarConnection.getEntryName();
            if (!normalizedPath.endsWith("/")) {
                normalizedPath += "/";
            }
            List<Long> result = new ArrayList<>();
            while (entries.hasMoreElements()) {
                String entryName = entries.nextElement().getName();
                if (entryName.startsWith(normalizedPath) && !entryName.equals(normalizedPath)) {
                    String filename = entryName.substring(normalizedPath.length());
                    if (!filename.contains("/")) {
                        result.add(Long.valueOf(filename.substring(0, filename.lastIndexOf('.'))));
                    }
                }
            }
            return result;
        } else {
            throw new IOException("Resource protocol not supported");
        }
    }
}
