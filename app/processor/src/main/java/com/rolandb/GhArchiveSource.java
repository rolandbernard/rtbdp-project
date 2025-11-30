package com.rolandb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A bounded Flink data stream source that reads GitHub events from the
 * GhArchive service.
 */
public class GhArchiveSource
        implements Source<JsonNode, GhArchiveSource.GhArchiveFile, Collection<GhArchiveSource.GhArchiveFile>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GhArchiveSource.class);

    /**
     * Class representing a single file from GhArchive with the URL where it can be
     * downloaded from.
     */
    public static class GhArchiveFile implements SourceSplit, Serializable {
        /** The URL at which the file can be downloaded. */
        private final String url;

        private GhArchiveFile(String url) {
            this.url = url;
        }

        @Override
        public String splitId() {
            return url;
        }
    }

    /**
     * The enumerator generates all of the individual file URLs from the base URL,
     * the start date, and the end date.
     */
    public static class GhArchiveEnumerator implements SplitEnumerator<GhArchiveFile, Collection<GhArchiveFile>> {
        private final SplitEnumeratorContext<GhArchiveFile> context;
        private final Queue<GhArchiveFile> remainingSplits;

        private GhArchiveEnumerator(
                SplitEnumeratorContext<GhArchiveFile> context, String url, String startDate, String endDate) {
            this.context = context;
            this.remainingSplits = new ArrayDeque<>();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-H");
            LocalDateTime current = LocalDateTime.parse(startDate, formatter);
            LocalDateTime end = LocalDateTime.parse(endDate, formatter);
            while (!current.isAfter(end)) {
                remainingSplits.add(new GhArchiveFile(url + "/" + current.format(formatter) + ".json.gz"));
                current = current.plusHours(1);
            }
        }

        private GhArchiveEnumerator(
                SplitEnumeratorContext<GhArchiveFile> context, Collection<GhArchiveFile> splits) {
            this.context = context;
            this.remainingSplits = new ArrayDeque<>(splits);
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            if (!remainingSplits.isEmpty()) {
                context.assignSplit(remainingSplits.poll(), subtaskId);
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void start() {
        }

        @Override
        public void addReader(int subtaskId) {
        }

        @Override
        public void addSplitsBack(List<GhArchiveFile> splits, int subtaskId) {
            remainingSplits.addAll(splits);
        }

        @Override
        public Collection<GhArchiveFile> snapshotState(long checkpointId) {
            return remainingSplits;
        }

        @Override
        public void close() {
        }
    }

    /**
     * The reader is given the files, and actually downloads and parses them into
     * {@link JsonNode} objects.
     */
    public static class GhArchiveReader implements SourceReader<JsonNode, GhArchiveFile> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Queue<GhArchiveFile> assignedSplits = new ArrayDeque<>();
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();
        private final SourceReaderContext context;
        private boolean noMoreSplits = false;

        private GhArchiveReader(SourceReaderContext context) {
            this.context = context;
        }

        private byte[] basicLoadGhArchiveFile(String url) throws IOException, InterruptedException {
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

        private void loadGhArchiveFile(String url, Consumer<String> consumer) throws InterruptedException {
            int reties = 0;
            while (true) {
                try {
                    byte[] bytes = basicLoadGhArchiveFile(url);
                    LOGGER.info("Starting reading events returned from {}", url);
                    try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
                            GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                            InputStreamReader reader = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
                            BufferedReader bufferedReader = new BufferedReader(reader)) {
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            consumer.accept(line);
                        }
                    }
                    return;
                } catch (RateLimitException e) {
                    LOGGER.warn("Hitting a rate limit in the download", e);
                    Thread.sleep(Duration.between(Instant.now(), e.getRetryAfter()).toMillis());
                } catch (IOException e) {
                    LOGGER.error("Failed to deserialize events data", e);
                    if (reties > 10) {
                        throw new IllegalStateException(e);
                    }
                    Thread.sleep(10_000);
                    reties += 1;
                }
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<JsonNode> output) throws InterruptedException {
            if (!assignedSplits.isEmpty()) {
                GhArchiveFile split = assignedSplits.poll();
                loadGhArchiveFile(split.url, s -> {
                    try {
                        output.collect(mapper.readTree(s));
                    } catch (JsonProcessingException e) {
                        LOGGER.error("Failed to parse some event", e);
                        throw new IllegalStateException(e);
                    }
                });
                LOGGER.info("Finished processing one split {} left", assignedSplits.size());
                return InputStatus.MORE_AVAILABLE;
            } else {
                if (noMoreSplits) {
                    LOGGER.info("Finished with the reader.");
                    return InputStatus.END_OF_INPUT;
                } else {
                    context.sendSplitRequest();
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }
        }

        @Override
        public void addSplits(List<GhArchiveFile> splits) {
            LOGGER.info("{} splits were added.", splits.size());
            assignedSplits.addAll(splits);
            if (!assignedSplits.isEmpty()) {
                futures.forEach(e -> e.complete(null));
                futures.clear();
            }
        }

        @Override
        public void notifyNoMoreSplits() {
            this.noMoreSplits = true;
            futures.forEach(e -> e.complete(null));
            futures.clear();
        }

        @Override
        public List<GhArchiveFile> snapshotState(long checkpointId) {
            return new ArrayList<>(assignedSplits);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            if (noMoreSplits || !assignedSplits.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            } else {
                CompletableFuture<Void> future = new CompletableFuture<>();
                futures.add(future);
                return future;
            }
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    /** The base URL for GhArchive. */
    private final String url;
    /** The first date that we want to load. */
    private final String startDate;
    /** The last date that we want to load. */
    private final String endDate;

    /**
     * Create a new instance of this source class.
     * 
     * @param url
     *            The base GhArchive URL to use.
     * @param startDate
     *            The start date to get.
     * @param endDate
     *            The end data to get.
     */
    public GhArchiveSource(String url, String startDate, String endDate) {
        this.url = url;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @Override
    public SourceReader<JsonNode, GhArchiveFile> createReader(SourceReaderContext readerContext) throws Exception {
        return new GhArchiveReader(readerContext);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<GhArchiveFile, Collection<GhArchiveFile>> createEnumerator(
            SplitEnumeratorContext<GhArchiveFile> enumContext) throws Exception {
        return new GhArchiveEnumerator(enumContext, url, startDate, endDate);
    }

    @Override
    public SplitEnumerator<GhArchiveFile, Collection<GhArchiveFile>> restoreEnumerator(
            SplitEnumeratorContext<GhArchiveFile> enumContext, Collection<GhArchiveFile> checkpoint) throws Exception {
        return new GhArchiveEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<GhArchiveFile> getSplitSerializer() {
        return new SimpleVersionedSerializer<GhArchiveSource.GhArchiveFile>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(GhArchiveFile obj) throws IOException {
                try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        DataOutputStream out = new DataOutputStream(bytes)) {
                    out.writeUTF(obj.url);
                    return bytes.toByteArray();
                }
            }

            @Override
            public GhArchiveFile deserialize(int version, byte[] serialized) throws IOException {
                try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
                        DataInputStream in = new DataInputStream(bytes)) {
                    return new GhArchiveFile(in.readUTF());
                }
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Collection<GhArchiveFile>> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<Collection<GhArchiveFile>>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Collection<GhArchiveFile> obj) throws IOException {
                try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        DataOutputStream out = new DataOutputStream(bytes)) {
                    out.writeInt(obj.size());
                    for (GhArchiveFile file : obj) {
                        out.writeUTF(file.url);
                    }
                    return bytes.toByteArray();
                }
            }

            @Override
            public Collection<GhArchiveFile> deserialize(int version, byte[] serialized) throws IOException {
                try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
                        DataInputStream in = new DataInputStream(bytes)) {
                    List<GhArchiveFile> files = new ArrayList<>();
                    int size = in.readInt();
                    for (int i = 0; i < size; i++) {
                        files.add(new GhArchiveFile(in.readUTF()));
                    }
                    return files;
                }
            }
        };
    }
}
