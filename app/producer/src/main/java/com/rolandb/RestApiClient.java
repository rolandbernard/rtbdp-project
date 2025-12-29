package com.rolandb;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This client can be used to get the events from the GitHub Events API through
 * HTTP requests to the REST API.
 */
public class RestApiClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestApiClient.class);

    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final HttpClient[] httpClients;
    private final String[] accessToken;
    private final Instant[] retryAfter;
    private final String[] lastEtags;
    private int lastToken = 0;

    /**
     * Create a new REST API client instance.
     *
     * @param baseUrl
     *            The base URL to connect to. (https://api.github.com for
     *            the official GitHub API)
     * @param accessToken
     *            The access token to use.
     */
    public RestApiClient(String baseUrl, String accessToken) {
        this.baseUrl = baseUrl;
        this.accessToken = accessToken.split(",");
        httpClients = new HttpClient[this.accessToken.length];
        for (int i = 0; i < httpClients.length; i++) {
            httpClients[i] = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
        }
        retryAfter = new Instant[this.accessToken.length];
        objectMapper = new ObjectMapper();
        lastEtags = new String[3];
    }

    /**
     * Fetch from the REST API the events at the given page with the given number
     * of events per page.
     *
     * @param page
     *            The page number to fetch (must be between 1 and 3 inclusive).
     * @param perPage
     *            The number of events per page (maximum 100).
     * @return The list of receiver events
     * @throws IOException
     *             In case there is any other I/O issue.
     * @throws RateLimitException
     *             In case the rate limit of the API is exceeded.
     * @throws InterruptedException
     *             If the operation is interrupted.
     */
    public List<GithubEvent> getEvents(int page, int perPage) throws IOException, InterruptedException {
        String url = baseUrl + "/events?page=" + page + "&per_page=" + perPage;
        LOGGER.info("Fetching events from url {}", url);
        int index = (lastToken++) % accessToken.length;
        while (retryAfter[index] != null && retryAfter[index].isAfter(Instant.now())) {
            if (Arrays.stream(retryAfter).allMatch(e -> e != null && e.isAfter(Instant.now()))) {
                throw new RateLimitException(retryAfter[0]);
            }
            index = (lastToken++) % accessToken.length;
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/vnd.github+json")
                .header("Accept-Encoding", "gzip, deflate")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header("Authorization", "token " + accessToken[index])
                .header("User-Agent", "curl/7.68.0")
                .timeout(Duration.ofSeconds(10))
                .GET();
        if (lastEtags[page - 1] != null) {
            builder.header("If-None-Match", lastEtags[page - 1]);
        }
        HttpResponse<InputStream> response = httpClients[index].send(builder.build(),
                HttpResponse.BodyHandlers.ofInputStream());
        int status = response.statusCode();
        HttpHeaders headers = response.headers();
        if (status == 304) {
            LOGGER.info("Finished fetching from {}. Not Modified", url);
            return List.of();
        } else if (status == 200) {
            String rateLimit = headers.firstValue("x-ratelimit-remaining").orElse("unknown");
            try {
                int limit = Integer.parseInt(rateLimit);
                if (limit == 0) {
                    LOGGER.warn("The rate limit has been reached");
                    for (String val : headers.allValues("x-ratelimit-reset")) {
                        retryAfter[index] = Instant.EPOCH.plusSeconds(Long.parseLong(val));
                    }
                } else if (limit < 25) {
                    LOGGER.warn("Close to reaching the rate limit {}", rateLimit);
                }
            } catch (NumberFormatException e) {
                // Ignore. This is only as a warning.
            }
            for (String newEtag : headers.allValues("etag")) {
                lastEtags[page - 1] = newEtag;
            }
            String encoding = headers.firstValue("content-encoding").orElse("");
            InputStream responseStream = response.body();
            if (encoding.equals("gzip")) {
                LOGGER.info("Received gzip compressed response");
                responseStream = new GZIPInputStream(responseStream);
            } else if (encoding.equals("deflate")) {
                LOGGER.info("Received deflate compressed response");
                responseStream = new InflaterInputStream(responseStream);
            }
            List<GithubEvent> result = objectMapper.readValue(responseStream, new TypeReference<List<GithubEvent>>() {
            });
            LOGGER.info("Finished fetching from {}. Rate limit: {}", url, rateLimit);
            return result;
        } else {
            LOGGER.warn("Failed to fetch events. status code {}", status);
            if (status == 403 || status == 429) {
                // If this is an issue of hitting the rate limit, try to figure
                // out when it will be safe again to call the API.
                if (headers.allValues("x-ratelimit-remaining").stream().anyMatch(e -> e.equals("0"))) {
                    for (String val : headers.allValues("x-ratelimit-reset")) {
                        try {
                            Instant retry = Instant.EPOCH.plusSeconds(Long.parseLong(val));
                            retryAfter[index] = retry;
                            throw new RateLimitException(retry);
                        } catch (NumberFormatException ex) {
                            // Simply ignore the header
                        }
                    }
                }
                for (String val : headers.allValues("retry-after")) {
                    try {
                        Instant retry = Instant.now().plusSeconds(Integer.parseInt(val));
                        retryAfter[index] = retry;
                        throw new RateLimitException(retry);
                    } catch (NumberFormatException ex) {
                        // Simply ignore the header
                    }
                }
                if (status == 429) {
                    // We are given no information about when to resume. Simply
                    // wait for one minute before restarting.
                    throw new RateLimitException(Instant.now().plusSeconds(60));
                }
            }
            throw new IOException("Failed to fetch events. status code " + response.statusCode());
        }
    }
}
