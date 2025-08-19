package com.rolandb;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This client can be used to get the events from the GitHub Events API through
 * HTTP requests to the REST API.
 */
public class RestApiClient {
    /**
     * A special kind of `IOException` that indicates that a rate limit has been
     * hit with the underlying API. In case this exception is thrown, no more
     * requests should be made before the retry after instant.
     */
    public class RateLimitException extends IOException {
        private final Instant retryAfter;

        /**
         * Create a new rate limit exception.
         *
         * @param retryAfter
         *                   The instant after which the API can be queries again.
         */
        public RateLimitException(Instant retryAfter) {
            this.retryAfter = retryAfter;
        }

        /**
         * Get the instant after which the API should be tried again.
         *
         * @return The retry after instant.
         */
        public Instant getRetryAfter() {
            return retryAfter;
        }
    }

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final String accessToken;

    /**
     * Create a new REST API client instance.
     *
     * @param baseUrl
     *                    The base URL to connect to. (https://api.github.com for
     *                    the official GitHub API)
     * @param accessToken
     *                    The access token to use.
     */
    public RestApiClient(String baseUrl, String accessToken) {
        this.baseUrl = baseUrl;
        this.accessToken = accessToken;
        httpClient = HttpClient.newHttpClient();
        objectMapper = new ObjectMapper();
    }

    /**
     * Fetch from the REST API the events at the given page with the given number
     * of events per page.
     *
     * @param page
     *                The page number to fetch (starting at 1).
     * @param perPage
     *                The number of events per page (maximum 100).
     * @return The list of receiver events
     * @throws IOException
     *                              In case there is any other I/O issue.
     * @throws RateLimitException
     *                              In case the rate limit of the API is exceeded.
     * @throws InterruptedException
     *                              If the operation is interrupted.
     */
    public List<GithubEvent> getEvents(int page, int perPage) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/events"))
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header("Authorization", "token " + accessToken)
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status == 200) {
            return objectMapper.readValue(response.body(), new TypeReference<List<GithubEvent>>() {
            });
        } else {
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
            throw new IOException("Failed to fetch events. status code " + response.statusCode());
        }
    }
}
