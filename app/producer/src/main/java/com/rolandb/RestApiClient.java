package com.rolandb;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;

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

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final String[] accessToken;
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
        httpClient = HttpClient.newHttpClient();
        objectMapper = new ObjectMapper();
    }

    /**
     * Fetch from the REST API the events at the given page with the given number
     * of events per page.
     *
     * @param page
     *            The page number to fetch (starting at 1).
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
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header("Authorization", "token " + accessToken[lastToken])
                .GET()
                .build();
        lastToken = (lastToken + 1) % accessToken.length;
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status == 200) {
            return objectMapper.readValue(response.body(), new TypeReference<List<GithubEvent>>() {
            });
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
            throw new IOException("Failed to fetch events. status code " + response.statusCode());
        }
    }
}
