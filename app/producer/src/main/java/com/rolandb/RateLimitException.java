package com.rolandb;

import java.io.IOException;
import java.time.Instant;

/**
 * A special kind of {@link IOException} that indicates that a rate limit has
 * been hit with the underlying API. In case this exception is thrown, no more
 * requests should be made before the retry after instant.
 */
public class RateLimitException extends IOException {
    /** The timestamp after which we can try again to access the API. */
    private final Instant retryAfter;

    /**
     * Create a new rate limit exception.
     *
     * @param retryAfter
     *            The instant after which the API can be queries again.
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
