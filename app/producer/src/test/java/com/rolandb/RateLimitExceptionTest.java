package com.rolandb;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RateLimitExceptionTest {

    @Test
    void testConstructor() {
        Instant retryAfter = Instant.now().plusSeconds(60);
        RateLimitException exception = new RateLimitException(retryAfter);
        assertEquals(retryAfter, exception.getRetryAfter());
    }

    @Test
    void testInheritance() {
        Instant retryAfter = Instant.now().plusSeconds(120);
        RateLimitException exception = new RateLimitException(retryAfter);
        assertTrue(exception instanceof Exception);
        assertTrue(exception instanceof IOException);
    }
}
