package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthHandlerTest {
    MockHandler mockHandler;
    AuthHandler authHandler;

    static class MockHandler implements HttpHandler {
        boolean called = false;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            called = true;
        }
    }

    @BeforeEach
    public void setUp() {
        mockHandler = new MockHandler();
        authHandler = new AuthHandler("test", mockHandler);
    }

    @Test
    public void testConstructorWithPassword() {
        assertNotNull(authHandler.getSecret());
        assertTrue(authHandler.getSecret().length() > 0);
    }

    @Test
    public void testConstructorWithoutPassword() {
        authHandler = new AuthHandler("", mockHandler);
        assertNull(authHandler.getSecret());
        authHandler = new AuthHandler("   ", mockHandler);
        assertNull(authHandler.getSecret());
        authHandler = new AuthHandler(null, mockHandler);
        assertNull(authHandler.getSecret());
    }

    @Test
    public void testGetLoginPageTemplate() throws Exception {
        java.lang.reflect.Method method = AuthHandler.class.getDeclaredMethod("getLoginPageTemplate");
        method.setAccessible(true);
        String template = (String) method.invoke(authHandler);
        assertNotNull(template);
        assertTrue(template.contains("{{message}}"));
    }

    @Test
    public void testHandleMissingCookieNoPassword() throws Exception {
        authHandler = new AuthHandler(null, mockHandler);
        HttpExchange exchange = new MockHttpExchange("http://localhost/", null);
        authHandler.handle(exchange);
        assertEquals(0, exchange.getResponseCode());
        assertTrue(mockHandler.called);
    }

    @Test
    public void testHandleMissingCookie() throws Exception {
        HttpExchange exchange = new MockHttpExchange("http://localhost/", null);
        authHandler.handle(exchange);
        assertEquals(303, exchange.getResponseCode());
        assertEquals("/login?url=%2F", exchange.getResponseHeaders().getFirst("Location"));
    }

    @Test
    public void testHandleWithCookie() throws Exception {
        HttpExchange exchange = new MockHttpExchange("http://localhost/", null);
        exchange.getRequestHeaders().set("Cookie", "rtgh_secret=" + authHandler.getSecret());
        authHandler.handle(exchange);
        assertEquals(0, exchange.getResponseCode());
        assertTrue(mockHandler.called);
    }

    @Test
    public void testHandleLogin() throws Exception {
        HttpExchange exchange = new MockHttpExchange("http://localhost/login", "secret=test");
        authHandler.handle(exchange);
        assertEquals(303, exchange.getResponseCode());
        assertEquals("/", exchange.getResponseHeaders().getFirst("Location"));
        assertTrue(exchange.getResponseHeaders().getFirst("Set-Cookie").contains("rtgh_secret="));
    }

    @Test
    public void testHandleLoginWrongPassword() throws Exception {
        MockHttpExchange exchange = new MockHttpExchange("http://localhost/login", "secret=test1");
        authHandler.handle(exchange);
        assertEquals(401, exchange.getResponseCode());
        assertTrue(new String(exchange.getResponseBody().toByteArray(), StandardCharsets.UTF_8)
                .contains("wrong password"));
    }
}
