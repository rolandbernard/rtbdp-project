package com.rolandb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StaticFileHandlerTest {
    StaticFileHandler fileHandler;

    @BeforeEach
    public void setUp() {
        fileHandler = new StaticFileHandler("src/test/resources");
    }

    @Test
    public void testGetFileExtension() throws Exception {
        Method method = StaticFileHandler.class.getDeclaredMethod("getFileExtension", String.class);
        method.setAccessible(true);
        assertEquals("html", method.invoke(null, "index.html"));
        assertEquals("css", method.invoke(null, "style.css"));
        assertEquals("js", method.invoke(null, "script.js"));
        assertEquals("png", method.invoke(null, "image.png"));
        assertEquals("jpg", method.invoke(null, "image.jpg"));
        assertEquals("ico", method.invoke(null, "favicon.ico"));
        assertEquals("svg", method.invoke(null, "logo.svg"));
        assertEquals("", method.invoke(null, "file"));
        assertEquals("", method.invoke(null, "file."));
        assertEquals("txt", method.invoke(null, "file.txt"));
        assertEquals("txt", method.invoke(null, "/path/to/file.txt"));
        assertEquals("", method.invoke(null, "/path/to/file"));
    }

    @Test
    public void testGenerateEtag() throws Exception {
        Method method = StaticFileHandler.class.getDeclaredMethod("generateEtag", byte[].class);
        method.setAccessible(true);
        String content = "Hello, World!";
        byte[] contentBytes = content.getBytes();
        String expectedEtag = "\"ZajifYh5KDgxtmS9i38K1A==\"";
        assertEquals(expectedEtag, method.invoke(null, (Object) contentBytes));
    }

    @Test
    public void testGetIndexFile() throws Exception {
        MockHttpExchange exchange = new MockHttpExchange("http://localhost/", null);
        fileHandler.handle(exchange);
        assertEquals(200, exchange.getResponseCode());
        assertEquals("text/html", exchange.getResponseHeaders().getFirst("Content-Type"));
        assertEquals("13", exchange.getResponseHeaders().getFirst("Content-Length"));
        assertEquals("Hello Index!\n", new String(exchange.getResponseBody().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testGetExplicitFile() throws Exception {
        MockHttpExchange exchange = new MockHttpExchange("http://localhost/index.html", null);
        fileHandler.handle(exchange);
        assertEquals(200, exchange.getResponseCode());
        assertEquals("text/html", exchange.getResponseHeaders().getFirst("Content-Type"));
        assertEquals("13", exchange.getResponseHeaders().getFirst("Content-Length"));
        assertEquals("Hello Index!\n", new String(exchange.getResponseBody().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testGetJavaScriptFile() throws Exception {
        MockHttpExchange exchange = new MockHttpExchange("http://localhost/test.js", null);
        fileHandler.handle(exchange);
        assertEquals(200, exchange.getResponseCode());
        assertEquals("application/javascript", exchange.getResponseHeaders().getFirst("Content-Type"));
        assertEquals("14", exchange.getResponseHeaders().getFirst("Content-Length"));
        assertEquals("\"Hello Test!\"\n", new String(exchange.getResponseBody().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testNotAbleToEscape() throws Exception {
        MockHttpExchange exchange = new MockHttpExchange("http://localhost/../../test.txt", null);
        fileHandler.handle(exchange);
        assertEquals(200, exchange.getResponseCode());
        assertEquals("application/octet-stream", exchange.getResponseHeaders().getFirst("Content-Type"));
        assertEquals("13", exchange.getResponseHeaders().getFirst("Content-Length"));
        assertEquals("Hello World!\n", new String(exchange.getResponseBody().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testMissingFile() throws Exception {
        MockHttpExchange exchange = new MockHttpExchange("http://localhost/../../../pom.xml", null);
        fileHandler.handle(exchange);
        assertEquals(404, exchange.getResponseCode());
    }
}
