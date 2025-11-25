package com.rolandb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

public class MockHttpExchange extends HttpExchange {
    private URI requestUri;
    private ByteArrayInputStream requestBody;
    private Headers requestHeaders = new Headers();
    private Headers responseHeaders = new Headers();
    private ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
    private int responseStatus;

    public MockHttpExchange(String url, String body) throws URISyntaxException {
        this.requestUri = new URI(url);
        this.requestBody = body != null ? new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)) : null;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public Object getAttribute(String name) {
        throw new UnsupportedOperationException("Unimplemented method 'getAttribute'");
    }

    @Override
    public HttpContext getHttpContext() {
        throw new UnsupportedOperationException("Unimplemented method 'getHttpContext'");
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        throw new UnsupportedOperationException("Unimplemented method 'getLocalAddress'");
    }

    @Override
    public HttpPrincipal getPrincipal() {
        throw new UnsupportedOperationException("Unimplemented method 'getPrincipal'");
    }

    @Override
    public String getProtocol() {
        throw new UnsupportedOperationException("Unimplemented method 'getProtocol'");
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return new InetSocketAddress(0);
    }

    @Override
    public InputStream getRequestBody() {
        return requestBody;
    }

    @Override
    public Headers getRequestHeaders() {
        return requestHeaders;
    }

    @Override
    public String getRequestMethod() {
        return requestBody == null ? "GET" : "POST";
    }

    @Override
    public URI getRequestURI() {
        return requestUri;
    }

    @Override
    public ByteArrayOutputStream getResponseBody() {
        return responseBody;
    }

    @Override
    public int getResponseCode() {
        return responseStatus;
    }

    @Override
    public Headers getResponseHeaders() {
        return responseHeaders;
    }

    @Override
    public void sendResponseHeaders(int rCode, long responseLength) throws IOException {
        responseStatus = rCode;
        getResponseHeaders().set("Content-Length", Long.toString(responseLength));
    }

    @Override
    public void setAttribute(String name, Object value) {
        throw new UnsupportedOperationException("Unimplemented method 'setAttribute'");
    }

    @Override
    public void setStreams(InputStream i, OutputStream o) {
        throw new UnsupportedOperationException("Unimplemented method 'setStreams'");
    }
}
