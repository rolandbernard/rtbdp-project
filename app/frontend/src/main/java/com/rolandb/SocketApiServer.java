package com.rolandb;

import java.net.InetSocketAddress;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a {@link WebSocketServer} for providing a live data
 * stream to the frontend web application.
 */
public class SocketApiServer extends WebSocketServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketApiServer.class);

    /**
     * Initialize a new socket server to listen on the given address.
     * 
     * @param address
     *            The address to listen on.
     */
    public SocketApiServer(InetSocketAddress address) {
        super(address);
    }

    @Override
    public void onOpen(WebSocket socket, ClientHandshake handshake) {
        LOGGER.info("Client connected {}", socket.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket socket, int code, String reason, boolean remote) {
        LOGGER.info("Client disconnected {}", socket.getRemoteSocketAddress());
    }

    @Override
    public void onError(WebSocket socket, Exception error) {
        if (socket == null) {
            LOGGER.error("WebSocket server error", error);
        } else {
            LOGGER.info("Client error {}: {}", socket.getRemoteSocketAddress(), error.getMessage());
        }
    }

    @Override
    public void onMessage(WebSocket socket, String message) {
        LOGGER.debug("Message from client {}: {}", socket.getRemoteSocketAddress(), message);
    }

    @Override
    public void onStart() {
        LOGGER.info("WebSocket server started at {}", this.getAddress());
    }
}
