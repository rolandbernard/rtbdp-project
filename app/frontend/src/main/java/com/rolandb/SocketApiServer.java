package com.rolandb;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.sql.Connection;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * An implementation of a {@link WebSocketServer} for providing a live data
 * stream to the frontend web application.
 */
public class SocketApiServer extends WebSocketServer {
    /**
     * The type of message that the client may send to the server to subscribe
     * to some number of tables, or to remove some previous subscriptions.
     */
    public static class ClientMessage {
        @JsonProperty("replay")
        public final List<Subscription> replay;
        @JsonProperty("subscribe")
        public final List<Subscription> subscribe;
        @JsonProperty("unsubscribe")
        public final List<Long> unsubscribe;

        public ClientMessage(List<Subscription> replay, List<Subscription> subscribe, List<Long> unsubscribe) {
            this.replay = replay;
            this.subscribe = subscribe;
            this.unsubscribe = unsubscribe;
        }
    }

    /**
     * This is the state that we keep for each client. We basically only keep
     * a list of the current subscriptions, and disposables for closing the
     * observables for a given table.
     */
    private static class ClientState {
        /**
         * Subscriptions by table. Required for filtering requests and handling
         * unsubscribing from tables.
         */
        private final Map<Table, Set<Subscription>> subscriptions = new HashMap<>();
        /**
         * Subscriptions by id. Required for handling unsubscribing.
         */
        private final Map<Long, Subscription> subscriptionsById = new HashMap<>();
        /**
         * Map from table to disposable. Note that we only ever keep one
         * subscription to each table observable. This means even if the client
         * create more than one subscription, every row will be processed out
         * only once.
         */
        private final Map<Table, Disposable> disposables = new HashMap<>();

        public void subscribe(Subscription newSubscription, Consumer<Map<String, ?>> consumer) {
            // todo
        }

        public void unsubscribe(Subscription newSubscription) {
            // todo
        }

        public void unsubscribeAll() {
            // todo
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketApiServer.class);

    private final String bootstrapServer;
    private final String groupId;
    private final String jdbcUrl;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Table> tables = new HashMap<>();
    private final Scheduler ioScheduler = Schedulers.from(Executors.newFixedThreadPool(8));

    /**
     * Initialize a new socket server to listen on the given address.
     * 
     * @param address
     *            The address to listen on.
     */
    public SocketApiServer(InetSocketAddress address, String bootstrapServer, String groupId, String jdbcUrl) {
        super(address);
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public void onOpen(WebSocket socket, ClientHandshake handshake) {
        LOGGER.info("Client connected {}", socket.getRemoteSocketAddress());
        socket.setAttachment(new ClientState());
    }

    @Override
    public void onClose(WebSocket socket, int code, String reason, boolean remote) {
        LOGGER.info("Client disconnected {}", socket.getRemoteSocketAddress());
        ClientState state = socket.getAttachment();
        state.unsubscribeAll();
    }

    @Override
    public void onError(WebSocket socket, Exception error) {
        if (socket == null) {
            LOGGER.error("WebSocket server error", error);
        } else {
            LOGGER.info("Client error {}: {}", socket.getRemoteSocketAddress(), error.getMessage());
        }
    }

    /**
     * Send the given row, from the given table to the given WebSocket.
     * 
     * @param socket
     *            The socket to send to.
     * @param table
     *            The table to send a row for.
     * @param row
     *            The new row values.
     */
    private void sendRow(WebSocket socket, String table, Map<String, ?> row) {

    }

    @Override
    public void onMessage(WebSocket socket, String messageString) {
        try {
            LOGGER.debug("Message from client {}: {}", socket.getRemoteSocketAddress(), messageString);
            ClientMessage message = objectMapper.readValue(messageString, ClientMessage.class);
            for (Subscription subscription : message.subscribe) {

            }
            for (Long unsubscribe : message.unsubscribe) {

            }
            for (Subscription replay : message.replay) {

            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to handle request", e);
        }
    }

    @Override
    public void onStart() {
        LOGGER.info("WebSocket server started at {}", this.getAddress());
    }
}
