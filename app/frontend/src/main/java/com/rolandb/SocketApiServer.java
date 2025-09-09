package com.rolandb;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.rolandb.Table.TableField;

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
        public final List<Subscription> replay;
        public final List<Subscription> subscribe;
        public final List<Long> unsubscribe;

        @JsonCreator
        public ClientMessage(
                @JsonProperty("replay") List<Subscription> replay,
                @JsonProperty("subscribe") List<Subscription> subscribe,
                @JsonProperty("unsubscribe") List<Long> unsubscribe) {
            this.replay = replay == null ? List.of() : replay;
            this.subscribe = subscribe == null ? List.of() : subscribe;
            this.unsubscribe = unsubscribe == null ? List.of() : unsubscribe;
        }
    }

    /**
     * This is the state that we keep for each client. We basically only keep
     * a list of the current subscriptions, and disposables for closing the
     * observables for a given table.
     */
    private class ClientState {
        /**
         * Subscriptions by table. Required for filtering requests and handling
         * unsubscribing from tables.
         */
        private final Map<String, Set<Subscription>> subscriptions = new HashMap<>();
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
        private final Map<String, Disposable> disposables = new HashMap<>();

        public synchronized void subscribe(
                Subscription newSubscription, Table table, Consumer<Map<String, ?>> consumer) {
            if (!subscriptionsById.containsKey(newSubscription.id)) {
                subscriptionsById.put(newSubscription.id, newSubscription);
                Set<Subscription> sameTable;
                if (subscriptions.containsKey(newSubscription.tableName)) {
                    sameTable = subscriptions.get(newSubscription.tableName);
                } else {
                    sameTable = new HashSet<>();
                    subscriptions.put(newSubscription.tableName, sameTable);
                    Disposable disposable = table.getLiveObservable()
                            .subscribeOn(rxScheduler)
                            .subscribe(row -> {
                                if (sameTable.stream().anyMatch(s -> s.accept(row))) {
                                    consumer.accept(row);
                                }
                            });
                    disposables.put(newSubscription.tableName, disposable);
                }
                sameTable.add(newSubscription);
            }
        }

        public synchronized void unsubscribe(long subscriptionId) {
            Subscription toRemove = subscriptionsById.remove(subscriptionId);
            if (toRemove != null) {
                Set<Subscription> sameTable = subscriptions.get(toRemove.tableName);
                sameTable.remove(toRemove);
                if (sameTable.isEmpty()) {
                    disposables.remove(toRemove.tableName).dispose();
                    subscriptions.remove(toRemove.tableName);
                }
            }
        }

        public synchronized void unsubscribeAll() {
            for (Disposable d : disposables.values()) {
                d.dispose();
            }
            subscriptions.clear();
            subscriptionsById.clear();
            disposables.clear();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketApiServer.class);

    private final Properties kafkaProperties;
    private final DbConnectionPool connections;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Table> tables = new HashMap<>();
    private final Scheduler rxScheduler = Schedulers.from(Executors.newFixedThreadPool(8));

    /**
     * Initialize a new socket server to listen on the given address.
     *
     * @param address
     *            The address to listen on.
     */
    public SocketApiServer(InetSocketAddress address, String bootstrapServer, String groupId, String jdbcUrl) {
        super(address);
        kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        connections = new DbConnectionPool(jdbcUrl);
        objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    }

    /**
     * Add the table to the set of available ones. Also start the live observable
     * for the table.
     *
     * @param table
     *            The table to add.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void addTable(Table table) {
        table.startLiveObservable(kafkaProperties);
        tables.put(table.name, table);
    }

    @Override
    public void start() {
        addTable(new Table("events", List.of()));
        addTable(new Table("counts_live", List.of(
                new TableField("kind", true, String.class),
                new TableField("window_size", true, String.class),
                new TableField("num_events", false, Long.class),
                new TableField("seq_num", false, Long.class))));
        addTable(new Table("counts_ranking", List.of(
                new TableField("kind", false, String.class),
                new TableField("window_size", true, String.class),
                new TableField("row_number", true, Long.class),
                new TableField("rank", false, Long.class),
                new TableField("seq_num", false, Long.class))));
        addTable(new Table("counts_history", List.of(
                new TableField("ts_start", true, String.class),
                new TableField("ts_end", true, String.class),
                new TableField("kind", true, String.class),
                new TableField("num_events", false, Long.class),
                new TableField("seq_num", false, Long.class))));
        setReuseAddr(true);
        super.start();
    }

    @Override
    public void stop() throws InterruptedException {
        super.stop();
        rxScheduler.shutdown();
        for (Table table : tables.values()) {
            table.stopLiveObservable();
        }
        tables.clear();
        try {
            connections.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close JDBC connections", e);
        }
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
        try {
            socket.send(objectMapper.writeValueAsString(Map.of("table", table, "row", row)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize row update", e);
        }
    }

    @Override
    public void onMessage(WebSocket socket, String messageString) {
        try {
            LOGGER.debug("Message from client {}: {}", socket.getRemoteSocketAddress(), messageString);
            ClientMessage message = objectMapper.readValue(messageString, ClientMessage.class);
            ClientState state = socket.getAttachment();
            for (Subscription subscription : message.subscribe) {
                Table table = tables.get(subscription.tableName);
                if (table != null && subscription.applicableTo(table)) {
                    state.subscribe(subscription, table, row -> {
                        sendRow(socket, table.name, row);
                    });
                }
            }
            for (Long unsubscribe : message.unsubscribe) {
                if (unsubscribe != null) {
                    state.unsubscribe(unsubscribe);
                }
            }
            for (Subscription replay : message.replay) {
                Table table = tables.get(replay.tableName);
                if (table != null && replay.applicableTo(table)) {
                    table.getReplayObservable(replay, connections)
                            .subscribeOn(rxScheduler)
                            .doOnError(error -> {
                                LOGGER.error("Error in table replay", error);
                            })
                            .subscribe(row -> {
                                sendRow(socket, table.name, row);
                            });
                }
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
