package com.rolandb;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.java_websocket.WebSocket;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.rolandb.Table.Field;
import com.rolandb.Table.FieldKind;

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
        /** The list of replay requests. */
        public final List<Subscription> replay;
        /** The list of subscriptions to start. */
        public final List<Subscription> subscribe;
        /** The list of subscriptions to cancel. */
        public final List<Long> unsubscribe;

        /**
         * Create a new client message.
         * 
         * @param replay
         *            The list of replay requests.
         * @param subscribe
         *            The list of new subscriptions.
         * @param unsubscribe
         *            The list of subscription ids to cancel.
         */
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
        private final Map<String, Set<Subscription>> subscriptions;
        /**
         * Subscriptions by id. Required for handling unsubscribing.
         */
        private final Map<Long, Subscription> subscriptionsById;
        /**
         * Map from table to disposable. Note that we only ever keep one
         * subscription to each table observable. This means even if the client
         * create more than one subscription, every row will be processed out
         * only once.
         */
        private final Map<String, Disposable> disposables;

        /**
         * Create a new empty client state. Each client connection is assigned one such
         * state on initial connection.
         */
        public ClientState() {
            subscriptions = new HashMap<>();
            subscriptionsById = new HashMap<>();
            disposables = new HashMap<>();
        }

        /**
         * Subscribe the client with the given subscription to the given table, and send
         * update events to the given web socket.
         * 
         * @param newSubscription
         *            The subscription to activate.
         * @param table
         *            The table to subscribe to.
         * @param socket
         *            The socket to send events to.
         */
        public synchronized void subscribe(Subscription newSubscription, Table table, WebSocket socket) {
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
                            .filter(row -> {
                                synchronized (sameTable) {
                                    return sameTable.stream().anyMatch(s -> s.accept(row));
                                }
                            })
                            .buffer(50, TimeUnit.MILLISECONDS, 1024)
                            .subscribe(rows -> {
                                sendRows(socket, table.name, rows);
                            });
                    disposables.put(newSubscription.tableName, disposable);
                }
                synchronized (sameTable) {
                    sameTable.add(newSubscription);
                }
            }
        }

        /**
         * Cancel the subscription with the given id.
         * 
         * @param subscriptionId
         *            The id of the subscription that should be canceled.
         */
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

        /**
         * Unsubscribe from all active subscriptions. This is to be used when a client
         * disconnects from the server.
         */
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

    /** The secret we expect in the handshake cookies. */
    private final String secret;
    /** Properties for connecting to Kafka with. */
    private final Properties kafkaProperties;
    /** The database connection pool. */
    private final DbConnectionPool connections;
    /** Object mapper for (un)parsing the messages with the clients. */
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** The set of tables we have access to. */
    private final Map<String, Table> tables = new HashMap<>();
    /** Scheduler to subscribe to the observables on. */
    private final Scheduler rxScheduler = Schedulers.from(Executors.newFixedThreadPool(8));

    /** Extension for using compression with WebSockets. */
    private static final Draft perMessageDeflateDraft = new Draft_6455(new PerMessageDeflateExtension());

    /**
     * Initialize a new socket server to listen on the given address.
     *
     * @param address
     *            The address to listen on.
     * @param bootstrapServer
     *            The broker address for Kafka.
     * @param groupId
     *            The group id to use for Kafka consumer.
     * @param jdbcUrl
     *            The JDBC URL with with to connect to the database.
     * @param secret
     *            The secret that should be present in the cookies of the handshake
     *            to allow clients to connect.
     */
    public SocketApiServer(
            InetSocketAddress address, String bootstrapServer, String groupId, String jdbcUrl, String secret) {
        super(address, List.of(perMessageDeflateDraft));
        this.secret = secret;
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
     */
    private void addTable(Table table) {
        table.startLiveObservable(kafkaProperties);
        tables.put(table.name, table);
    }

    @Override
    public void start() {
        addTable(new Table("events", 1_000L, List.of(
                new Field("created_at", FieldKind.SORTED_KEY, String.class, 50L),
                new Field("id", FieldKind.SORTED_KEY, Long.class, 1L),
                new Field("kind", FieldKind.INDEXED, String.class, null),
                new Field("repo_id", FieldKind.INDEXED, Long.class, null),
                new Field("user_id", FieldKind.INDEXED, Long.class, null),
                new Field("details", String.class),
                new Field("seq_num", Long.class))));
        addTable(new UpdateTable("users", 1_000L, List.of(
                new Field("id", FieldKind.KEY, Long.class, 1L),
                new Field("username", FieldKind.INDEXED, String.class, 1L),
                new Field("avatar_url", String.class),
                new Field("html_url", String.class),
                new Field("user_type", String.class),
                new Field("seq_num", Long.class))));
        addTable(new UpdateTable("repos", 1_000L, List.of(
                new Field("id", FieldKind.KEY, Long.class, 1L),
                new Field("reponame", FieldKind.INDEXED, String.class, 100L),
                new Field("fullname", FieldKind.INDEXED, String.class, 1L),
                new Field("owner_id", Long.class),
                new Field("html_url", String.class),
                new Field("homepage", String.class),
                new Field("descr", String.class),
                new Field("topics", String.class),
                new Field("lang", String.class),
                new Field("license", String.class),
                new Field("fork_count", Long.class),
                new Field("issue_count", Long.class),
                new Field("star_count", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("counts_live", null, List.of(
                new Field("window_size", FieldKind.KEY, String.class, 16L),
                new Field("kind", FieldKind.KEY, String.class, 4L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new RankingTable("counts_ranking", null, 4L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, 16L),
                new Field("kind", FieldKind.KEY, String.class, 4L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("counts_history", null, List.of(
                new Field("kind", FieldKind.KEY, String.class, null),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, 16L),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, 16L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("counts_history_fine", 5_000L, List.of(
                new Field("kind", FieldKind.KEY, String.class, null),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, 16L),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, 16L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("repos_live", 1_000L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, null),
                new Field("repo_id", FieldKind.KEY, Long.class, 4L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new RankingTable("repos_ranking", 1_000L, 4L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, null),
                new Field("repo_id", FieldKind.KEY, Long.class, 4L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("repos_history", 5_000L, List.of(
                new Field("repo_id", FieldKind.KEY, Long.class, 5_000L),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, null),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, null),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("repos_history_fine", 5_000L, List.of(
                new Field("repo_id", FieldKind.KEY, Long.class, 5_000L),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, null),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, null),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("users_live", 1_000L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, null),
                new Field("user_id", FieldKind.KEY, Long.class, 4L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new RankingTable("users_ranking", 1_000L, 4L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, null),
                new Field("user_id", FieldKind.KEY, Long.class, 4L),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("users_history", 5_000L, List.of(
                new Field("user_id", FieldKind.KEY, Long.class, 5_000L),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, null),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, null),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("users_history_fine", 5_000L, List.of(
                new Field("user_id", FieldKind.KEY, Long.class, 5_000L),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, null),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, null),
                new Field("num_events", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("stars_live", 1_000L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, null),
                new Field("repo_id", FieldKind.KEY, Long.class, 4L),
                new Field("num_stars", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new RankingTable("stars_ranking", 1_000L, 4L, List.of(
                new Field("window_size", FieldKind.KEY, String.class, null),
                new Field("repo_id", FieldKind.KEY, Long.class, 4L),
                new Field("num_stars", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("stars_history", 5_000L, List.of(
                new Field("repo_id", FieldKind.KEY, Long.class, 5_000L),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, null),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, null),
                new Field("num_stars", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("stars_history_fine", 5_000L, List.of(
                new Field("repo_id", FieldKind.KEY, Long.class, 5_000L),
                new Field("ts_start", FieldKind.SORTED_KEY, String.class, null),
                new Field("ts_end", FieldKind.SORTED_KEY, String.class, null),
                new Field("num_stars", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new Table("trending_live", 1_000L, List.of(
                new Field("repo_id", FieldKind.KEY, Long.class, 1L),
                new Field("trending_score", Long.class),
                new Field("seq_num", Long.class))));
        addTable(new RankingTable("trending_ranking", 1_000L, 1L, List.of(
                new Field("repo_id", FieldKind.KEY, Long.class, 1L),
                new Field("trending_score", Long.class),
                new Field("seq_num", Long.class))));
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
        if (secret == null
                || handshake.getFieldValue("Cookie").contains(secret)
                || handshake.getResourceDescriptor().contains(secret)) {
            LOGGER.info("Client connected {}", socket.getRemoteSocketAddress());
            socket.setAttachment(new ClientState());
        } else {
            LOGGER.info("Client missing auth cookie {}", socket.getRemoteSocketAddress());
            socket.close(CloseFrame.POLICY_VALIDATION, "missing auth");
        }
    }

    @Override
    public void onClose(WebSocket socket, int code, String reason, boolean remote) {
        LOGGER.info("Client disconnected {}", socket.getRemoteSocketAddress());
        ClientState state = socket.getAttachment();
        if (state != null) {
            state.unsubscribeAll();
        }
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
            String json = objectMapper.writeValueAsString(Map.of("table", table, "row", row));
            synchronized (socket) {
                socket.send(json);
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize row update", e);
        } catch (WebsocketNotConnectedException e) {
            // If the client is no longer connected, we can ignore the message all together.
        }
    }

    /**
     * Send the given list of rows, from the given table to the given WebSocket.
     *
     * @param socket
     *            The socket to send to.
     * @param table
     *            The table to send a row for.
     * @param rows
     *            The list of new row values to send.
     */
    private void sendRows(WebSocket socket, String table, List<Map<String, ?>> rows) {
        if (rows.isEmpty()) {
            return;
        } else if (rows.size() == 1) {
            sendRow(socket, table, rows.get(0));
            return;
        } else {
            try {
                Map<String, List<Object>> columnRows = new HashMap<>();
                for (Map<String, ?> row : rows) {
                    for (Entry<String, ?> e : row.entrySet()) {
                        if (!columnRows.containsKey(e.getKey())) {
                            columnRows.put(e.getKey(), new ArrayList<>());
                        }
                        columnRows.get(e.getKey()).add(e.getValue());
                    }
                }
                String json = objectMapper.writeValueAsString(Map.of("table", table, "rows", columnRows));
                synchronized (socket) {
                    socket.send(json);
                }
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Failed to serialize rows update", e);
            } catch (WebsocketNotConnectedException e) {
                // If the client is no longer connected, we can ignore the message all together.
            }
        }
    }

    /**
     * Send a replay completion event for the replay id to the given WebSocket.
     * These events are useful for the client to know when a replay has completed.
     *
     * @param socket
     *            The socket to send to.
     * @param id
     *            The id of the replay that was finished.
     */
    private void sendReplayComplete(WebSocket socket, long id) {
        try {
            String json = objectMapper.writeValueAsString(Map.of("replayed", Long.valueOf(id)));
            synchronized (socket) {
                socket.send(json);
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize replay completion", e);
        } catch (WebsocketNotConnectedException e) {
            // If the client is no longer connected, we can ignore the message all together.
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
                if (table != null && subscription.applicableTo(table, false)) {
                    state.subscribe(subscription, table, socket);
                } else {
                    LOGGER.warn("Client sent a non-applicable subscription request");
                }
            }
            for (Long unsubscribe : message.unsubscribe) {
                if (unsubscribe != null) {
                    state.unsubscribe(unsubscribe);
                }
            }
            for (Subscription replay : message.replay) {
                Table table = tables.get(replay.tableName);
                if (table != null && replay.applicableTo(table, true)) {
                    List<Map<String, ?>> rows = new ArrayList<>();
                    table.getReplayObservable(replay, connections)
                            .subscribeOn(rxScheduler)
                            .subscribe(row -> {
                                rows.add(row);
                                if (rows.size() >= 1024) {
                                    sendRows(socket, table.name, rows);
                                    rows.clear();
                                }
                            }, error -> {
                                LOGGER.error("Error in table replay", error);
                            }, () -> {
                                sendRows(socket, table.name, rows);
                                sendReplayComplete(socket, replay.id);
                            });
                } else {
                    LOGGER.warn("Client sent a non-applicable replay request");
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
