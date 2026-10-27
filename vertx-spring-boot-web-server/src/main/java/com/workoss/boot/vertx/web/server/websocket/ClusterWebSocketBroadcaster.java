package com.workoss.boot.vertx.web.server.websocket;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Tracks the WebSocket sessions held by <em>this</em> JVM and fans messages out across the cluster.
 * <p>
 * A WebSocket session is bound to the TCP connection of a single node, so it can never literally be
 * shared. What crosses the cluster is a {@link ClusterBroadcast} — a holder with no reference to
 * Vert.x or Spring types — and every node pushes it only to the connections it owns locally. The
 * node that owns the target session is the one that writes to it.
 * <p>
 * The payload must be serializable by the clustered event bus codec. A POJO or a
 * {@code ClusterSerializable} works; passing a {@code WebSocketSession}, a Netty buffer or any other
 * live object fails at send time. Callers therefore reduce a message to a transferable value before
 * it reaches {@link #broadcast(Object)}.
 * <p>
 * With a non-clustered Vert.x instance {@code eventBus().publish()} still reaches local consumers, so
 * this class works as a plain local broadcaster with no extra configuration.
 */
public class ClusterWebSocketBroadcaster {

    private static final Logger logger = LoggerFactory.getLogger(ClusterWebSocketBroadcaster.class);

    /** Default address carrying cluster-wide WebSocket fan-out. */
    public static final String DEFAULT_ADDRESS = "vertx.ws.broadcast";

    private final Vertx vertx;
    private final String address;
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> byGroup = new ConcurrentHashMap<>();
    /**
     * Per-session serialized write chain. Every outbound operation (send or close) for a session is
     * appended here so that, no matter how many broadcasts land on the same session at once, only one
     * {@code session.send()} is ever in flight. Spring's {@link WebSocketSession} contract forbids
     * concurrent sends; violating it makes Vert.x throw on the shared {@code ServerWebSocket} and
     * tear the connection down with a self-inflicted close.
     */
    private final Map<String, WriteChain> chains = new ConcurrentHashMap<>();

    public ClusterWebSocketBroadcaster(Vertx vertx) {
        this(vertx, DEFAULT_ADDRESS);
    }

    public ClusterWebSocketBroadcaster(Vertx vertx, String address) {
        this.vertx = vertx;
        this.address = address;
    }

    // --- Local session registry ---

    /**
     * Registers a session under its own id, and under {@code group} when one is given.
     *
     * @return the id the session is tracked by
     */
    public String register(WebSocketSession session, String group) {
        String id = session.getId();
        sessions.put(id, session);
        if (group != null && !group.isEmpty()) {
            byGroup.computeIfAbsent(group, k -> ConcurrentHashMap.newKeySet()).add(id);
        }
        return id;
    }

    /**
     * Removes a session and any group membership it held.
     */
    public void unregister(String sessionId) {
        if (sessions.remove(sessionId) != null) {
            byGroup.values().forEach(ids -> ids.remove(sessionId));
            byGroup.entrySet().removeIf(e -> e.getValue().isEmpty());
        }
        chains.remove(sessionId);
    }

    /**
     * @return the sessions held by this node, keyed by session id
     */
    public Map<String, WebSocketSession> localSessions() {
        return Map.copyOf(sessions);
    }

    public int localSessionCount() {
        return sessions.size();
    }

    // --- Cluster fan-out ---

    /**
     * Publishes to every node in the cluster; each delivers to the sessions it owns.
     *
     * @param payload a serializable value — never a live session or buffer
     */
    public void broadcast(Object payload) {
        broadcast(payload, null, new DeliveryOptions());
    }

    /**
     * Publishes to every node, restricted to one group.
     */
    public void broadcast(Object payload, String group) {
        broadcast(payload, group, new DeliveryOptions());
    }

    /**
     * Publishes to every node with explicit delivery options.
     * <p>
     * Delivery stays local per node: {@link #deliverLocally} picks sessions out of the local
     * registry, so no node writes to a connection it does not own.
     */
    public void broadcast(Object payload, String group, DeliveryOptions options) {
        // Stringified here so the wire format only ever carries strings, whatever the caller passed.
        String text = payload instanceof String s ? s : String.valueOf(payload);
        vertx.eventBus().publish(address, new ClusterBroadcast(group, text), options);
    }

    /**
     * Sends to a node-local session by id, if this node owns it.
     *
     * @return {@code true} when this node held the session and the write was accepted
     */
    public boolean sendToLocal(String sessionId, Object payload) {
        WebSocketSession session = sessions.get(sessionId);
        if (session == null || !session.isOpen()) {
            return false;
        }
        String text = payload instanceof String s ? s : String.valueOf(payload);
        enqueue(sessionId, session, () -> write(session, text)).subscribe();
        return true;
    }

    /**
     * Registers the codec and subscribes to the broadcast address, so a publish from any node —
     * local or remote — reaches {@link #deliverLocally}.
     * <p>
     * The codec registration is not optional bookkeeping. Implementing
     * {@link io.vertx.core.shareddata.ClusterSerializable} only tells Vert.x how to <em>encode</em>
     * a body; the receiving node still has to be able to rebuild it, and a clustered instance that
     * never registered the codec fails at publish time with
     * {@code IllegalArgumentException: No message codec for type}. Doing it here means no
     * construction path can forget it.
     */
    public void registerListener() {
        vertx.eventBus().registerDefaultCodec(ClusterBroadcast.class, new ClusterBroadcastCodec());
        vertx.eventBus().<ClusterBroadcast>consumer(address, message ->
                deliverLocally(message.body()).subscribe());
    }

    /**
     * Writes {@code message} to the matching local sessions, dropping any that have gone away.
     */
    Mono<Void> deliverLocally(ClusterBroadcast message) {
        if (message == null) {
            return Mono.empty();
        }
        return Flux.fromIterable(targetsOf(message))
                .filter(WebSocketSession::isOpen)
                // flatMap (not concatMap): sessions fan out concurrently, but each session's own
                // writes stay serialized inside enqueue — so one slow peer can't block the others,
                // and no session ever sees two in-flight sends at once.
                .flatMap(session -> enqueue(session.getId(), session, () -> write(session, message.payload()))
                        // One dead peer must not cancel the writes queued behind it.
                        .onErrorResume(t -> {
                            logger.debug("Dropping session {}: {}", session.getId(), t.toString());
                            closeQuietly(session);
                            unregister(session.getId());
                            return Mono.empty();
                        }))
                .then();
    }

    private Collection<WebSocketSession> targetsOf(ClusterBroadcast message) {
        if (message.group() == null) {
            return new ArrayList<>(sessions.values());
        }
        Set<String> ids = byGroup.get(message.group());
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        List<WebSocketSession> found = new ArrayList<>();
        for (String id : ids) {
            WebSocketSession session = sessions.get(id);
            if (session != null) {
                found.add(session);
            }
        }
        return found;
    }

    private Mono<Void> write(WebSocketSession session, String payload) {
        return session.send(Flux.just(session.textMessage(payload)));
    }

    /**
     * Appends an outbound operation (send or close) to the session's write chain so that, regardless
     * of how many broadcasts target the same session at once, only one {@code session.send()} is ever
     * in flight. Spring's {@link WebSocketSession} contract forbids concurrent sends; violating it
     * makes Vert.x throw on the shared {@code ServerWebSocket} and tear the connection down with a
     * self-inflicted close (the very symptom this class existed to avoid).
     */
    private Mono<Void> enqueue(String sessionId, WebSocketSession session, Supplier<Mono<Void>> op) {
        WriteChain chain = chains.computeIfAbsent(sessionId, k -> new WriteChain());
        chain.lock.lock();
        try {
            // opMono runs the real write. .cache() makes it idempotent: no matter how many times the
            // chain tail gets subscribed, the underlying session.send() executes exactly once.
            Mono<Void> opMono = Mono.defer(op)
                    .onErrorResume(e -> {
                        logger.debug("Session {} write chain op failed: {}", sessionId, e.toString());
                        return Mono.empty();
                    })
                    .cache();
            // chain.tail is the cached completion of everything queued before this op. Appending via
            // .then(opMono).cache() keeps the whole tail replayable but never re-executing: a later
            // broadcast that re-subscribes the tail replays only the *completion* of earlier ops, then
            // runs its own opMono after them. That is what guarantees one send in flight at a time.
            chain.tail = chain.tail.then(opMono).cache();
            return chain.tail;
        } finally {
            chain.lock.unlock();
        }
    }

    /** Serializes every outbound operation for one session. */
    private static final class WriteChain {
        final ReentrantLock lock = new ReentrantLock();
        volatile Mono<Void> tail = Mono.empty();
    }

    private void closeQuietly(WebSocketSession session) {
        // Route through the chain so close happens only after any pending writes have drained,
        // never concurrently with a send on the same session.
        enqueue(session.getId(), session, () -> session.close(CloseStatus.SERVER_ERROR)).subscribe();
    }

    /**
     * The cluster message: a group plus an already-stringified payload, and nothing else. No
     * session, no buffer, no Vert.x or Spring type.
     * <p>
     * It implements {@link io.vertx.core.shareddata.ClusterSerializable} because the clustered event
     * bus codec has no other way to move it: without this, {@code publish} fails at runtime with
     * {@code No message codec for type}. Holding only a {@code String} keeps the payload trivially
     * serializable — callers convert richer objects before broadcasting.
     * <p>
     * A plain class with a no-arg constructor and non-final fields, because the codec instantiates
     * it reflectively and then fills it in through {@link #readFromBuffer}.
     */
    public static final class ClusterBroadcast implements io.vertx.core.shareddata.ClusterSerializable {

        private String group;
        private String payload;

        public ClusterBroadcast() {
        }

        public ClusterBroadcast(String group, String payload) {
            this.group = group;
            this.payload = payload;
        }

        public String group() {
            return group;
        }

        public String payload() {
            return payload;
        }

        @Override
        public void writeToBuffer(io.vertx.core.buffer.Buffer buffer) {
            byte[] groupBytes = group == null
                    ? new byte[0]
                    : group.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] payloadBytes = payload == null
                    ? new byte[0]
                    : payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.appendInt(groupBytes.length).appendBytes(groupBytes);
            buffer.appendInt(payloadBytes.length).appendBytes(payloadBytes);
        }

        @Override
        public int readFromBuffer(int pos, io.vertx.core.buffer.Buffer buffer) {
            int groupLength = buffer.getInt(pos);
            pos += 4;
            this.group = groupLength == 0 ? null : buffer.getString(pos, pos + groupLength);
            pos += groupLength;

            int payloadLength = buffer.getInt(pos);
            pos += 4;
            this.payload = payloadLength == 0 ? null : buffer.getString(pos, pos + payloadLength);
            pos += payloadLength;

            return pos;
        }

        @Override
        public String toString() {
            return "ClusterBroadcast{group='" + group + "', payload='" + payload + "'}";
        }
    }

    /**
     * Mirrors the {@link ClusterBroadcast} framing so Vert.x can rebuild the message on the
     * receiving node. Registered per node by {@link #registerListener()}.
     */
    static final class ClusterBroadcastCodec
            implements io.vertx.core.eventbus.MessageCodec<ClusterBroadcast, ClusterBroadcast> {

        @Override
        public void encodeToWire(io.vertx.core.buffer.Buffer buffer, ClusterBroadcast broadcast) {
            broadcast.writeToBuffer(buffer);
        }

        @Override
        public ClusterBroadcast decodeFromWire(int pos, io.vertx.core.buffer.Buffer buffer) {
            ClusterBroadcast broadcast = new ClusterBroadcast();
            int end = broadcast.readFromBuffer(pos, buffer);
            // readFromBuffer works from an absolute offset; re-run it over the exact slice so
            // decoding does not depend on where the body sits in the wire buffer.
            broadcast.readFromBuffer(0, buffer.getBuffer(pos, end));
            return broadcast;
        }

        @Override
        public ClusterBroadcast transform(ClusterBroadcast broadcast) {
            return broadcast;
        }

        @Override
        public String name() {
            return "vertx-ws-cluster-broadcast";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
