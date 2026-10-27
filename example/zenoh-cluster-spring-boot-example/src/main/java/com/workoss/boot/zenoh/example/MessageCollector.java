package com.workoss.boot.zenoh.example;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.ZenohMembership;
import com.workoss.boot.zenoh.cluster.ZenohSubscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * Demo of the declarative listener annotations of {@code spring-boot-zenoh-cluster}:
 * {@link ZenohSubscribe} for messages and {@link ZenohMembership} for membership events. Every
 * method below is discovered by the framework's processors and bound automatically once the context
 * is ready — no manual {@code messaging().subscribe(...)} or
 * {@code addMembershipListener(...)} wiring, and the bindings survive cluster restarts.
 * <p>
 * Covered subscription kinds and parameter types:
 * <ul>
 *   <li>exact broadcast {@code demo/broadcast} — {@link ClusterMessage} parameter;</li>
 *   <li>wildcard broadcast {@code alerts/**} — {@link ClusterMessage} parameter;</li>
 *   <li>wildcard broadcast {@code raw/**} — raw {@code byte[]} payload;</li>
 *   <li>wildcard broadcast {@code notices/**} — {@link String} payload (UTF-8);</li>
 *   <li>point-to-point ({@code direct = true}) — {@link ClusterMessage} parameter;</li>
 *   <li>broadcast with {@code replyTo} — the returned value is published as a reply;</li>
 *   <li>broadcast with {@code replyTo} and a {@link CompletableFuture} — the reply is published
 *       when the future completes, without blocking the sender;</li>
 *   <li>two handlers on {@code demo/ordered/**} sequenced by Spring's {@link Order}.</li>
 * </ul>
 * Membership events are recorded by the {@link ZenohMembership} handlers. Received messages and
 * membership events are kept in memory and exposed through {@code /cluster/messages} and
 * {@code /cluster/membership}.
 */
@Component
public class MessageCollector {

    private static final Logger log = LoggerFactory.getLogger(MessageCollector.class);

    /** Broadcast address the exact demo handler listens on and the default publish target. */
    public static final String DEMO_BROADCAST_ADDRESS = "demo/broadcast";

    /** Broadcast address of the request/response demo; replies land on {@link #ECHO_REPLY_ADDRESS}. */
    public static final String ECHO_ADDRESS = "demo/echo";

    /** Broadcast address the echo replies are published to. */
    public static final String ECHO_REPLY_ADDRESS = "demo/echo/replies";

    /** Broadcast address of the asynchronous (future based) echo demo. */
    public static final String SLOW_ECHO_ADDRESS = "demo/slow-echo";

    /** Broadcast address of the @Order demo; both handlers share this pattern. */
    public static final String ORDERED_ADDRESS = "demo/ordered/**";

    /** How long the asynchronous echo takes, so the reply visibly arrives after the request. */
    private static final long SLOW_ECHO_MILLIS = 500L;

    private final int maxKept = 100;
    private final ConcurrentLinkedDeque<ReceivedView> received = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<MembershipView> membership = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<String> orderedSequence = new ConcurrentLinkedDeque<>();

    // --- exact broadcast (ClusterMessage) ---

    @ZenohSubscribe(DEMO_BROADCAST_ADDRESS)
    void onExactBroadcast(ClusterMessage message) {
        record("broadcast", message);
    }

    // --- wildcard broadcast (ClusterMessage) ---

    @ZenohSubscribe("alerts/**")
    void onAlert(ClusterMessage message) {
        record("alert", message);
    }

    // --- wildcard broadcast (raw bytes) ---

    @ZenohSubscribe("raw/**")
    void onRawBytes(byte[] payload) {
        recordRaw("raw", payload);
    }

    // --- wildcard broadcast (UTF-8 string) ---

    @ZenohSubscribe("notices/**")
    void onNotice(String payload) {
        recordString("notice", payload);
    }

    // --- point-to-point (direct) ---

    @ZenohSubscribe(direct = true)
    void onDirect(ClusterMessage message) {
        record("direct", message);
    }

    // --- request/response: the return value is published to replyTo ---

    @ZenohSubscribe(value = ECHO_ADDRESS, replyTo = ECHO_REPLY_ADDRESS)
    String onEcho(String payload) {
        return "echo:" + payload;
    }

    @ZenohSubscribe(ECHO_REPLY_ADDRESS)
    void onEchoReply(ClusterMessage message) {
        record("reply", message);
    }

    // --- asynchronous offload: the reply is published when the future completes ---

    @ZenohSubscribe(value = SLOW_ECHO_ADDRESS, replyTo = ECHO_REPLY_ADDRESS)
    CompletableFuture<String> onSlowEcho(String payload) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(SLOW_ECHO_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "slow-echo:" + payload;
        });
    }

    // --- @Order: same address, two handlers, declared order is the run order ---

    @ZenohSubscribe(ORDERED_ADDRESS)
    @Order(1)
    void onOrderedFirst(ClusterMessage message) {
        orderedSequence.addLast("first:" + message.payloadAsString());
    }

    @ZenohSubscribe(ORDERED_ADDRESS)
    @Order(2)
    void onOrderedSecond(ClusterMessage message) {
        orderedSequence.addLast("second:" + message.payloadAsString());
    }

    // --- membership events ---

    @ZenohMembership(ZenohMembership.Event.NODE_JOINED)
    void onNodeJoined(NodeInfo node) {
        log.info("@ZenohMembership nodeJoined: {}", node.nodeId());
        membership.addLast(new MembershipView("join", node.nodeId(), node.incarnation(),
                node.hostName(), Instant.now()));
        trim(membership);
    }

    @ZenohMembership(ZenohMembership.Event.NODE_LEFT)
    void onNodeLeft(NodeInfo node) {
        log.info("@ZenohMembership nodeLeft: {}", node.nodeId());
        membership.addLast(new MembershipView("leave", node.nodeId(), node.incarnation(),
                node.hostName(), Instant.now()));
        trim(membership);
    }

    // --- recording ---

    private void record(String kind, ClusterMessage message) {
        ReceivedView view = new ReceivedView(kind, message.address(), message.sourceNodeId(),
                message.targetNodeId(), message.payloadAsString(), Instant.ofEpochMilli(message.timestamp()));
        publish(view);
    }

    private void recordRaw(String kind, byte[] payload) {
        publish(new ReceivedView(kind, null, null, null, new String(payload, StandardCharsets.UTF_8),
                Instant.now()));
    }

    private void recordString(String kind, String payload) {
        publish(new ReceivedView(kind, null, null, null, payload, Instant.now()));
    }

    private void publish(ReceivedView view) {
        log.info("@ZenohSubscribe handler received [{}]: {}", view.kind(), view);
        received.addLast(view);
        trim(received);
    }

    private void trim(ConcurrentLinkedDeque<?> deque) {
        while (deque.size() > maxKept) {
            deque.removeFirst();
        }
    }

    public List<ReceivedView> recent() {
        return List.copyOf(received);
    }

    public List<MembershipView> recentMembership() {
        return List.copyOf(membership);
    }

    public List<String> recentOrderedSequence() {
        return List.copyOf(orderedSequence);
    }

    /**
     * Immutable view of a message received by one of the annotated handlers, safe to serialize in a
     * JSON response. {@code address}/{@code sourceNodeId}/{@code targetNodeId} are {@code null} for
     * the {@code byte[]} and {@code String} handlers, which only see the payload.
     *
     * @param kind which handler captured it: broadcast, alert, raw, notice, direct or reply
     */
    public record ReceivedView(String kind, String address, String sourceNodeId, String targetNodeId,
                               String payload, Instant receivedAt) {
        public boolean isBroadcast() {
            return address != null;
        }
    }

    /**
     * Immutable view of a membership event captured by a {@link ZenohMembership} handler.
     *
     * @param kind join or leave
     */
    public record MembershipView(String kind, String nodeId, String incarnation, String hostName,
                                 Instant receivedAt) {
    }
}
