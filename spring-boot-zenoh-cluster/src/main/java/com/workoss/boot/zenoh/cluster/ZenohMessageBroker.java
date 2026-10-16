package com.workoss.boot.zenoh.cluster;

import com.workoss.zenoh.Sample;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cluster message broker: broadcast (pub/sub) and point-to-point messaging over Zenoh, with no
 * Vert.x dependency.
 * <p>
 * Two channels:
 * <ul>
 *   <li><b>Broadcast</b> — one wildcard subscription on {@code bus/broadcast/**} fans out to the
 *       handlers registered for each address via {@link #subscribe(String, MessageHandler)}.
 *       {@link #broadcast(String, byte[])} reaches every node subscribed to the address, including
 *       the sender itself (plain Zenoh pub/sub semantics). Broadcasts are transient: a node that
 *       joins later does not see earlier messages — use the shared map for state that must
 *       survive.</li>
 *   <li><b>Point-to-point</b> — each node subscribes to its own {@code bus/inbox/{nodeId}} key.
 *       {@link #sendToNode(String, byte[])} delivers to exactly one node's
 *       {@link #onNodeMessage(MessageHandler)} handlers; messages for an offline node are dropped
 *       (no storage), so pair it with {@link ZenohClusterManager#getNodes()} when delivery
 *       guarantees matter.</li>
 * </ul>
 * Wire envelope for both channels:
 * {@code [8B timestamp][2B srcLen][src UTF-8][2B addrLen][addr UTF-8][payload]}. The address is
 * carried inside the envelope, not read back from the key, because {@link KeyPrefixStrategy}'s
 * sanitisation is lossy ({@code /} and {@code []} collapse to {@code _}) and two distinct addresses
 * could otherwise share a key.
 * <p>
 * This bean is bound to the live Zenoh session by {@link ZenohClusterManager} while it is joined;
 * before that, {@link #isActive()} is {@code false} and send operations fail fast.
 */
public class ZenohMessageBroker {

    private static final Logger logger = LoggerFactory.getLogger(ZenohMessageBroker.class);

    /** Zenoh sample kind for a delete. */
    private static final byte KIND_DELETE = 1;

    private final ZenohClusterManager manager;
    private volatile ExecutorService dispatchExecutor;

    /** address -> handlers, for broadcasts. */
    private final Map<String, List<MessageHandler>> broadcastHandlers = new ConcurrentHashMap<>();
    /** handlers for point-to-point messages addressed to this node. */
    private final List<MessageHandler> nodeHandlers = new CopyOnWriteArrayList<>();

    private final AtomicBoolean bound = new AtomicBoolean(false);
    private volatile Session session;
    private volatile Subscriber broadcastSubscriber;
    private volatile Subscriber inboxSubscriber;
    private volatile Thread broadcastPollThread;
    private volatile Thread inboxPollThread;

    public ZenohMessageBroker(ZenohClusterManager manager) {
        this.manager = manager;
        this.dispatchExecutor = newDispatchExecutor();
    }

    private static ExecutorService newDispatchExecutor() {
        return Executors.newFixedThreadPool(4, daemonFactory("zenoh-bus-dispatch"));
    }

    // --- Lifecycle (driven by ZenohClusterManager) ---

    void bind(Session session) {
        if (!bound.compareAndSet(false, true)) {
            return;
        }
        this.session = session;
        // Recreate the dispatch executor when this broker is being restarted after a stop().
        if (dispatchExecutor.isShutdown()) {
            this.dispatchExecutor = newDispatchExecutor();
        }
        KeyPrefixStrategy keyPrefix = manager.getKeyPrefix();
        String nodeId = manager.getNodeId();

        this.broadcastSubscriber = new Subscriber(session, keyPrefix.busBroadcastPattern());
        this.broadcastPollThread = startPollThread(this::pollBroadcasts, "zenoh-bus-broadcast");

        this.inboxSubscriber = new Subscriber(session, keyPrefix.busInboxKey(nodeId));
        this.inboxPollThread = startPollThread(this::pollInbox, "zenoh-bus-inbox");

        logger.info("Message broker bound for node {} (broadcast={}, inbox={})",
                nodeId, keyPrefix.busBroadcastPattern(), keyPrefix.busInboxKey(nodeId));
    }

    void unbind() {
        if (!bound.compareAndSet(true, false)) {
            return;
        }
        Subscriber broadcast = this.broadcastSubscriber;
        this.broadcastSubscriber = null;
        if (broadcast != null) {
            try {
                broadcast.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare broadcast subscriber: {}", e.getMessage());
            }
        }
        Subscriber inbox = this.inboxSubscriber;
        this.inboxSubscriber = null;
        if (inbox != null) {
            try {
                inbox.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare inbox subscriber: {}", e.getMessage());
            }
        }
        joinThread(broadcastPollThread);
        broadcastPollThread = null;
        joinThread(inboxPollThread);
        inboxPollThread = null;
        this.session = null;
        logger.info("Message broker unbound for node {}", manager.getNodeId());
    }

    public boolean isActive() {
        return bound.get() && session != null;
    }

    // --- Broadcast API ---

    /**
     * Subscribe to a broadcast address. Subscribing the same handler twice is a no-op.
     */
    public void subscribe(String address, MessageHandler handler) {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(handler, "handler");
        broadcastHandlers.compute(address, (a, handlers) -> {
            if (handlers == null) {
                handlers = new CopyOnWriteArrayList<>();
            }
            if (!handlers.contains(handler)) {
                handlers.add(handler);
            }
            return handlers;
        });
    }

    /**
     * Unsubscribe a handler from a broadcast address.
     */
    public void unsubscribe(String address, MessageHandler handler) {
        List<MessageHandler> handlers = broadcastHandlers.get(address);
        if (handlers != null) {
            handlers.remove(handler);
            if (handlers.isEmpty()) {
                broadcastHandlers.remove(address, handlers);
            }
        }
    }

    /**
     * @return the addresses with at least one local handler
     */
    public Set<String> getSubscribedAddresses() {
        return Set.copyOf(broadcastHandlers.keySet());
    }

    /**
     * Broadcast a message to every node subscribed to {@code address}, including this one.
     *
     * @throws IllegalStateException when the cluster is not joined
     */
    public void broadcast(String address, byte[] payload) {
        Objects.requireNonNull(address, "address");
        Session current = requireSession();
        String key = manager.getKeyPrefix().busBroadcastKey(address);
        current.put(key, encode(address, manager.getNodeId(), payload));
    }

    // --- Point-to-point API ---

    /**
     * Register a handler for point-to-point messages addressed to this node. Registering the same
     * handler twice is a no-op.
     */
    public void onNodeMessage(MessageHandler handler) {
        Objects.requireNonNull(handler, "handler");
        if (!nodeHandlers.contains(handler)) {
            nodeHandlers.add(handler);
        }
    }

    /**
     * Remove a point-to-point handler.
     */
    public void removeNodeMessageHandler(MessageHandler handler) {
        nodeHandlers.remove(handler);
    }

    /**
     * Send a message to exactly one node. The recipient must be alive — messages are not stored,
     * so a send to a departed node is silently dropped.
     *
     * @param targetNodeId the recipient node id
     * @throws IllegalStateException when the cluster is not joined
     */
    public void sendToNode(String targetNodeId, byte[] payload) {
        Objects.requireNonNull(targetNodeId, "targetNodeId");
        Session current = requireSession();
        String key = manager.getKeyPrefix().busInboxKey(targetNodeId);
        current.put(key, encode(null, manager.getNodeId(), payload));
    }

    // --- Wire format ---

    private static byte[] encode(String address, String sourceNodeId, byte[] payload) {
        byte[] src = sourceNodeId.getBytes(StandardCharsets.UTF_8);
        byte[] addr = address == null ? new byte[0] : address.getBytes(StandardCharsets.UTF_8);
        byte[] body = payload == null ? new byte[0] : payload;
        ByteBuffer buffer = ByteBuffer.allocate(8 + 2 + src.length + 2 + addr.length + body.length);
        buffer.putLong(System.currentTimeMillis());
        buffer.putShort((short) src.length).put(src);
        buffer.putShort((short) addr.length).put(addr);
        buffer.put(body);
        return buffer.array();
    }

    private static Decoded decode(String keyExpr, byte[] payload) {
        if (payload == null || payload.length < 12) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            long timestamp = buffer.getLong();
            int srcLen = Short.toUnsignedInt(buffer.getShort());
            if (srcLen < 0 || buffer.remaining() < srcLen + 2) {
                return null;
            }
            byte[] srcBytes = new byte[srcLen];
            buffer.get(srcBytes);
            String sourceNodeId = new String(srcBytes, StandardCharsets.UTF_8);
            int addrLen = Short.toUnsignedInt(buffer.getShort());
            if (addrLen < 0 || buffer.remaining() < addrLen) {
                return null;
            }
            byte[] addrBytes = new byte[addrLen];
            buffer.get(addrBytes);
            String address = addrLen == 0 ? null : new String(addrBytes, StandardCharsets.UTF_8);
            byte[] body = new byte[buffer.remaining()];
            buffer.get(body);
            String targetNodeId = lastSegment(keyExpr);
            return new Decoded(address, sourceNodeId, targetNodeId, body, timestamp);
        } catch (Exception e) {
            logger.debug("Failed to decode message envelope on {}: {}", keyExpr, e.getMessage());
            return null;
        }
    }

    private record Decoded(String address, String sourceNodeId, String targetNodeId,
                           byte[] payload, long timestamp) {
    }

    private static String lastSegment(String keyExpr) {
        if (keyExpr == null) {
            return null;
        }
        int slash = keyExpr.lastIndexOf('/');
        return slash >= 0 && slash < keyExpr.length() - 1 ? keyExpr.substring(slash + 1) : null;
    }

    private Session requireSession() {
        Session current = session;
        if (current == null || !bound.get()) {
            throw new IllegalStateException(
                    "Zenoh message broker is not active — the cluster has not joined yet");
        }
        return current;
    }

    // --- Polling ---

    private void pollBroadcasts() {
        while (bound.get() && broadcastSubscriber != null && !broadcastSubscriber.isUndeclared()) {
            try {
                Optional<Sample> sampleOpt = broadcastSubscriber.tryRecv();
                if (sampleOpt.isEmpty()) {
                    Thread.sleep(50);
                    continue;
                }
                Sample sample = sampleOpt.get();
                if (sample.kind() == KIND_DELETE) {
                    continue;
                }
                Decoded decoded = decode(sample.keyExpr(), sample.payload());
                if (decoded == null) {
                    continue;
                }
                List<MessageHandler> handlers = decoded.address() == null
                        ? List.of() : broadcastHandlers.get(decoded.address());
                if (handlers == null || handlers.isEmpty()) {
                    continue;
                }
                ClusterMessage message = new ClusterMessage(decoded.address(), decoded.sourceNodeId(),
                        null, decoded.payload(), decoded.timestamp());
                for (MessageHandler handler : List.copyOf(handlers)) {
                    dispatch(message, handler);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.debug("Broadcast poll error: {}", e.getMessage());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void pollInbox() {
        while (bound.get() && inboxSubscriber != null && !inboxSubscriber.isUndeclared()) {
            try {
                Optional<Sample> sampleOpt = inboxSubscriber.tryRecv();
                if (sampleOpt.isEmpty()) {
                    Thread.sleep(50);
                    continue;
                }
                Sample sample = sampleOpt.get();
                if (sample.kind() == KIND_DELETE) {
                    continue;
                }
                Decoded decoded = decode(sample.keyExpr(), sample.payload());
                if (decoded == null) {
                    continue;
                }
                ClusterMessage message = new ClusterMessage(null, decoded.sourceNodeId(),
                        decoded.targetNodeId(), decoded.payload(), decoded.timestamp());
                for (MessageHandler handler : List.copyOf(nodeHandlers)) {
                    dispatch(message, handler);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.debug("Inbox poll error: {}", e.getMessage());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void dispatch(ClusterMessage message, MessageHandler handler) {
        ExecutorService executor = dispatchExecutor;
        executor.execute(() -> {
            try {
                handler.onMessage(message);
            } catch (Exception e) {
                logger.warn("Message handler failed on {}: {}", message.address(), e.getMessage());
            }
        });
    }

    // --- Threads ---

    private Thread startPollThread(Runnable loop, String namePrefix) {
        String nodeId = manager.getNodeId();
        String suffix = nodeId.length() >= 8 ? nodeId.substring(0, 8) : nodeId;
        Thread thread = new Thread(loop, namePrefix + "-" + suffix);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void joinThread(Thread thread) {
        if (thread == null) {
            return;
        }
        thread.interrupt();
        try {
            thread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory daemonFactory(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Shut down the dispatch executor. Called by the manager when it stops; a later
     * {@link #bind(Session)} recreates it.
     */
    void shutdown() {
        ExecutorService executor = dispatchExecutor;
        List<Runnable> pending = executor.shutdownNow();
        if (!pending.isEmpty()) {
            logger.debug("Dropped {} pending message dispatch task(s)", pending.size());
        }
    }
}
