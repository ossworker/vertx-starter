package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Query;
import com.workoss.zenoh.Queryable;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Sample;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Cluster-wide replica of the EventBus registration table and of the per-node {@link NodeInfo}.
 * <p>
 * {@code ClusteredEventBus} routes every message through two {@code ClusterManager} callbacks:
 * <ul>
 *   <li>{@link #registrationsOf(String)} — {@code DefaultNodeSelector} asks which nodes serve an
 *       address before it can pick send/publish targets. Returning only local registrations pins
 *       every message to the local JVM, so this must answer for the whole cluster.</li>
 *   <li>{@link #nodeInfoOf(String)} — {@code ClusteredEventBus.connect()} needs the peer's
 *       {@code host:port} to open its outbound Netty socket.</li>
 * </ul>
 * Both are served from local memory. Zenoh replicates writes outward, and each node also declares a
 * queryable over both keyspaces so a joining node can pull the current state in one round trip —
 * mirroring how {@link ZenohMapStore} replaces the storage-manager plugin.
 * <p>
 * Serialization goes through Vert.x's own {@code writeToBuffer}/{@code readFromBuffer} — i.e. the
 * {@code ClusterSerializable} contract — so the wire format is the same one the native clustered
 * event bus would have produced. Encoding these with {@code toString()} instead would be lossy:
 * {@link NodeInfo} and {@link RegistrationInfo} emit human-readable debug text with no parser.
 * <p>
 * Consistency: a local mutation is visible to this node immediately and replicated asynchronously,
 * so peers converge eventually. The registration table is advisory — it feeds node <em>selection</em>
 * only, and a stale entry costs one failed connect attempt, not a lost message.
 */
public class ClusterRegistrationTable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRegistrationTable.class);

    /** Zenoh sample kind for a delete. */
    private static final byte KIND_DELETE = 1;

    private final KeyPrefixStrategy keyPrefix;
    private final String nodeId;

    /** address -> nodeId -> registrations reported by that node. */
    private final Map<String, Map<String, List<RegistrationInfo>>> registrations = new ConcurrentHashMap<>();
    /** nodeId -> info, for this node and every discovered peer. */
    private final Map<String, NodeInfo> nodeInfos = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Session session;
    private Subscriber registrationSubscriber;
    private Subscriber nodeInfoSubscriber;
    private Queryable registrationQueryable;
    private Queryable nodeInfoQueryable;
    private Thread registrationPollThread;
    private Thread nodeInfoPollThread;
    private Thread registrationQueryThread;
    private Thread nodeInfoQueryThread;
    private volatile RegistrationListener registrationListener;

    public ClusterRegistrationTable(KeyPrefixStrategy keyPrefix, String nodeId) {
        this.keyPrefix = keyPrefix;
        this.nodeId = nodeId;
    }

    // --- Lifecycle ---

    /**
     * Replicates the registration table and node-info keyspace from peers, and answers their queries
     * about what this node owns.
     */
    public void start(Session session) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.session = session;
        if (session != null) {
            String registrationPattern = keyPrefix.allRegistrationsPattern();
            String nodeInfoPattern = keyPrefix.allNodeInfosPattern();
            this.registrationSubscriber = new Subscriber(session, registrationPattern);
            this.nodeInfoSubscriber = new Subscriber(session, nodeInfoPattern);
            this.registrationPollThread = startPoll(this::pollRegistrations, "zenoh-reg-poll");
            this.nodeInfoPollThread = startPoll(this::pollNodeInfos, "zenoh-nodeinfo-poll");

            // Queryables are what make a late joiner converge immediately instead of on the next
            // heartbeat: without them a peer's `get` finds nothing, because a put with no storage
            // manager is a one-shot notification rather than queryable state.
            this.registrationQueryable = new Queryable(session, registrationPattern);
            this.nodeInfoQueryable = new Queryable(session, nodeInfoPattern);
            this.registrationQueryThread = startPoll(this::pollRegistrationQueries, "zenoh-reg-query");
            this.nodeInfoQueryThread = startPoll(this::pollNodeInfoQueries, "zenoh-nodeinfo-query");
        }
        logger.info("Cluster registration table started for node {}", nodeId);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        undeclare(registrationSubscriber);
        registrationSubscriber = null;
        undeclare(nodeInfoSubscriber);
        nodeInfoSubscriber = null;
        undeclare(registrationQueryable);
        registrationQueryable = null;
        undeclare(nodeInfoQueryable);
        nodeInfoQueryable = null;
        join(registrationPollThread);
        registrationPollThread = null;
        join(nodeInfoPollThread);
        nodeInfoPollThread = null;
        join(registrationQueryThread);
        registrationQueryThread = null;
        join(nodeInfoQueryThread);
        nodeInfoQueryThread = null;
        this.session = null;
        registrations.clear();
        nodeInfos.clear();
        logger.info("Cluster registration table stopped for node {}", nodeId);
    }

    public void setRegistrationListener(RegistrationListener listener) {
        this.registrationListener = listener;
    }

    // --- ClusterManager callbacks ---

    /**
     * Registrations for {@code address} across every node that has one. This is what
     * {@code DefaultNodeSelector} turns into send/publish targets, so a local-only answer is what
     * silently confines a clustered publish to one JVM.
     */
    public List<RegistrationInfo> registrationsOf(String address) {
        Map<String, List<RegistrationInfo>> byNode = registrations.get(address);
        if (byNode == null || byNode.isEmpty()) {
            return List.of();
        }
        List<RegistrationInfo> all = new ArrayList<>();
        for (List<RegistrationInfo> nodeRegistrations : byNode.values()) {
            all.addAll(nodeRegistrations);
        }
        return List.copyOf(all);
    }

    /**
     * @return the peer's {@code host:port}, or {@code null} when this node has not learned it yet
     */
    public NodeInfo nodeInfoOf(String targetNodeId) {
        return targetNodeId == null ? null : nodeInfos.get(targetNodeId);
    }

    // --- Local mutations (called by ClusterManager, replicated on the way out) ---

    /**
     * Records a locally added registration and publishes it so peers can route to this node.
     */
    public void addRegistration(String address, RegistrationInfo info) {
        registrations.computeIfAbsent(address, k -> new ConcurrentHashMap<>())
                .put(info.nodeId(), List.of(info));
        publishRegistration(address, info.nodeId(), encodeRegistrationEnvelope(address, info));
        fireRegistrationsUpdated(address);
    }

    /**
     * Drops a locally removed registration, and deletes the per-node key when it was the last one.
     */
    public void removeRegistration(String address, RegistrationInfo info) {
        Map<String, List<RegistrationInfo>> byNode = registrations.get(address);
        if (byNode != null) {
            List<RegistrationInfo> remaining = byNode.get(info.nodeId());
            if (remaining != null && remaining.size() <= 1) {
                byNode.remove(info.nodeId());
            }
            if (byNode.isEmpty()) {
                registrations.remove(address, byNode);
            }
        }
        Session current = this.session;
        if (current != null) {
            try {
                current.delete(keyPrefix.registrationKey(address, info.nodeId()));
            } catch (Exception e) {
                logger.debug("Failed to delete registration key for {}@{}: {}",
                        address, info.nodeId(), e.getMessage());
            }
        }
        fireRegistrationsUpdated(address);
    }

    /**
     * Records this node's own {@link NodeInfo} and publishes it for peers to dial back on.
     */
    public void setLocalNodeInfo(NodeInfo info) {
        nodeInfos.put(nodeId, info);
        Session current = this.session;
        if (current != null) {
            try {
                current.put(keyPrefix.nodeInfoKey(nodeId), encode(info));
            } catch (Exception e) {
                logger.warn("Failed to publish node info for {}", nodeId, e);
            }
        }
    }

    /**
     * Forgets a departed node's registrations and info, then notifies the selector for every address
     * it touched — otherwise {@code DefaultNodeSelector} keeps a cached entry pointing at a node
     * whose socket is gone, and each publish to it fails on connect.
     */
    public void removeNode(String departedNodeId) {
        if (departedNodeId == null || departedNodeId.equals(nodeId)) {
            return;
        }
        nodeInfos.remove(departedNodeId);
        int before = registrations.size();
        forgetNodeRegistrations(departedNodeId);
        if (registrations.size() != before) {
            logger.info("Removed registrations of departed node {}", departedNodeId);
        }
    }

    // --- Replication ---

    private void publishRegistration(String address, String ownerNodeId, byte[] payload) {
        Session current = this.session;
        if (current == null) {
            return;
        }
        try {
            current.put(keyPrefix.registrationKey(address, ownerNodeId), payload);
        } catch (Exception e) {
            logger.warn("Failed to publish registration {}@{}", address, ownerNodeId, e);
        }
    }

    private void pollRegistrations() {
        while (running.get() && registrationSubscriber != null && !registrationSubscriber.isUndeclared()) {
            try {
                Sample sample = registrationSubscriber.recv().orElse(null);
                if (sample == null) {
                    continue;
                }
                applyRegistrationSample(sample);
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted() || !running.get()) {
                    break;
                }
                logger.debug("Registration poll error: {}", e.getMessage());
            }
        }
    }

    private void applyRegistrationSample(Sample sample) {
        String owner = registrationOwnerOf(sample.keyExpr());
        if (owner == null || owner.equals(nodeId)) {
            // Either an unparseable key, or our own write coming back — the local replica has it.
            return;
        }
        if (sample.kind() == KIND_DELETE) {
            // A Zenoh delete carries no payload, so the address cannot be read back from it.
            // Clearing by owner is both correct and cheaper: a node holds one registration per
            // address, so a stale entry here would keep the selector routing at a dead node.
            forgetNodeRegistrations(owner);
            return;
        }
        String address = decodeEnvelopeAddress(sample.payload());
        if (address == null) {
            logger.debug("Registration sample on {} carried no address envelope", sample.keyExpr());
            return;
        }
        RegistrationInfo info = decodeEnvelopeInfo(sample.payload());
        if (info == null) {
            return;
        }
        registrations.computeIfAbsent(address, k -> new ConcurrentHashMap<>()).put(owner, List.of(info));
        fireRegistrationsUpdated(address);
    }

    /**
     * Drops every registration owned by {@code owner} and re-fires each affected address.
     */
    private void forgetNodeRegistrations(String owner) {
        Set<String> affected = new HashSet<>();
        for (Map.Entry<String, Map<String, List<RegistrationInfo>>> entry : registrations.entrySet()) {
            if (entry.getValue().remove(owner) != null) {
                affected.add(entry.getKey());
            }
        }
        registrations.entrySet().removeIf(e -> e.getValue().isEmpty());
        affected.forEach(this::fireRegistrationsUpdated);
    }

    private void pollNodeInfos() {
        while (running.get() && nodeInfoSubscriber != null && !nodeInfoSubscriber.isUndeclared()) {
            try {
                Sample sample = nodeInfoSubscriber.recv().orElse(null);
                if (sample == null) {
                    continue;
                }
                String owner = lastSegment(sample.keyExpr());
                if (owner == null || owner.equals(nodeId)) {
                    continue;
                }
                if (sample.kind() == KIND_DELETE) {
                    nodeInfos.remove(owner);
                    continue;
                }
                NodeInfo info = decodeNodeInfo(sample.payload());
                if (info != null) {
                    nodeInfos.put(owner, info);
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted() || !running.get()) {
                    break;
                }
                logger.debug("Node info poll error: {}", e.getMessage());
            }
        }
    }

    /**
     * Seeds {@link #nodeInfoOf} for peers that were already up before this node subscribed. Each peer
     * answers from its own node-info queryable, so this resolves in one round trip rather than waiting
     * for the peer's next heartbeat {@code put}.
     */
    public void discoverExistingNodeInfos() {
        Session current = this.session;
        if (current == null) {
            return;
        }
        try {
            for (Reply reply : current.get(keyPrefix.allNodeInfosPattern())) {
                if (!reply.success() || reply.sampleKeyExpr().isEmpty()) {
                    continue;
                }
                String owner = lastSegment(reply.sampleKeyExpr().get());
                if (owner == null || owner.equals(nodeId)) {
                    continue;
                }
                NodeInfo info = decodeNodeInfo(reply.samplePayload().orElse(null));
                if (info != null) {
                    nodeInfos.put(owner, info);
                }
            }
        } catch (Exception e) {
            logger.debug("Node info discovery failed: {}", e.getMessage());
        }
    }

    /**
     * Seeds {@link #registrationsOf} with what peers were already serving before this node subscribed.
     * <p>
     * Without this a late joiner knows no address exists on a peer until that peer's next heartbeat
     * re-publishes, and every publish in the window is dropped by {@code DefaultNodeSelector} — it
     * cannot pick a node it has no registration for.
     */
    public void discoverExistingRegistrations() {
        Session current = this.session;
        if (current == null) {
            return;
        }
        try {
            for (Reply reply : current.get(keyPrefix.allRegistrationsPattern())) {
                if (!reply.success() || reply.sampleKeyExpr().isEmpty()) {
                    continue;
                }
                String owner = lastSegment(reply.sampleKeyExpr().get());
                if (owner == null || owner.equals(nodeId)) {
                    continue;
                }
                byte[] payload = reply.samplePayload().orElse(null);
                String address = decodeEnvelopeAddress(payload);
                RegistrationInfo info = decodeEnvelopeInfo(payload);
                if (address == null || info == null) {
                    continue;
                }
                registrations.computeIfAbsent(address, k -> new ConcurrentHashMap<>())
                        .put(owner, List.of(info));
                fireRegistrationsUpdated(address);
            }
        } catch (Exception e) {
            logger.debug("Registration discovery failed: {}", e.getMessage());
        }
    }

    /**
     * Re-publishes every registration this node owns.
     * <p>
     * Peers normally learn these from the queryable at join time; the timer covers the cases a query
     * cannot — a link that dropped and reformed, or a peer whose own discovery ran before this node
     * had anything to answer with.
     */
    public void republishLocalRegistrations() {
        Session current = this.session;
        if (current == null) {
            return;
        }
        forEachOwnRegistration((address, info) -> {
            try {
                current.put(keyPrefix.registrationKey(address, nodeId),
                        encodeRegistrationEnvelope(address, info));
            } catch (Exception e) {
                logger.debug("Failed to re-publish registration {}@{}: {}", address, nodeId, e.getMessage());
            }
        });
    }

    /**
     * Re-publishes this node's {@link NodeInfo} so peers that missed the queryable answer still resolve it.
     */
    public void republishLocalNodeInfo() {
        NodeInfo info = nodeInfos.get(nodeId);
        if (info == null) {
            return;
        }
        Session current = this.session;
        if (current == null) {
            return;
        }
        try {
            current.put(keyPrefix.nodeInfoKey(nodeId), encode(info));
        } catch (Exception e) {
            logger.debug("Failed to re-publish node info for {}: {}", nodeId, e.getMessage());
        }
    }

    // --- Queryables: each node asserts only what it owns ---

    private void pollNodeInfoQueries() {
        while (running.get() && nodeInfoQueryable != null && !nodeInfoQueryable.isUndeclared()) {
            Queryable current = nodeInfoQueryable;
            if (current == null) {
                break;
            }
            // try-with-resources matters: dropping the query without replying is what tells the
            // requesting node that no more replies are coming.
            try (Query query = new Query(current)) {
                NodeInfo info = nodeInfos.get(nodeId);
                if (info != null) {
                    query.reply(keyPrefix.nodeInfoKey(nodeId), encode(info));
                }
            } catch (Exception e) {
                if (!keepPolling(nodeInfoQueryable)) {
                    break;
                }
                logger.debug("Node info query poll error: {}", e.getMessage());
            }
        }
    }

    private void pollRegistrationQueries() {
        while (running.get() && registrationQueryable != null && !registrationQueryable.isUndeclared()) {
            Queryable current = registrationQueryable;
            if (current == null) {
                break;
            }
            try (Query query = new Query(current)) {
                forEachOwnRegistration((address, info) ->
                        query.reply(keyPrefix.registrationKey(address, nodeId),
                                encodeRegistrationEnvelope(address, info)));
            } catch (Exception e) {
                if (!keepPolling(registrationQueryable)) {
                    break;
                }
                logger.debug("Registration query poll error: {}", e.getMessage());
            }
        }
    }

    private boolean keepPolling(Queryable queryable) {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        return queryable != null && !queryable.isUndeclared();
    }

    /** Visits every registration this node owns, as an address plus its own {@link RegistrationInfo}. */
    private void forEachOwnRegistration(BiConsumer<String, RegistrationInfo> action) {
        for (Map.Entry<String, Map<String, List<RegistrationInfo>>> entry : registrations.entrySet()) {
            List<RegistrationInfo> own = entry.getValue().get(nodeId);
            if (own != null && !own.isEmpty()) {
                action.accept(entry.getKey(), own.get(0));
            }
        }
    }

    /**
     * Notifies the selector that {@code address} now has a different set of registrations.
     */
    private void fireRegistrationsUpdated(String address) {
        RegistrationListener listener = this.registrationListener;
        if (listener == null || !listener.wantsUpdatesFor(address)) {
            return;
        }
        try {
            listener.registrationsUpdated(new RegistrationUpdateEvent(address, registrationsOf(address)));
        } catch (Exception e) {
            logger.debug("registrationsUpdated({}) failed: {}", address, e.getMessage());
        }
    }

    // --- Codec helpers ---

    private static byte[] encode(RegistrationInfo info) {
        Buffer buffer = Buffer.buffer();
        info.writeToBuffer(buffer);
        return buffer.getBytes();
    }

    /**
     * Wraps a registration with its address: {@code [int addressLen][address][RegistrationInfo]}.
     * <p>
     * The Zenoh key cannot carry the address (see {@link #registrationOwnerOf}), and a delete
     * sample arrives with no payload at all, so the address must be recoverable from the put that
     * preceded it. Keeping it in the envelope makes the replica independent of key parsing.
     */
    private static byte[] encodeRegistrationEnvelope(String address, RegistrationInfo info) {
        byte[] addressBytes = address.getBytes(StandardCharsets.UTF_8);
        byte[] infoBytes = encode(info);
        Buffer buffer = Buffer.buffer();
        buffer.appendInt(addressBytes.length).appendBytes(addressBytes).appendBytes(infoBytes);
        return buffer.getBytes();
    }

    private static String decodeEnvelopeAddress(byte[] payload) {
        if (payload == null || payload.length < 4) {
            return null;
        }
        Buffer buffer = Buffer.buffer(payload);
        try {
            int length = buffer.getInt(0);
            if (length < 0 || length > payload.length - 4) {
                return null;
            }
            return buffer.getString(4, 4 + length);
        } catch (Exception e) {
            return null;
        }
    }

    private static RegistrationInfo decodeEnvelopeInfo(byte[] payload) {
        if (payload == null || payload.length < 4) {
            return null;
        }
        Buffer buffer = Buffer.buffer(payload);
        try {
            int length = buffer.getInt(0);
            int offset = 4 + length;
            if (length < 0 || offset > payload.length) {
                return null;
            }
            RegistrationInfo info = new RegistrationInfo();
            info.readFromBuffer(offset, buffer);
            return info;
        } catch (Exception e) {
            logger.debug("Failed to decode registration envelope: {}", e.getMessage());
            return null;
        }
    }

    private static byte[] encode(NodeInfo info) {
        Buffer buffer = Buffer.buffer();
        info.writeToBuffer(buffer);
        return buffer.getBytes();
    }

    private static NodeInfo decodeNodeInfo(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            NodeInfo info = new NodeInfo();
            info.readFromBuffer(0, Buffer.buffer(payload));
            return info;
        } catch (Exception e) {
            logger.debug("Failed to decode NodeInfo: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extracts the owning nodeId from a {@code registrationKey}.
     * <p>
     * The address cannot be recovered from the key because {@link KeyPrefixStrategy#sanitize} is
     * lossy ({@code /} and {@code []} all collapse to {@code _}), and two distinct addresses may
     * therefore share a key prefix. The address travels inside the payload envelope instead, which
     * is what {@link #applyRegistrationSample} actually uses.
     */
    private String registrationOwnerOf(String keyExpr) {
        return lastSegment(keyExpr);
    }

    private String lastSegment(String keyExpr) {
        if (keyExpr == null) {
            return null;
        }
        int slash = keyExpr.lastIndexOf('/');
        return slash >= 0 && slash < keyExpr.length() - 1 ? keyExpr.substring(slash + 1) : null;
    }

    private Thread startPoll(Runnable body, String namePrefix) {
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (Exception e) {
                if (running.get()) {
                    logger.debug("{} thread terminated: {}", namePrefix, e.getMessage());
                }
            }
        }, namePrefix + "-" + nodeId.substring(0, 8));
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void undeclare(Subscriber subscriber) {
        if (subscriber == null) {
            return;
        }
        try {
            subscriber.undeclare();
        } catch (Exception e) {
            logger.debug("Failed to undeclare subscriber: {}", e.getMessage());
        }
    }

    private static void undeclare(Queryable queryable) {
        if (queryable == null) {
            return;
        }
        try {
            queryable.undeclare();
        } catch (Exception e) {
            logger.debug("Failed to undeclare queryable: {}", e.getMessage());
        }
    }

    private static void join(Thread thread) {
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
}
