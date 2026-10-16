package com.workoss.boot.zenoh.cluster;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.EmbeddedRouter;
import com.workoss.zenoh.Query;
import com.workoss.zenoh.Queryable;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Sample;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pure Spring Boot cluster manager backed by Zenoh JNI — no Vert.x anywhere.
 * <p>
 * Provides the four cluster primitives:
 * <ul>
 *   <li><b>Cluster management</b> — liveliness heartbeat, node discovery, membership events
 *       ({@link MembershipListener}), peer metadata</li>
 *   <li><b>Broadcast messaging</b> — {@link ZenohMessageBroker#broadcast(String, byte[])} to every
 *       node subscribed to an address</li>
 *   <li><b>Point-to-point messaging</b> — {@link ZenohMessageBroker#sendToNode(String, byte[])}
 *       into a specific node's inbox</li>
 *   <li><b>Shared map</b> — {@link #getSharedMap(String)} plus {@link #getCounter(String)} and
 *       {@link #tryLock(String, long)}, all replicated across nodes</li>
 * </ul>
 * Lifecycle follows Spring's {@link SmartLifecycle}: the node joins the cluster when the
 * application context finishes refreshing and leaves when it closes. The same instance can be
 * driven manually in tests via {@link #start()} / {@link #stop()}.
 * <p>
 * Key prefix scheme: {@code zenoh/{app-name}/{environment}/...}. Different apps/environments
 * sharing the same Zenoh network are completely isolated, and the keyspace cannot collide with the
 * {@code vertx/{app}/{env}/...} space of {@code vertx-spring-boot-cluster-zenoh}.
 */
public class ZenohClusterManager implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ZenohClusterManager.class);

    private final ZenohClusterProperties properties;
    private final KeyPrefixStrategy keyPrefix;
    private final String nodeId;
    private final NodeMetadata ownMetadata;

    private final ScheduledExecutorService scheduler;
    private final ExecutorService ioExecutor;
    private final ZenohMapStore store;
    private final ZenohMessageBroker messageBroker;

    private final List<String> knownNodes = new CopyOnWriteArrayList<>();
    private final Map<String, NodeMetadata> peerMetadata = new ConcurrentHashMap<>();
    private final List<MembershipListener> membershipListeners = new CopyOnWriteArrayList<>();
    private final Map<String, ZenohSharedMap<?, ?>> sharedMaps = new ConcurrentHashMap<>();
    private final Map<String, ZenohClusterCounter> counters = new ConcurrentHashMap<>();

    private volatile Session session;
    private volatile EmbeddedRouter embeddedRouter;
    private volatile boolean running = false;

    private Subscriber livelinessSubscriber;
    private Subscriber nodeInfoSubscriber;
    private Queryable nodesQueryable;
    private Queryable nodeInfoQueryable;
    private Thread livelinessPollThread;
    private Thread nodeInfoPollThread;
    private Thread nodesQueryPollThread;
    private Thread nodeInfoQueryPollThread;
    private ScheduledFuture<?> heartbeatFuture;
    private final AtomicBoolean nodesQueryableRunning = new AtomicBoolean(false);
    private final AtomicBoolean nodeInfoQueryableRunning = new AtomicBoolean(false);

    public ZenohClusterManager(ZenohClusterProperties properties) {
        this.properties = properties;
        this.keyPrefix = new KeyPrefixStrategy(properties.getAppName(), properties.getEnvironment());
        this.nodeId = properties.getNodeId() != null && !properties.getNodeId().isEmpty()
                ? properties.getNodeId()
                : UUID.randomUUID().toString();
        this.ownMetadata = new NodeMetadata(nodeId, System.currentTimeMillis(), hostName());
        this.scheduler = singleThreadScheduler("zenoh-cluster-scheduler-" + shortId());
        // Single-threaded: replication puts for the same key must reach peers in the order they
        // were issued, otherwise a node applying put(5) before put(4) converges to the wrong value.
        // This matches the reference implementation, which funnelled every put through
        // vertx.executeBlocking (ordered per context).
        this.ioExecutor = Executors.newSingleThreadExecutor(daemonFactory("zenoh-cluster-io-" + shortId()));
        this.store = new ZenohMapStore(keyPrefix, nodeId, scheduler, ioExecutor);
        this.messageBroker = new ZenohMessageBroker(this);
    }

    // --- Spring lifecycle ---

    @Override
    public void start() {
        logger.info("Joining Zenoh cluster with nodeId={}, app={}, env={}",
                nodeId, properties.getAppName(), properties.getEnvironment());
        try {
            openSession();
            store.start(session);

            // Bind the message broker before discovery: a peer may dial our inbox as soon as it
            // sees our liveliness key.
            messageBroker.bind(session);

            // Mark the manager running *before* the poll/query threads are started: every polling
            // loop (liveliness, node-info) gates on this flag, so setting it here is what keeps
            // those threads alive. The queryables use their own flags, but they are started below
            // too.
            running = true;

            // Declared before the heartbeat so a peer joining concurrently can discover us by
            // querying, not only by waiting for our next periodic put.
            startNodesQueryable();
            startNodeInfoQueryable();
            startLivelinessHeartbeat();
            subscribeLivelinessChanges();
            subscribeNodeInfo();

            // Discover nodes that were already alive before we subscribed
            discoverExistingNodes();
            discoverExistingNodeInfos();

            knownNodes.add(nodeId);
            logger.info("Successfully joined Zenoh cluster, zid={}", session.zid());
        } catch (Exception e) {
            logger.error("Failed to join Zenoh cluster", e);
            throw new IllegalStateException("Failed to join Zenoh cluster", e);
        }
    }

    @Override
    public void stop() {
        logger.info("Leaving Zenoh cluster, nodeId={}", nodeId);
        try {
            running = false;
            messageBroker.unbind();
            messageBroker.shutdown();
            stopLivelinessHeartbeat();
            store.stop();
            stopNodesQueryable();
            stopNodeInfoQueryable();
            closeSession();
            knownNodes.clear();
            peerMetadata.clear();
        } catch (Exception e) {
            logger.error("Failed to leave Zenoh cluster gracefully", e);
        } finally {
            shutdownExecutors();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    // --- Cluster management API ---

    public String getNodeId() {
        return nodeId;
    }

    public NodeMetadata getOwnMetadata() {
        return ownMetadata;
    }

    /**
     * @return this node's id followed by every peer currently considered alive
     */
    public List<String> getNodes() {
        return List.copyOf(knownNodes);
    }

    public int getNodeCount() {
        return knownNodes.size();
    }

    public boolean isActive() {
        return running && session != null;
    }

    /**
     * @return the peer's metadata, or {@code null} when it has not been learned yet
     */
    public NodeMetadata getPeerMetadata(String nodeId) {
        return peerMetadata.get(nodeId);
    }

    public void addMembershipListener(MembershipListener listener) {
        membershipListeners.add(listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        membershipListeners.remove(listener);
    }

    // --- Data structure accessors ---

    /**
     * Get (or lazily create) the shared map with the given name. The same instance is returned for
     * the same name, and it keeps working across stop/start cycles of this manager.
     */
    @SuppressWarnings("unchecked")
    public <K, V> ZenohSharedMap<K, V> getSharedMap(String name) {
        return (ZenohSharedMap<K, V>) sharedMaps.computeIfAbsent(name,
                n -> new ZenohSharedMap<>(n, keyPrefix, store, ValueCodec.jdk()));
    }

    /**
     * Get (or lazily create) the shared map with the given name and value codec. A map already
     * created under {@code name} with the default codec is returned as-is; the codec only applies
     * on first creation.
     */
    @SuppressWarnings("unchecked")
    public <K, V> ZenohSharedMap<K, V> getSharedMap(String name, ValueCodec<V> valueCodec) {
        return (ZenohSharedMap<K, V>) sharedMaps.computeIfAbsent(name,
                n -> new ZenohSharedMap<>(n, keyPrefix, store, valueCodec));
    }

    /**
     * Get (or lazily create) the cluster-wide counter with the given name.
     */
    public ZenohClusterCounter getCounter(String name) {
        return counters.computeIfAbsent(name, n -> new ZenohClusterCounter(n, keyPrefix, store));
    }

    /**
     * Try to acquire the named lock, waiting up to {@code timeoutMs}. Best-effort mutual exclusion
     * — see {@link ZenohClusterLock} for the consistency caveat.
     */
    public ZenohClusterLock tryLock(String name, long timeoutMs) {
        return ZenohClusterLock.tryAcquire(name, nodeId, keyPrefix, store, timeoutMs);
    }

    // --- Messaging ---

    public ZenohMessageBroker getMessageBroker() {
        return messageBroker;
    }

    // --- Advanced access ---

    public KeyPrefixStrategy getKeyPrefix() {
        return keyPrefix;
    }

    /**
     * The underlying Zenoh session, or {@code null} while the cluster is not joined.
     */
    public Session getSession() {
        return session;
    }

    // --- Session management ---

    private void openSession() {
        logger.info("Opening Zenoh session with mode={}, connect={}, listen={}",
                properties.getMode(), properties.getConnect(), properties.getListen());

        if ("router".equalsIgnoreCase(properties.getMode())) {
            openRouterSession();
        } else {
            openRegularSession();
        }
    }

    /**
     * Start an EmbeddedRouter and connect a client Session to it.
     * <p>
     * The router carries no plugins: cluster state is replicated in process by {@link ZenohMapStore}
     * and exchanged over plain pub/sub and queryables, both of which the router forwards.
     */
    private void openRouterSession() {
        List<String> listenEndpoints = properties.getListen();
        if (listenEndpoints == null || listenEndpoints.isEmpty()) {
            listenEndpoints = List.of("tcp/[::]:7447");
        }

        Config routerConfig = Config.defaultConfig();
        routerConfig.setMode("router");
        routerConfig.setListenEndpoints(listenEndpoints);

        this.embeddedRouter = new EmbeddedRouter(routerConfig);
        logger.info("EmbeddedRouter started, zid={}", embeddedRouter.zid());

        Config clientConfig = Config.defaultConfig();
        clientConfig.setMode("client");
        clientConfig.setConnectEndpoints(List.copyOf(listenEndpoints));

        this.session = new Session(clientConfig);
        clientConfig.close();
        logger.info("Client session connected to EmbeddedRouter successfully");
    }

    /**
     * Open a Session directly, for peer and client modes where no local router is needed.
     */
    private void openRegularSession() {
        Config config;
        if (properties.getConfigFile() != null && !properties.getConfigFile().isEmpty()) {
            config = new Config(properties.getConfigFile());
        } else {
            config = Config.defaultConfig();
        }

        String mode = properties.getMode();
        if (mode != null && !mode.isEmpty()) {
            config.setMode(mode);
        }

        List<String> connectEndpoints = properties.getConnect();
        if (connectEndpoints != null && !connectEndpoints.isEmpty()) {
            config.setConnectEndpoints(connectEndpoints);
        }

        List<String> listenEndpoints = properties.getListen();
        if (listenEndpoints != null && !listenEndpoints.isEmpty()) {
            config.setListenEndpoints(listenEndpoints);
        }

        this.session = new Session(config);
        config.close();
        logger.info("Zenoh session opened successfully");
    }

    private void closeSession() {
        logger.info("Closing Zenoh session");
        Subscriber liveliness = this.livelinessSubscriber;
        this.livelinessSubscriber = null;
        if (liveliness != null) {
            try {
                liveliness.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare liveliness subscriber");
            }
        }
        joinThread(livelinessPollThread);
        livelinessPollThread = null;

        Subscriber nodeInfo = this.nodeInfoSubscriber;
        this.nodeInfoSubscriber = null;
        if (nodeInfo != null) {
            try {
                nodeInfo.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare node info subscriber");
            }
        }
        joinThread(nodeInfoPollThread);
        nodeInfoPollThread = null;

        Session current = session;
        session = null;
        if (current != null) {
            try {
                current.closeSession();
            } catch (Exception e) {
                logger.debug("Failed to close Zenoh session");
            }
        }
        if (embeddedRouter != null) {
            try {
                embeddedRouter.closeRouter();
            } catch (Exception e) {
                logger.debug("Failed to close EmbeddedRouter");
            }
            embeddedRouter = null;
        }
    }

    // --- Liveliness heartbeat ---

    /**
     * Refresh this node's liveliness key and re-publish its metadata on a fixed cadence.
     * <p>
     * The re-publish is what makes late joiners converge: a Zenoh {@code put} without a storage
     * manager is a transient notification, so a peer that starts afterwards can only learn our
     * metadata from a write it actually sees — either its own bootstrap query (answered by the
     * node-info queryable) or the next heartbeat round.
     */
    private void startLivelinessHeartbeat() {
        String key = keyPrefix.nodeKey(nodeId);
        byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        long interval = Math.max(1000L, properties.getHeartbeatInterval());

        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            if (!running || session == null) {
                return;
            }
            try {
                session.put(key, nodeIdBytes);
                session.put(keyPrefix.nodeInfoKey(nodeId), NodeMetadata.encode(ownMetadata));
            } catch (Exception e) {
                logger.debug("Heartbeat put failed: {}", e.getMessage());
            }
        }, 0L, interval, TimeUnit.MILLISECONDS);

        logger.info("Liveliness heartbeat started for node: {} (interval={}ms)", nodeId, interval);
    }

    private void stopLivelinessHeartbeat() {
        ScheduledFuture<?> future = heartbeatFuture;
        heartbeatFuture = null;
        if (future != null) {
            future.cancel(false);
        }
        Session current = session;
        if (current != null) {
            try {
                current.delete(keyPrefix.nodeKey(nodeId));
                current.delete(keyPrefix.nodeInfoKey(nodeId));
            } catch (Exception e) {
                logger.debug("Failed to delete liveliness keys on leave");
            }
        }
    }

    private void subscribeLivelinessChanges() {
        String pattern = keyPrefix.allNodesPattern();
        logger.debug("Subscribing to liveliness changes at pattern: {}", pattern);

        this.livelinessSubscriber = new Subscriber(session, pattern);
        this.livelinessPollThread = startPollThread(() -> {
            while (running && livelinessSubscriber != null && !livelinessSubscriber.isUndeclared()) {
                try {
                    Optional<Sample> sampleOpt = livelinessSubscriber.tryRecv();
                    if (sampleOpt.isPresent()) {
                        applyLivelinessSample(sampleOpt.get());
                    } else {
                        Thread.sleep(100); // No data available, sleep briefly
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.debug("Liveliness poll error: {}", e.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }, "zenoh-liveliness-poll");
        logger.info("Liveliness subscriber started for pattern: {}", pattern);
    }

    private void applyLivelinessSample(Sample sample) {
        String detectedNodeId = extractNodeId(sample.keyExpr());
        if (detectedNodeId == null || detectedNodeId.equals(nodeId)) {
            return;
        }
        // kind: 0=PUT, 1=DELETE (from zenoh SampleKind)
        if (sample.kind() == 0) {
            if (!knownNodes.contains(detectedNodeId)) {
                knownNodes.add(detectedNodeId);
                logger.info("Node joined: {}", detectedNodeId);
                fireNodeJoined(detectedNodeId);
                // The newcomer may have published its metadata before this node's subscriber
                // existed; the queryable below picks it up immediately instead of waiting for the
                // next heartbeat round.
                queryPeerMetadata(detectedNodeId);
            }
        } else if (sample.kind() == 1) {
            if (knownNodes.remove(detectedNodeId)) {
                logger.info("Node left: {}", detectedNodeId);
                peerMetadata.remove(detectedNodeId);
                fireNodeLeft(detectedNodeId);
            }
        }
    }

    /**
     * Answers {@code session.get()} on the nodes keyspace with this node's own liveliness key.
     * <p>
     * Each live node asserts only itself, so the replies a joining node collects are exactly the
     * set of nodes that were up at query time.
     */
    private void startNodesQueryable() {
        String pattern = keyPrefix.allNodesPattern();
        String ownKey = keyPrefix.nodeKey(nodeId);
        byte[] ownPayload = nodeId.getBytes(StandardCharsets.UTF_8);

        this.nodesQueryable = new Queryable(session, pattern);
        nodesQueryableRunning.set(true);

        this.nodesQueryPollThread = startPollThread(() -> {
            while (nodesQueryableRunning.get() && nodesQueryable != null && !nodesQueryable.isUndeclared()) {
                Queryable current = nodesQueryable;
                if (current == null) {
                    break;
                }
                // try-with-resources matters: dropping the query without replying is what tells the
                // requesting node that no more replies are coming.
                try (Query query = new Query(current)) {
                    String selector = query.getKeyExpr();
                    if (selector.endsWith("*") || selector.endsWith("/**") || selector.equals(ownKey)) {
                        query.reply(ownKey, ownPayload);
                    }
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    if (nodesQueryable == null || nodesQueryable.isUndeclared()) {
                        break;
                    }
                    logger.debug("Node queryable poll error: {}", e.getMessage());
                }
            }
        }, "zenoh-nodes-query");
        logger.info("Node queryable started for pattern: {}", pattern);
    }

    private void stopNodesQueryable() {
        nodesQueryableRunning.set(false);
        Queryable q = this.nodesQueryable;
        this.nodesQueryable = null;
        if (q != null) {
            try {
                q.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare node queryable");
            }
        }
        joinThread(nodesQueryPollThread);
        nodesQueryPollThread = null;
    }

    /**
     * Discover nodes that were already alive before this node subscribed.
     * Each peer answers from its own node queryable.
     */
    private void discoverExistingNodes() {
        try {
            List<Reply> replies = session.get(keyPrefix.allNodesPattern());
            int discoveredCount = 0;
            for (Reply reply : replies) {
                if (reply.success() && reply.sampleKeyExpr().isPresent()) {
                    String detectedNodeId = extractNodeId(reply.sampleKeyExpr().get());
                    if (detectedNodeId != null && !detectedNodeId.equals(nodeId)
                            && !knownNodes.contains(detectedNodeId)) {
                        knownNodes.add(detectedNodeId);
                        logger.info("Discovered existing node: {}", detectedNodeId);
                        fireNodeJoined(detectedNodeId);
                        discoveredCount++;
                    }
                }
            }
            if (discoveredCount > 0) {
                logger.info("Discovered {} existing node(s)", discoveredCount);
            }
        } catch (Exception e) {
            logger.warn("Failed to discover existing nodes", e);
        }
    }

    // --- Node metadata ---

    private void subscribeNodeInfo() {
        String pattern = keyPrefix.allNodeInfoPattern();
        this.nodeInfoSubscriber = new Subscriber(session, pattern);
        this.nodeInfoPollThread = startPollThread(() -> {
            while (running && nodeInfoSubscriber != null && !nodeInfoSubscriber.isUndeclared()) {
                try {
                    Optional<Sample> sampleOpt = nodeInfoSubscriber.tryRecv();
                    if (sampleOpt.isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }
                    Sample sample = sampleOpt.get();
                    String owner = extractNodeId(sample.keyExpr());
                    if (owner == null || owner.equals(nodeId)) {
                        continue;
                    }
                    if (sample.kind() == 1) {
                        peerMetadata.remove(owner);
                        continue;
                    }
                    NodeMetadata metadata = NodeMetadata.decode(sample.payload());
                    if (metadata != null) {
                        peerMetadata.put(owner, metadata);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.debug("Node info poll error: {}", e.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }, "zenoh-nodeinfo-poll");
        logger.info("Node info subscriber started for pattern: {}", pattern);
    }

    /**
     * Answer {@code session.get()} on the node-info keyspace with this node's metadata, so a
     * joining peer resolves us without waiting for the next heartbeat.
     */
    private void startNodeInfoQueryable() {
        String pattern = keyPrefix.allNodeInfoPattern();
        String ownKey = keyPrefix.nodeInfoKey(nodeId);
        byte[] ownPayload = NodeMetadata.encode(ownMetadata);

        this.nodeInfoQueryable = new Queryable(session, pattern);
        nodeInfoQueryableRunning.set(true);

        this.nodeInfoQueryPollThread = startPollThread(() -> {
            while (nodeInfoQueryableRunning.get() && nodeInfoQueryable != null && !nodeInfoQueryable.isUndeclared()) {
                Queryable current = nodeInfoQueryable;
                if (current == null) {
                    break;
                }
                try (Query query = new Query(current)) {
                    String selector = query.getKeyExpr();
                    if (selector.endsWith("/**") || selector.equals(ownKey)) {
                        query.reply(ownKey, ownPayload);
                    }
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    if (nodeInfoQueryable == null || nodeInfoQueryable.isUndeclared()) {
                        break;
                    }
                    logger.debug("Node info queryable poll error: {}", e.getMessage());
                }
            }
        }, "zenoh-nodeinfo-query");
        logger.info("Node info queryable started for pattern: {}", pattern);
    }

    private void stopNodeInfoQueryable() {
        nodeInfoQueryableRunning.set(false);
        Queryable q = this.nodeInfoQueryable;
        this.nodeInfoQueryable = null;
        if (q != null) {
            try {
                q.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare node info queryable");
            }
        }
        joinThread(nodeInfoQueryPollThread);
        nodeInfoQueryPollThread = null;
    }

    private void discoverExistingNodeInfos() {
        Session current = session;
        if (current == null) {
            return;
        }
        try {
            for (Reply reply : current.get(keyPrefix.allNodeInfoPattern())) {
                if (!reply.success() || reply.sampleKeyExpr().isEmpty()) {
                    continue;
                }
                String owner = extractNodeId(reply.sampleKeyExpr().get());
                if (owner == null || owner.equals(nodeId)) {
                    continue;
                }
                NodeMetadata metadata = NodeMetadata.decode(reply.samplePayload().orElse(null));
                if (metadata != null) {
                    peerMetadata.put(owner, metadata);
                }
            }
        } catch (Exception e) {
            logger.debug("Node info discovery failed: {}", e.getMessage());
        }
    }

    /**
     * Ask a freshly seen peer for its metadata right away instead of waiting for its next
     * heartbeat. Best-effort: the peer answers from its node-info queryable.
     */
    private void queryPeerMetadata(String peerNodeId) {
        Session current = session;
        if (current == null) {
            return;
        }
        try {
            for (Reply reply : current.get(keyPrefix.nodeInfoKey(peerNodeId))) {
                if (!reply.success()) {
                    continue;
                }
                NodeMetadata metadata = NodeMetadata.decode(reply.samplePayload().orElse(null));
                if (metadata != null) {
                    peerMetadata.put(peerNodeId, metadata);
                }
            }
        } catch (Exception e) {
            logger.debug("Peer metadata query failed for {}: {}", peerNodeId, e.getMessage());
        }
    }

    // --- Membership events ---

    private void fireNodeJoined(String joinedNodeId) {
        for (MembershipListener listener : membershipListeners) {
            try {
                listener.nodeJoined(joinedNodeId);
            } catch (Exception e) {
                logger.debug("nodeJoined({}) listener failed: {}", joinedNodeId, e.getMessage());
            }
        }
    }

    private void fireNodeLeft(String departedNodeId) {
        for (MembershipListener listener : membershipListeners) {
            try {
                listener.nodeLeft(departedNodeId);
            } catch (Exception e) {
                logger.debug("nodeLeft({}) listener failed: {}", departedNodeId, e.getMessage());
            }
        }
    }

    // --- Helpers ---

    private String extractNodeId(String keyExpr) {
        if (keyExpr == null) {
            return null;
        }
        int lastSlash = keyExpr.lastIndexOf('/');
        return lastSlash >= 0 && lastSlash < keyExpr.length() - 1 ? keyExpr.substring(lastSlash + 1) : null;
    }

    private String shortId() {
        return nodeId.length() >= 8 ? nodeId.substring(0, 8) : nodeId;
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private Thread startPollThread(Runnable loop, String namePrefix) {
        Thread thread = new Thread(loop, namePrefix + "-" + shortId());
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

    private void shutdownExecutors() {
        scheduler.shutdownNow();
        ioExecutor.shutdownNow();
    }

    private static ScheduledExecutorService singleThreadScheduler(String name) {
        return Executors.newSingleThreadScheduledExecutor(daemonFactory(name));
    }

    private static ThreadFactory daemonFactory(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }
}
