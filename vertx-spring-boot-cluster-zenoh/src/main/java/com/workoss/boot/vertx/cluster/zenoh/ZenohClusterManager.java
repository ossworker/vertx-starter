package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.EmbeddedRouter;
import com.workoss.zenoh.Query;
import com.workoss.zenoh.Queryable;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import io.vertx.core.Completable;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Vert.x 5 ClusterManager implementation backed by Zenoh JNI.
 * Supports multi-tenant isolation through key prefix strategy.
 * <p>
 * Cluster state (shared maps, locks, counters, node liveness) is replicated in process by
 * {@link ZenohMapStore} and served to peers over a Zenoh queryable, so no storage-manager plugin is
 * loaded and no plugin ABI has to match the JNI runtime.
 *
 * Key prefix scheme: vertx/{app-name}/{environment}/cluster/...
 * Different apps/environments sharing the same Zenoh network are completely isolated.
 */
public class ZenohClusterManager implements ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ZenohClusterManager.class);

    private final ZenohClusterProperties properties;
    private final KeyPrefixStrategy keyPrefix;

    private Vertx vertx;
    private final String nodeId;
    private NodeListener nodeListener;
    private RegistrationListener registrationListener;
    private NodeInfo nodeInfo;

    private Session zenohSession;
    private EmbeddedRouter embeddedRouter;
    private Subscriber livelinessSubscriber;
    private Thread livelinessPollThread;
    private Queryable nodesQueryable;
    private Thread nodesQueryPollThread;
    private final AtomicBoolean nodesQueryableRunning = new AtomicBoolean(false);
    private Long heartbeatTimerId;
    private final AtomicBoolean livelinessRunning = new AtomicBoolean(false);

    // Replicated in-process store backing shared maps, locks and counters
    private ZenohMapStore mapStore;

    private ClusterRegistrationTable registrationTable;

    private final List<String> knownNodes = new CopyOnWriteArrayList<>();

    private volatile boolean active = false;

    public ZenohClusterManager(ZenohClusterProperties properties) {
        this.properties = properties;
        this.keyPrefix = new KeyPrefixStrategy(properties.getAppName(), properties.getEnvironment());
        this.nodeId = UUID.randomUUID().toString();
        // Created eagerly: VertxImpl.initClustered calls registrationListener() before init(),
        // so anything the SPI may touch has to exist from construction onwards.
        this.registrationTable = new ClusterRegistrationTable(keyPrefix, nodeId);
        if (registrationListener != null) {
            this.registrationTable.setRegistrationListener(registrationListener);
        }
    }

    @Override
    public void init(Vertx vertx) {
        this.vertx = vertx;
        this.mapStore = new ZenohMapStore(vertx, keyPrefix, nodeId);
    }

    @Override
    public <K, V> void getAsyncMap(String name, Completable<AsyncMap<K, V>> completable) {
        completable.succeed(new ZenohAsyncMap<>(name, keyPrefix, mapStore));
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        ZenohAsyncMap<K, V> asyncMap = new ZenohAsyncMap<>(name, keyPrefix, mapStore);
        return asyncMap.entries().await();
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Completable<Lock> completable) {
        vertx.<ZenohLock>executeBlocking(() ->
                ZenohLock.tryAcquireWithTimeout(name, nodeId, keyPrefix, mapStore, timeout)
        ).onComplete(ar -> {
            if (ar.failed()) {
                completable.fail(ar.cause());
            } else if (ar.result().isHeld()) {
                completable.succeed(ar.result());
            } else {
                completable.fail("Failed to acquire lock '" + name + "' within " + timeout + "ms");
            }
        });
    }

    @Override
    public void getCounter(String name, Completable<Counter> completable) {
        completable.succeed(new ZenohCounter(name, keyPrefix, mapStore));
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public List<String> getNodes() {
        return List.copyOf(knownNodes);
    }

    @Override
    public void nodeListener(NodeListener listener) {
        this.nodeListener = listener;
    }

    @Override
    public void registrationListener(RegistrationListener listener) {
        this.registrationListener = listener;
        // Vert.x calls this before init(), so the table must already exist — hence it is built in
        // the constructor rather than here.
        this.registrationTable.setRegistrationListener(listener);
    }

    @Override
    public void setNodeInfo(NodeInfo nodeInfo, Completable<Void> completable) {
        this.nodeInfo = nodeInfo;
        // Publishes to Zenoh and caches locally; peers resolve it back via getNodeInfo(nodeId).
        this.registrationTable.setLocalNodeInfo(nodeInfo);
        completable.succeed();
    }

    @Override
    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    @Override
    public void getNodeInfo(String nodeId, Completable<NodeInfo> completable) {
        NodeInfo info = registrationTable.nodeInfoOf(nodeId);
        if (info != null) {
            completable.succeed(info);
        } else {
            completable.fail("Node info not found for: " + nodeId);
        }
    }

    @Override
    public void join(Completable<Void> completable) {
        logger.info("Joining Zenoh cluster with nodeId={}, app={}, env={}",
                nodeId, properties.getAppName(), properties.getEnvironment());

        try {
            openZenohSession();
            mapStore.start(zenohSession);

            // Before discovery: a peer that already knows about us may dial back as soon as it sees
            // our liveliness key, and connect() resolves our NodeInfo through this table.
            registrationTable.start(zenohSession);

            // Declared before the heartbeat so a peer joining concurrently can discover us by
            // querying, not only by waiting for our next periodic put.
            startNodeQueryable();
            startLivelinessHeartbeat();
            subscribeLivelinessChanges();

            // Discover nodes that were already alive before we subscribed
            discoverExistingNodes();
            registrationTable.discoverExistingNodeInfos();

            knownNodes.add(nodeId);
            active = true;
            logger.info("Successfully joined Zenoh cluster, zid={}", zenohSession.zid());
            completable.succeed();
        } catch (Exception e) {
            logger.error("Failed to join Zenoh cluster", e);
            completable.fail(e);
        }
    }

    @Override
    public void leave(Completable<Void> completable) {
        logger.info("Leaving Zenoh cluster, nodeId={}", nodeId);
        try {
            if (registrationTable != null) {
                registrationTable.stop();
            }
            mapStore.stop();
            stopNodeQueryable();
            stopLivelinessHeartbeat();
            closeZenohSession();
            knownNodes.remove(nodeId);
            active = false;
            completable.succeed();
        } catch (Exception e) {
            logger.error("Failed to leave Zenoh cluster gracefully", e);
            completable.fail(e);
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void addRegistration(String address, RegistrationInfo registrationInfo, Completable<Void> completable) {
        logger.debug("Adding registration for address={}, nodeId={}", address, registrationInfo.nodeId());
        registrationTable.addRegistration(address, registrationInfo);
        completable.succeed();
    }

    @Override
    public void removeRegistration(String address, RegistrationInfo registrationInfo, Completable<Void> completable) {
        logger.debug("Removing registration for address={}, nodeId={}", address, registrationInfo.nodeId());
        registrationTable.removeRegistration(address, registrationInfo);
        completable.succeed();
    }

    @Override
    public void getRegistrations(String address, Completable<List<RegistrationInfo>> completable) {
        // Must span the whole cluster: DefaultNodeSelector turns this into publish targets, so a
        // local-only answer silently confines every clustered message to this JVM.
        completable.succeed(registrationTable.registrationsOf(address));
    }

    public KeyPrefixStrategy getKeyPrefix() {
        return keyPrefix;
    }

    /**
     * Get the underlying Zenoh session (for advanced use).
     */
    public Session getZenohSession() {
        return zenohSession;
    }

    /**
     * Synchronous view of {@link #getRegistrations(String, Completable)} for diagnostics and tests.
     * The SPI stays callback-based; nothing on the Vert.x hot path should use this.
     */
    public List<RegistrationInfo> getRegistrationsOf(String address) {
        return registrationTable.registrationsOf(address);
    }

    /**
     * Synchronous view of {@link #getNodeInfo(String, Completable)} for diagnostics and tests.
     *
     * @return the peer's info, or {@code null} when it has not been learned yet
     */
    public NodeInfo getNodeInfoOf(String nodeId) {
        return registrationTable.nodeInfoOf(nodeId);
    }

    // --- Private Zenoh interaction methods ---

    private void openZenohSession() {
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

        this.zenohSession = new Session(clientConfig);
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

        // Set mode
        String mode = properties.getMode();
        if (mode != null && !mode.isEmpty()) {
            config.setMode(mode);
        }

        // Set connect endpoints
        List<String> connectEndpoints = properties.getConnect();
        if (connectEndpoints != null && !connectEndpoints.isEmpty()) {
            config.setConnectEndpoints(connectEndpoints);
        }

        // Set listen endpoints
        List<String> listenEndpoints = properties.getListen();
        if (listenEndpoints != null && !listenEndpoints.isEmpty()) {
            config.setListenEndpoints(listenEndpoints);
        }

        this.zenohSession = new Session(config);
        config.close();
        logger.info("Zenoh session opened successfully");
    }

    /**
     * Start a heartbeat that periodically puts this node's liveliness key.
     * Uses vertx.setPeriodic() with executeBlocking() to avoid blocking the event loop.
     */
    private void startLivelinessHeartbeat() {
        String key = keyPrefix.nodeKey(nodeId);
        byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        logger.debug("Starting liveliness heartbeat at key: {}", key);

        // Initial put (on calling thread, which is a Vert.x worker thread during join)
        try {
            zenohSession.put(key, nodeIdBytes);
        } catch (Exception e) {
            logger.warn("Failed initial liveliness put", e);
        }

        livelinessRunning.set(true);
        // Refresh heartbeat every 5 seconds via Vert.x timer, offloaded to worker thread
        heartbeatTimerId = vertx.setPeriodic(5000, id -> {
            if (!livelinessRunning.get()) return;
            vertx.<Void>executeBlocking(() -> {
                try {
                    zenohSession.put(key, nodeIdBytes);
                } catch (Exception e) {
                    logger.debug("Heartbeat put failed: {}", e.getMessage());
                }
                // A put without a storage manager is a transient notification, not stored state, so
                // anything a late joiner must see has to be rewritten on the same cadence.
                registrationTable.republishLocalNodeInfo();
                registrationTable.republishLocalRegistrations();
                return null;
            });
        });

        logger.info("Liveliness heartbeat started for node: {}", nodeId);
    }

    private void stopLivelinessHeartbeat() {
        livelinessRunning.set(false);
        if (heartbeatTimerId != null) {
            vertx.cancelTimer(heartbeatTimerId);
            heartbeatTimerId = null;
        }
        // Delete liveliness key
        if (zenohSession != null) {
            try {
                zenohSession.delete(keyPrefix.nodeKey(nodeId));
            } catch (Exception e) {
                logger.debug("Failed to delete liveliness key on leave");
            }
        }
    }

    private void subscribeLivelinessChanges() {
        String pattern = keyPrefix.allNodesPattern();
        logger.debug("Subscribing to liveliness changes at pattern: {}", pattern);

        this.livelinessSubscriber = new Subscriber(zenohSession, pattern);

        // Start polling thread for liveliness changes
        Thread pollThread = new Thread(() -> {
            while (livelinessRunning.get() && livelinessSubscriber != null && !livelinessSubscriber.isUndeclared()) {
                try {
                    Optional<com.workoss.zenoh.Sample> sampleOpt = livelinessSubscriber.tryRecv();
                    if (sampleOpt.isPresent()) {
                        com.workoss.zenoh.Sample sample = sampleOpt.get();
                        String detectedNodeId = extractNodeId(sample.keyExpr());
                        if (detectedNodeId.equals(nodeId)) continue;

                        // kind: 0=PUT, 1=DELETE (from zenoh SampleKind)
                        if (sample.kind() == 0) { // PUT
                            if (!knownNodes.contains(detectedNodeId)) {
                                knownNodes.add(detectedNodeId);
                                logger.info("Node joined: {}", detectedNodeId);
                                if (nodeListener != null) {
                                    nodeListener.nodeAdded(detectedNodeId);
                                }
                                // The newcomer may have published its registrations and NodeInfo
                                // before this node's subscribers existed. Liveliness is the earliest
                                // reliable signal that a peer is up; its state converges on the
                                // next re-publish round.
                                registrationTable.discoverExistingNodeInfos();
                            }
                        } else if (sample.kind() == 1) { // DELETE
                            if (knownNodes.remove(detectedNodeId)) {
                                logger.info("Node left: {}", detectedNodeId);
                                if (nodeListener != null) {
                                    nodeListener.nodeLeft(detectedNodeId);
                                }
                                // Drops its NodeInfo and every registration, and re-fires
                                // registrationsUpdated so the node selector stops targeting it.
                                registrationTable.removeNode(detectedNodeId);
                            }
                        }
                    } else {
                        Thread.sleep(100); // No data available, sleep briefly
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.debug("Liveliness poll error: {}", e.getMessage());
                    try { Thread.sleep(500); } catch (InterruptedException ie) { break; }
                }
            }
        }, "zenoh-liveliness-poll-" + nodeId.substring(0, 8));
        pollThread.setDaemon(true);
        pollThread.start();
        this.livelinessPollThread = pollThread;

        logger.info("Liveliness subscriber started for pattern: {}", pattern);
    }

    /**
     * Answers {@code session.get()} on the nodes keyspace with this node's own liveliness key.
     * <p>
     * Each live node asserts only itself, so the replies a joining node collects are exactly the
     * set of nodes that were up at query time — the role the storage backend used to play.
     */
    private void startNodeQueryable() {
        String pattern = keyPrefix.allNodesPattern();
        String ownKey = keyPrefix.nodeKey(nodeId);
        byte[] ownPayload = nodeId.getBytes(StandardCharsets.UTF_8);

        this.nodesQueryable = new Queryable(zenohSession, pattern);
        nodesQueryableRunning.set(true);

        Thread pollThread = new Thread(() -> {
            while (nodesQueryableRunning.get() && nodesQueryable != null && !nodesQueryable.isUndeclared()) {
                Queryable current = nodesQueryable;
                if (current == null) {
                    break;
                }
                // try-with-resources matters: dropping the query without replying is what tells the
                // requesting node that no more replies are coming.
                try (Query query = new Query(current)) {
                    String selector = query.getKeyExpr();
                    if (selector.endsWith("*") || selector.equals(ownKey)) {
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
        }, "zenoh-nodes-query-" + nodeId.substring(0, 8));
        pollThread.setDaemon(true);
        pollThread.start();
        this.nodesQueryPollThread = pollThread;

        logger.info("Node queryable started for pattern: {}", pattern);
    }

    private void stopNodeQueryable() {
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
        if (nodesQueryPollThread != null) {
            nodesQueryPollThread.interrupt();
            try { nodesQueryPollThread.join(2000); } catch (InterruptedException ignored) {}
            nodesQueryPollThread = null;
        }
    }

    /**
     * Discover nodes that were already alive before this node subscribed.
     * Each peer answers from its own node queryable.
     */
    private void discoverExistingNodes() {
        try {
            String pattern = keyPrefix.allNodesPattern();
            logger.debug("Discovering existing nodes with pattern: {}", pattern);

            List<Reply> replies = zenohSession.get(pattern);
            int discoveredCount = 0;

            for (Reply reply : replies) {
                if (reply.success() && reply.sampleKeyExpr().isPresent()) {
                    String detectedNodeId = extractNodeId(reply.sampleKeyExpr().get());
                    if (!detectedNodeId.equals(nodeId) && !knownNodes.contains(detectedNodeId)) {
                        knownNodes.add(detectedNodeId);
                        logger.info("Discovered existing node: {}", detectedNodeId);
                        if (nodeListener != null) {
                            nodeListener.nodeAdded(detectedNodeId);
                        }
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

    private void closeZenohSession() {
        logger.info("Closing Zenoh session");
        if (livelinessSubscriber != null) {
            try {
                livelinessSubscriber.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare liveliness subscriber");
            }
            livelinessSubscriber = null;
        }
        // Wait for liveliness poll thread to exit
        if (livelinessPollThread != null) {
            livelinessPollThread.interrupt();
            try { livelinessPollThread.join(2000); } catch (InterruptedException ignored) {}
            livelinessPollThread = null;
        }
        if (zenohSession != null) {
            try {
                zenohSession.closeSession();
            } catch (Exception e) {
                logger.debug("Failed to close Zenoh session", e);
            }
            zenohSession = null;
        }
        if (embeddedRouter != null) {
            try {
                embeddedRouter.closeRouter();
            } catch (Exception e) {
                logger.debug("Failed to close EmbeddedRouter", e);
            }
            embeddedRouter = null;
        }
    }

    private String extractNodeId(String keyExpr) {
        int lastSlash = keyExpr.lastIndexOf('/');
        return lastSlash >= 0 ? keyExpr.substring(lastSlash + 1) : keyExpr;
    }
}
