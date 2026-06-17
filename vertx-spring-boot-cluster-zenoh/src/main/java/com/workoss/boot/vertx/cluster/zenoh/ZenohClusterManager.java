package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.EmbeddedRouter;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Vert.x 5 ClusterManager implementation backed by Zenoh JNI.
 * Supports multi-tenant isolation through key prefix strategy.
 * Uses zenoh-jni native bindings with storage plugin support via EmbeddedRouter-compatible Config.
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
    private Long heartbeatTimerId;
    private final AtomicBoolean livelinessRunning = new AtomicBoolean(false);

    // EventBus components
    private ZenohMessageCodec messageCodec;
    private ZenohEventBusPublisher eventBusPublisher;
    private ZenohEventBusSender eventBusSender;
    private ZenohEventBusSubscriber eventBusSubscriber;

    private final List<String> knownNodes = new CopyOnWriteArrayList<>();
    private final Map<String, NodeInfo> nodeInfoMap = new ConcurrentHashMap<>();
    private final Map<String, List<RegistrationInfo>> registrations = new ConcurrentHashMap<>();

    private volatile boolean active = false;

    public ZenohClusterManager(ZenohClusterProperties properties) {
        this.properties = properties;
        this.keyPrefix = new KeyPrefixStrategy(properties.getAppName(), properties.getEnvironment());
        this.nodeId = UUID.randomUUID().toString();
    }

    @Override
    public void init(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public <K, V> void getAsyncMap(String name, Completable<AsyncMap<K, V>> completable) {
        completable.succeed(new ZenohAsyncMap<>(name, keyPrefix, zenohSession, vertx));
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        ZenohAsyncMap<K, V> asyncMap = new ZenohAsyncMap<>(name, keyPrefix, zenohSession, vertx);
        return asyncMap.entries().await();
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Completable<Lock> completable) {
        vertx.<ZenohLock>executeBlocking(() ->
                ZenohLock.tryAcquireWithTimeout(name, nodeId, keyPrefix, zenohSession, timeout)
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
        completable.succeed(new ZenohCounter(name, keyPrefix, zenohSession, vertx));
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
    }

    @Override
    public void setNodeInfo(NodeInfo nodeInfo, Completable<Void> completable) {
        this.nodeInfo = nodeInfo;
        nodeInfoMap.put(nodeId, nodeInfo);
        String key = keyPrefix.nodeInfoKey(nodeId);
        logger.debug("Publishing node info to key: {}", key);
        if (zenohSession != null) {
            try {
                zenohSession.put(key, nodeInfo.toString().getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                logger.warn("Failed to publish node info to Zenoh", e);
            }
        }
        completable.succeed();
    }

    @Override
    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    @Override
    public void getNodeInfo(String nodeId, Completable<NodeInfo> completable) {
        NodeInfo info = nodeInfoMap.get(nodeId);
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
            startLivelinessHeartbeat();
            subscribeLivelinessChanges();

            // Discover nodes that were already alive before we subscribed
            discoverExistingNodes();

            // Initialize EventBus components
            initializeEventBusComponents();

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
            cleanupEventBusComponents();
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
        registrations.computeIfAbsent(address, k -> new CopyOnWriteArrayList<>()).add(registrationInfo);
        String key = keyPrefix.registrationKey(address, registrationInfo.nodeId());
        if (zenohSession != null) {
            try {
                zenohSession.put(key, address.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                logger.warn("Failed to publish registration to Zenoh", e);
            }
        }
        completable.succeed();
    }

    @Override
    public void removeRegistration(String address, RegistrationInfo registrationInfo, Completable<Void> completable) {
        logger.debug("Removing registration for address={}, nodeId={}", address, registrationInfo.nodeId());
        List<RegistrationInfo> regs = registrations.get(address);
        if (regs != null) {
            regs.remove(registrationInfo);
        }
        String key = keyPrefix.registrationKey(address, registrationInfo.nodeId());
        if (zenohSession != null) {
            try {
                zenohSession.delete(key);
            } catch (Exception e) {
                logger.warn("Failed to delete registration from Zenoh", e);
            }
        }
        completable.succeed();
    }

    @Override
    public void getRegistrations(String address, Completable<List<RegistrationInfo>> completable) {
        List<RegistrationInfo> regs = registrations.getOrDefault(address, List.of());
        completable.succeed(List.copyOf(regs));
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

    // --- Private Zenoh interaction methods ---

    private void openZenohSession() {
        logger.info("Opening Zenoh session with mode={}, connect={}, listen={}",
                properties.getMode(), properties.getConnect(), properties.getListen());

        String backend = properties.getBackend();
        boolean useEmbeddedRouter = "router".equalsIgnoreCase(properties.getMode())
                && backend != null && !backend.isEmpty() && !"none".equalsIgnoreCase(backend);

        if (useEmbeddedRouter) {
            // Use EmbeddedRouter to load storage plugins, then connect a Session to it
            openWithEmbeddedRouter(backend);
        } else {
            // Regular session without storage plugins
            openRegularSession();
        }
    }

    /**
     * Start an EmbeddedRouter with storage backend plugin and connect a Session to it.
     * <p>
     * Uses {@link Config#fromJson5(String)} to parse the full JSON5 config (including plugins),
     * then {@link Config#ensureDefaultPluginsSearchDirs()} to ensure {@code ~/.zenoh/lib}
     * is in the search path. Additional custom dirs from {@code plugin-dir} property are appended.
     */
    private void openWithEmbeddedRouter(String backend) {
        logger.info("Starting EmbeddedRouter with '{}' storage backend", backend);

        // Build listen endpoints JSON array
        List<String> listenEndpoints = properties.getListen();
        String listenJson;
        if (listenEndpoints != null && !listenEndpoints.isEmpty()) {
            listenJson = listenEndpoints.stream()
                    .map(e -> "\"" + e + "\"")
                    .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
        } else {
            listenJson = "[\"tcp/[::]:7447\"]";
        }

        // Resolve storage dir — zenoh filesystem backend requires a RELATIVE path
        // (relative to the working directory where the JVM was started)
        String storageDir = properties.getStorageDir();
        java.io.File storageDirFile = new java.io.File(storageDir);
        if (storageDirFile.isAbsolute()) {
            // Convert absolute path to relative (best-effort: strip user.dir prefix)
            String userDir = System.getProperty("user.dir");
            if (storageDir.startsWith(userDir)) {
                storageDir = storageDir.substring(userDir.length());
                if (storageDir.startsWith(java.io.File.separator)) {
                    storageDir = storageDir.substring(1);
                }
            }
            // If we can't make it relative, keep as-is and let zenoh report the error
        }
        String absStorageDir = new java.io.File(storageDir).getAbsolutePath();

        // Build storage key expression and names  只拦截 /cluster/**
        String storageKeyExpr = keyPrefix.getPrefix() + "/**";
        String volumeName = backend.toLowerCase();
        String storageName = "vertx_" + volumeName;

        // Build volume config based on backend type
        String volumeConfig;
        switch (volumeName) {
            case "filesystem", "fs" ->
                    // zenoh filesystem backend requires explicit backend + relative dir
                    volumeConfig = "{ id: \"" + volumeName + "\", dir: \"" + storageDir + "\" }";
            case "rocksdb" ->
                    volumeConfig = "{ id: \"" + volumeName + "\", dir: \"" + storageDir + "\" }";
            default ->
                    volumeConfig = "\"" + volumeName + "\"";
        }

        // Build full JSON5 config with plugin configuration
        String json5Config = "{\n"
                + "  mode: \"router\",\n"
                + "  plugins_loading: { enabled: true },\n"
                + "  plugins: {\n"
                + "    storage_manager: {\n"
                + "      volumes: { " + volumeName + ": {} },\n"
                + "      storages: {\n"
                + "        " + storageName + ": {\n"
                + "          key_expr: \"" + storageKeyExpr + "\",\n"
                + "          volume: " + volumeConfig + "\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        logger.info("Storage dir: {} (resolved: ~/.zenoh/zenoh_backend_fs/{})", absStorageDir, storageDir);
        logger.info("EmbeddedRouter JSON5 config:\n{}", json5Config);

        // Parse JSON5 into Config object (preserves plugin fields)
        Config routerConfig = Config.fromJson5(json5Config);

        // Ensure ~/.zenoh/lib is in plugin search dirs
        routerConfig.ensureDefaultPluginsSearchDirs();

        // Add custom plugin directories if configured
        List<String> customPluginDirs = properties.getPluginDir();
        if (customPluginDirs != null) {
            for (String dir : customPluginDirs) {
                if (dir != null && !dir.isEmpty()) {
                    routerConfig.addPluginsSearchDir(dir);
                    logger.info("Added custom plugin search dir: {}", dir);
                }
            }
        }

        // Start the embedded router with Config (loads plugins from search dirs)
        this.embeddedRouter = new EmbeddedRouter(routerConfig);
        logger.info("EmbeddedRouter started with storage backend, zid={}", embeddedRouter.zid());

        // Create a client session that connects to the local router
        Config clientConfig = Config.defaultConfig();
        clientConfig.setMode("client");
        clientConfig.setConnectEndpoints(listenEndpoints != null && !listenEndpoints.isEmpty()
                ? listenEndpoints : List.of());

        this.zenohSession = new Session(clientConfig);
        clientConfig.close();
        logger.info("Client session connected to EmbeddedRouter (with storage) successfully");
    }

    /**
     * Open a regular Session without EmbeddedRouter (no storage plugin loading).
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
                            }
                        } else if (sample.kind() == 1) { // DELETE
                            if (knownNodes.remove(detectedNodeId)) {
                                logger.info("Node left: {}", detectedNodeId);
                                if (nodeListener != null) {
                                    nodeListener.nodeLeft(detectedNodeId);
                                }
                                registrations.values().forEach(regs ->
                                        regs.removeIf(reg -> detectedNodeId.equals(reg.nodeId()))
                                );
                                nodeInfoMap.remove(detectedNodeId);
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
     * Discover nodes that were already alive before this node subscribed.
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
            zenohSession.close();
            zenohSession = null;
        }
        if (embeddedRouter != null) {
            try {
                embeddedRouter.close();
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

    // --- EventBus component management ---

    private void initializeEventBusComponents() {
        logger.info("Initializing EventBus components for nodeId: {}", nodeId);

        this.messageCodec = new ZenohMessageCodec();
        this.eventBusPublisher = new ZenohEventBusPublisher(zenohSession, keyPrefix, messageCodec);
        this.eventBusSender = new ZenohEventBusSender(zenohSession, keyPrefix, messageCodec, nodeId, registrations);
        this.eventBusSubscriber = new ZenohEventBusSubscriber(
                zenohSession, keyPrefix, messageCodec, vertx, nodeId
        );
        this.eventBusSubscriber.start();

        logger.info("EventBus components initialized successfully");
    }

    private void cleanupEventBusComponents() {
        logger.info("Cleaning up EventBus components");

        if (eventBusSubscriber != null) {
            eventBusSubscriber.stop();
            eventBusSubscriber = null;
        }

        if (eventBusSender != null) {
            eventBusSender.cleanup();
            eventBusSender = null;
        }

        if (eventBusPublisher != null) {
            eventBusPublisher.close();
            eventBusPublisher = null;
        }

        messageCodec = null;
        logger.info("EventBus components cleaned up");
    }

    public ZenohEventBusPublisher getEventBusPublisher() {
        return eventBusPublisher;
    }

    public ZenohEventBusSender getEventBusSender() {
        return eventBusSender;
    }

    public ZenohEventBusSubscriber getEventBusSubscriber() {
        return eventBusSubscriber;
    }
}
