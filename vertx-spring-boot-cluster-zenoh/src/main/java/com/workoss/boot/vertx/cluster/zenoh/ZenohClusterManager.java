package com.workoss.boot.vertx.cluster.zenoh;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Completable;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.zenoh.Config;
import io.zenoh.Session;
import io.zenoh.Zenoh;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.liveliness.LivelinessToken;
import io.zenoh.pubsub.CallbackSubscriber;
import io.zenoh.sample.Sample;
import io.zenoh.sample.SampleKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Vert.x 5 ClusterManager implementation backed by Zenoh.
 * Supports multi-tenant isolation through key prefix strategy.
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
    private LivelinessToken livelinessToken;
    private CallbackSubscriber livelinessSubscriber;

    // EventBus components
    private ZenohMessageCodec messageCodec;
    private ZenohEventBusPublisher eventBusPublisher;
    private ZenohEventBusSender eventBusSender;
    private ZenohEventBusSubscriber eventBusSubscriber;

    private final List<String> knownNodes = new CopyOnWriteArrayList<>();
    private final Map<String, NodeInfo> nodeInfoMap = new ConcurrentHashMap<>();
    private final Map<String, List<RegistrationInfo>> registrations = new ConcurrentHashMap<>();

    private volatile boolean active = false;

    private final ObjectMapper objectMapper;

    public ZenohClusterManager(ZenohClusterProperties properties,ObjectMapper objectMapper) {
        this.properties = properties;
        this.keyPrefix = new KeyPrefixStrategy(properties.getAppName(), properties.getEnvironment());
        this.nodeId = UUID.randomUUID().toString();
        this.objectMapper = objectMapper;
    }

    @Override
    public void init(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public <K, V> void getAsyncMap(String name, Completable<AsyncMap<K, V>> completable) {
        completable.succeed(new ZenohAsyncMap<>(name, keyPrefix, zenohSession));
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        ZenohAsyncMap<K, V> asyncMap = new ZenohAsyncMap<>(name, keyPrefix, zenohSession);
        return asyncMap.entries().await();
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Completable<Lock> completable) {
        completable.succeed(new ZenohLock(name, nodeId, keyPrefix, zenohSession));
    }

    @Override
    public void getCounter(String name, Completable<Counter> completable) {
        completable.succeed(new ZenohCounter(name, keyPrefix, zenohSession));
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
        // Publish node info to Zenoh for other nodes to discover
        if (zenohSession != null) {
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                zenohSession.put(keyExpr, nodeInfo.toString());
                keyExpr.close();
            } catch (ZError e) {
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
            declareLivelinessToken();
            subscribeLivelinessChanges();

            // Discover nodes that were already alive before we subscribed
            discoverExistingNodes();

            // Initialize EventBus components
            initializeEventBusComponents();

            knownNodes.add(nodeId);
            active = true;
            logger.info("Successfully joined Zenoh cluster");
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
            // Cleanup EventBus components
            cleanupEventBusComponents();

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
        // Publish registration to Zenoh
        if (zenohSession != null) {
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                zenohSession.put(keyExpr, address);
                keyExpr.close();
            } catch (ZError e) {
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
        // Delete registration from Zenoh
        if (zenohSession != null) {
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                zenohSession.delete(keyExpr);
                keyExpr.close();
            } catch (ZError e) {
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

    private void openZenohSession() throws ZError {
        logger.info("Opening Zenoh session with mode={}, connect={}, listen={}",
                properties.getMode(), properties.getConnect(), properties.getListen());

        Config config;
        if (properties.getConfigFile() != null && !properties.getConfigFile().isEmpty()) {
            config = Config.fromFile(new File(properties.getConfigFile()));
        } else {
            config = Config.loadDefault();
        }

        // Set mode
        String mode = properties.getMode();
        if (mode != null && !mode.isEmpty()) {
            config.insertJson5("mode", "\"" + mode + "\"");
        }

        // Set connect endpoints
        List<String> connectEndpoints = properties.getConnect();
        if (connectEndpoints != null && !connectEndpoints.isEmpty()) {
            String endpointsJson = toJsonArray(connectEndpoints);
            config.insertJson5("connect/endpoints", endpointsJson);
        }

        // Set listen endpoints
        List<String> listenEndpoints = properties.getListen();
        if (listenEndpoints != null && !listenEndpoints.isEmpty()) {
            String endpointsJson = toJsonArray(listenEndpoints);
            config.insertJson5("listen/endpoints", endpointsJson);
        }

        this.zenohSession = Zenoh.open(config);
        logger.info("Zenoh session opened successfully");
    }

    private void declareLivelinessToken() throws ZError {
        String key = keyPrefix.nodeKey(nodeId);
        logger.debug("Declaring liveliness token at key: {}", key);
        KeyExpr keyExpr = KeyExpr.tryFrom(key);
        this.livelinessToken = zenohSession.liveliness().declareToken(keyExpr);
        logger.info("Liveliness token declared at: {}", key);
    }

    private void subscribeLivelinessChanges() throws ZError {
        String pattern = keyPrefix.allNodesPattern();
        logger.debug("Subscribing to liveliness changes at pattern: {}", pattern);
        KeyExpr keyExpr = KeyExpr.tryFrom(pattern);

        this.livelinessSubscriber = zenohSession.liveliness().declareSubscriber(keyExpr, (Sample sample) -> {
            String detectedNodeId = extractNodeId(sample.getKeyExpr().toString());
            if (detectedNodeId.equals(nodeId)) {
                return;
            }

            if (sample.getKind() == SampleKind.PUT) {
                if (!knownNodes.contains(detectedNodeId)) {
                    knownNodes.add(detectedNodeId);
                    logger.info("Node joined: {}", detectedNodeId);
                    if (nodeListener != null) {
                        nodeListener.nodeAdded(detectedNodeId);
                    }
                }
            } else if (sample.getKind() == SampleKind.DELETE) {
                if (knownNodes.remove(detectedNodeId)) {
                    logger.info("Node left: {}", detectedNodeId);
                    if (nodeListener != null) {
                        nodeListener.nodeLeft(detectedNodeId);
                    }
                    // Clean up registrations for the departed node
                    registrations.values().forEach(regs ->
                            regs.removeIf(reg -> detectedNodeId.equals(reg.nodeId()))
                    );
                    nodeInfoMap.remove(detectedNodeId);
                }
            }
        });
        logger.info("Liveliness subscriber declared for pattern: {}", pattern);
    }

    /**
     * Discover nodes that were already alive before this node subscribed to liveliness.
     * Zenoh liveliness subscriber only receives events that occur AFTER subscription,
     * so we must actively query for existing tokens.
     */
    private void discoverExistingNodes() {
        try {
            String pattern = keyPrefix.allNodesPattern();
            logger.debug("Discovering existing nodes with pattern: {}", pattern);
            KeyExpr keyExpr = KeyExpr.tryFrom(pattern);

            java.util.concurrent.BlockingQueue<java.util.Optional<io.zenoh.query.Reply>> replies =
                    zenohSession.liveliness().get(keyExpr);

            int discoveredCount = 0;
            // Poll replies with timeout to avoid blocking forever
            while (true) {
                java.util.Optional<io.zenoh.query.Reply> replyOpt =
                        replies.poll(2, TimeUnit.SECONDS);
                if (replyOpt == null || !replyOpt.isPresent()) {
                    break;
                }

                io.zenoh.query.Reply reply = replyOpt.get();
                try {
                    // Reply.Success has getSample() method
                    java.lang.reflect.Method getSampleMethod = reply.getClass().getMethod("getSample");
                    Object sample = getSampleMethod.invoke(reply);
                    if (sample != null) {
                        java.lang.reflect.Method getKeyExprMethod = sample.getClass().getMethod("getKeyExpr");
                        Object sampleKeyExpr = getKeyExprMethod.invoke(sample);
                        String detectedNodeId = extractNodeId(sampleKeyExpr.toString());

                        if (!detectedNodeId.equals(nodeId) && !knownNodes.contains(detectedNodeId)) {
                            knownNodes.add(detectedNodeId);
                            logger.info("Discovered existing node: {}", detectedNodeId);
                            if (nodeListener != null) {
                                nodeListener.nodeAdded(detectedNodeId);
                            }
                            discoveredCount++;
                        }
                    }
                } catch (NoSuchMethodException e) {
                    // Reply.Error or other non-success reply, skip
                    logger.debug("Skipping non-success reply during node discovery");
                }
            }

            keyExpr.close();
            if (discoveredCount > 0) {
                logger.info("Discovered {} existing node(s) via liveliness get", discoveredCount);
            }
        } catch (Exception e) {
            logger.warn("Failed to discover existing nodes", e);
        }
    }

    private void closeZenohSession() {
        logger.info("Closing Zenoh session");
        if (livelinessSubscriber != null) {
            livelinessSubscriber.undeclare();
            livelinessSubscriber = null;
        }
        if (livelinessToken != null) {
            livelinessToken.close();
            livelinessToken = null;
        }
        if (zenohSession != null) {
            zenohSession.close();
            zenohSession = null;
        }
    }

    private String extractNodeId(String keyExpr) {
        int lastSlash = keyExpr.lastIndexOf('/');
        return lastSlash >= 0 ? keyExpr.substring(lastSlash + 1) : keyExpr;
    }

    private static String toJsonArray(List<String> items) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(items.get(i)).append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    // --- EventBus component management ---

    private void initializeEventBusComponents() {
        logger.info("Initializing EventBus components for nodeId: {}", nodeId);

        this.messageCodec = new ZenohMessageCodec(objectMapper);
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

    /**
     * Get the EventBus publisher (for testing).
     */
    public ZenohEventBusPublisher getEventBusPublisher() {
        return eventBusPublisher;
    }

    /**
     * Get the EventBus sender (for testing).
     */
    public ZenohEventBusSender getEventBusSender() {
        return eventBusSender;
    }

    /**
     * Get the EventBus subscriber (for bridge integration).
     */
    public ZenohEventBusSubscriber getEventBusSubscriber() {
        return eventBusSubscriber;
    }
}
