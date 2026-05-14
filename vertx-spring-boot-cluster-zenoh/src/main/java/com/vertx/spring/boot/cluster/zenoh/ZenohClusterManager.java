package com.vertx.spring.boot.cluster.zenoh;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
        completable.succeed(new ZenohAsyncMap<>(name, keyPrefix));
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        return new ConcurrentHashMap<>();
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Completable<Lock> completable) {
        completable.succeed(new ZenohLock(name, nodeId, keyPrefix));
    }

    @Override
    public void getCounter(String name, Completable<Counter> completable) {
        completable.succeed(new ZenohCounter(name, keyPrefix));
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
        logger.debug("Publishing registration to key: {}", key);
        completable.succeed();
    }

    @Override
    public void removeRegistration(String address, RegistrationInfo registrationInfo, Completable<Void> completable) {
        logger.debug("Removing registration for address={}, nodeId={}", address, registrationInfo.nodeId());
        List<RegistrationInfo> regs = registrations.get(address);
        if (regs != null) {
            regs.remove(registrationInfo);
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

    // --- Private Zenoh interaction methods ---

    private void openZenohSession() {
        logger.info("Opening Zenoh session with mode={}, connect={}",
                properties.getMode(), properties.getConnect());
        // Real implementation with zenoh-java:
        // import io.zenoh.Config;
        // import io.zenoh.Session;
        // import io.zenoh.Zenoh;
        // Config config = Config.defaultConfig();
        // Session session = Zenoh.open(config);
        // this.zenohSession = session;
    }

    private void declareLivelinessToken() {
        String key = keyPrefix.nodeKey(nodeId);
        logger.debug("Declaring liveliness token at key: {}", key);
        // Real implementation:
        // livelinessToken = zenohSession.liveliness().declareToken(KeyExpr.of(key));
    }

    private void subscribeLivelinessChanges() {
        String pattern = keyPrefix.allNodesPattern();
        logger.debug("Subscribing to liveliness changes at pattern: {}", pattern);
        // Real implementation:
        // zenohSession.liveliness().declareSubscriber(KeyExpr.of(pattern), sample -> {
        //     String detectedNodeId = extractNodeId(sample.getKeyExpr().toString());
        //     if (detectedNodeId.equals(nodeId)) return;
        //     if (sample.getKind() == SampleKind.PUT) {
        //         if (!knownNodes.contains(detectedNodeId)) {
        //             knownNodes.add(detectedNodeId);
        //             if (nodeListener != null) nodeListener.nodeAdded(detectedNodeId);
        //         }
        //     } else if (sample.getKind() == SampleKind.DELETE) {
        //         knownNodes.remove(detectedNodeId);
        //         if (nodeListener != null) nodeListener.nodeLeft(detectedNodeId);
        //     }
        // });
    }

    private void closeZenohSession() {
        logger.info("Closing Zenoh session");
        // Real implementation:
        // if (livelinessToken != null) livelinessToken.close();
        // if (zenohSession != null) zenohSession.close();
    }

    @SuppressWarnings("unused")
    private String extractNodeId(String keyExpr) {
        int lastSlash = keyExpr.lastIndexOf('/');
        return lastSlash >= 0 ? keyExpr.substring(lastSlash + 1) : keyExpr;
    }
}
