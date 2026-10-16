package com.workoss.boot.zenoh.cluster;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.Session;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Shared helpers for {@code spring-boot-zenoh-cluster} tests: free-port allocation, two-node
 * peer/client {@link ZenohClusterProperties} builders, raw session open/close, and a polling await.
 */
final class ZenohTestSupport {

    private ZenohTestSupport() {
    }

    static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    static ZenohClusterProperties peerProperties(String app, String nodeId, int port) {
        ZenohClusterProperties p = new ZenohClusterProperties();
        p.setAppName(app);
        p.setEnvironment("test");
        p.setNodeId(nodeId);
        p.setMode("peer");
        p.setListen(List.of("tcp/127.0.0.1:" + port));
        p.setHeartbeatInterval(500L);
        return p;
    }

    static ZenohClusterProperties clientProperties(String app, String nodeId, int port) {
        ZenohClusterProperties p = new ZenohClusterProperties();
        p.setAppName(app);
        p.setEnvironment("test");
        p.setNodeId(nodeId);
        p.setMode("client");
        p.setConnect(List.of("tcp/127.0.0.1:" + port));
        p.setHeartbeatInterval(500L);
        return p;
    }

    static Session openPeer(int port) {
        Config config = Config.defaultConfig();
        config.setMode("peer");
        config.setListenEndpoints(List.of("tcp/127.0.0.1:" + port));
        Session session = new Session(config);
        config.close();
        return session;
    }

    static Session openClient(int port) {
        Config config = Config.defaultConfig();
        config.setMode("client");
        config.setConnectEndpoints(List.of("tcp/127.0.0.1:" + port));
        Session session = new Session(config);
        config.close();
        return session;
    }

    /**
     * Poll {@code condition} until it is true or {@code timeoutMillis} elapses.
     *
     * @throws AssertionError when the condition is not met in time
     */
    static void await(BooleanSupplier condition, long timeoutMillis, String description) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Condition not satisfied within " + timeoutMillis
                + " ms: " + description);
    }
}
