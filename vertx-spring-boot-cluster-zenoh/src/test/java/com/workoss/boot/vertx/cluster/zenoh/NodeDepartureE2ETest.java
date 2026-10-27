package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.spi.cluster.RegistrationInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双节点 E2E:节点离开后,它的注册项与 NodeInfo 必须从对端消失。
 * <p>
 * 这是修复前必然失败的一环。旧的 {@code getRegistrations} 只返回本节点注册项,加上 Zenoh 的
 * {@code put} 是一次性通知而非可回查状态,一个下线的节点会永久留在对端的注册表里 ——
 * {@code DefaultNodeSelector} 于是持续把消息投递到一个已经不存在的节点。
 * <p>
 * 与 {@link ClusteredEventBusE2ETest} 共用同一集群拓扑,因此也在回归「下线后能否重新加入」。
 */
class NodeDepartureE2ETest {

    private static final String APP = "e2e-depart-" + UUID.randomUUID().toString().substring(0, 8);
    private static final long TIMEOUT_MS = 30_000L;

    private static int zenohPort;

    @BeforeAll
    static void reservePort() {
        zenohPort = freePort();
    }

    @AfterAll
    static void releasePort() {
    }

    @Test
    @DisplayName("a departed node disappears from the survivor's routing tables")
    void departureIsPropagated() throws Exception {
        ZenohClusterManager managerA = new ZenohClusterManager(peerProps());
        Vertx vertxA = clustered(managerA, freePort());
        ZenohClusterManager managerB;
        Vertx vertxB = null;
        MessageConsumer<String> consumerOnB = null;

        try {
            Thread.sleep(500);
            managerB = new ZenohClusterManager(clientProps());
            vertxB = clustered(managerB, freePort());

            String address = "depart.topic";
            // A handler is required, otherwise the consumer registers lazily and never reaches the
            // cluster manager.
            consumerOnB = vertxB.eventBus().consumer(address, m -> {
            });

            assertTrue(await(() -> {
                for (RegistrationInfo info : managerA.getRegistrationsOf(address)) {
                    if (managerB.getNodeId().equals(info.nodeId())) {
                        return true;
                    }
                }
                return false;
            }), "node A never saw node B's registration, so departure cannot be observed");

            // The roster is fed by the liveliness heartbeat, so a peer appears one interval after
            // its registration does. Wait rather than assuming they arrive together.
            assertTrue(await(() -> managerA.getNodes().size() == 2),
                    "both nodes should be visible before departure, roster=" + managerA.getNodes());

            // B leaves the cluster for real: this triggers leave() on its ClusterManager.
            vertxB.close().toCompletionStage().toCompletableFuture().join();
            vertxB = null;

            assertTrue(await(() -> managerA.getRegistrationsOf(address).stream()
                            .noneMatch(info -> managerB.getNodeId().equals(info.nodeId()))),
                    "node A still routes to the departed node — its selector would keep targeting a "
                            + "node that no longer exists");
            assertTrue(await(() -> managerA.getNodes().size() == 1),
                    "the departed node should be gone from the roster, still="
                            + managerA.getNodes());

            // A replacement joining must not resurrect the stale entry.
            ZenohClusterManager managerC = new ZenohClusterManager(clientProps());
            Vertx vertxC = null;
            try {
                vertxC = clustered(managerC, freePort());
                assertTrue(await(() -> managerA.getNodes().size() == 2),
                        "the replacement node never joined");
                assertTrue(await(() -> managerA.getRegistrationsOf(address).stream()
                                .noneMatch(info -> managerB.getNodeId().equals(info.nodeId()))),
                        "the stale registration came back");
                assertFalse(managerA.getNodeInfoOf(managerB.getNodeId()) != null,
                        "the departed node's NodeInfo should have been dropped");
            } finally {
                if (vertxC != null) {
                    vertxC.close().toCompletionStage().toCompletableFuture().join();
                }
            }
        } finally {
            if (consumerOnB != null) {
                try {
                    consumerOnB.unregister();
                } catch (Exception ignored) {
                    // node already gone
                }
            }
            if (vertxB != null) {
                vertxB.close().toCompletionStage().toCompletableFuture().join();
            }
            vertxA.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    private static ZenohClusterProperties peerProps() {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("test");
        props.setMode("peer");
        props.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        return props;
    }

    private static ZenohClusterProperties clientProps() {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("test");
        props.setMode("client");
        props.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));
        return props;
    }

    private static Vertx clustered(ZenohClusterManager manager, int eventBusPort) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost("127.0.0.1").setPort(eventBusPort);
        return Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
    }

    private static boolean await(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port for the E2E cluster", e);
        }
    }
}
