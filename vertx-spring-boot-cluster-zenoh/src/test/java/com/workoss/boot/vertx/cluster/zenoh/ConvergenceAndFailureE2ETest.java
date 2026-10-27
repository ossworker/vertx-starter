package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageConsumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 收敛延迟与故障可诊断性。
 * <p>
 * 两个部署前必须知道的事实:
 * <ol>
 *   <li><b>收敛延迟</b> —— 后加入的节点要等一个心跳重发周期才对已有节点可路由。这个延迟直接决定
 *       滚动发布时新实例能否立刻接流量,以及「注册完就 publish」是否会丢消息。</li>
 *   <li><b>故障可诊断性</b> —— eventbus 端口不通时,失败应该尽早且可读,而不是超时或静默丢消息。</li>
 * </ol>
 */
class ConvergenceAndFailureE2ETest {

    private static final String APP = "e2e-conv-" + UUID.randomUUID().toString().substring(0, 8);
    /** The heartbeat that re-publishes registrations and node info. */
    private static final long HEARTBEAT_MS = 5_000L;
    private static final long TIMEOUT_MS = 40_000L;

    @Test
    @DisplayName("an existing consumer is discovered by a node that joins later")
    void existingConsumerIsDiscoveredByLateJoiner() throws Exception {
        int zenohPort = freePort();

        ZenohClusterProperties propsA = new ZenohClusterProperties();
        propsA.setAppName(APP);
        propsA.setEnvironment("conv");
        propsA.setMode("peer");
        propsA.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        ZenohClusterManager managerA = new ZenohClusterManager(propsA);
        Vertx vertxA = build(managerA, freePort());

        try {
            Thread.sleep(500);

            // A registers a consumer BEFORE B exists. This is the direction that cannot rely on a
            // live notification: A's write happens long before B's subscriber is declared, and a
            // Zenoh put is not queryable state. Only the periodic re-publish can rescue it.
            String address = "conv.preexisting";
            List<String> onA = Collections.synchronizedList(new ArrayList<>());
            MessageConsumer<String> consumerA = vertxA.eventBus().consumer(address, m -> onA.add(m.body()));

            ZenohClusterProperties propsB = new ZenohClusterProperties();
            propsB.setAppName(APP);
            propsB.setEnvironment("conv");
            propsB.setMode("client");
            propsB.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));
            ZenohClusterManager managerB = new ZenohClusterManager(propsB);
            Vertx vertxB = build(managerB, freePort());

            try {
                long startedAt = System.currentTimeMillis();
                boolean discovered = await(() -> !managerB.getRegistrationsOf(address).isEmpty());
                long convergeMs = System.currentTimeMillis() - startedAt;

                assertTrue(discovered,
                        "the late joiner never discovered the pre-existing consumer on node A");
                assertTrue(convergeMs <= HEARTBEAT_MS * 3,
                        "discovery took " + convergeMs + "ms, more than three heartbeat periods ("
                                + HEARTBEAT_MS + "ms); a rolling deploy would drop traffic for that long");

                System.out.println("### pre-existing consumer discovered in " + convergeMs
                        + "ms (heartbeat=" + HEARTBEAT_MS + "ms)");

                // Now prove it is really routable, not just visible in the table.
                List<String> onB = Collections.synchronizedList(new ArrayList<>());
                MessageConsumer<String> consumerB = vertxB.eventBus().consumer(address, m -> onB.add(m.body()));
                try {
                    await(() -> {
                        vertxB.eventBus().publish(address, "to-a");
                        return onA.contains("to-a");
                    });
                    assertTrue(onA.contains("to-a"),
                            "the message never reached node A even though B discovered its consumer");
                } finally {
                    consumerB.unregister();
                }
            } finally {
                vertxB.close().toCompletionStage().toCompletableFuture().join();
            }
            consumerA.unregister();
        } finally {
            vertxA.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Test
    @DisplayName("an unreachable peer address fails fast with a readable error")
    void unreachablePeerFailsDiagnosably() throws Exception {
        int zenohPort = freePort();

        ZenohClusterProperties propsA = new ZenohClusterProperties();
        propsA.setAppName(APP);
        propsA.setEnvironment("unreachable");
        propsA.setMode("peer");
        propsA.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        ZenohClusterManager managerA = new ZenohClusterManager(propsA);

        // Stand up a listener and close it again, yielding a port that is almost certainly free —
        // the "firewall dropped the eventbus port" case, without needing privileged ports.
        int deadPort = closedPort();

        Vertx vertxA = build(managerA, deadPort);
        try {
            String address = "unreachable.topic";
            // Nothing consumes it locally, so there is no selector entry to make this succeed.
            Throwable failure = null;
            try {
                vertxA.eventBus().request(address, "ping")
                        .toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
            } catch (Exception e) {
                failure = e;
            }

            assertNotNull(failure,
                    "requesting an address with no handler must not hang or silently succeed");

            // The message has to name the address or the failure is unactionable in production.
            String text = String.valueOf(failure.getMessage());
            assertTrue(text.contains(address) || text.contains("NO_HANDLERS"),
                    "the failure should identify the address or the missing-handler cause; got: " + text);
            System.out.println("### unreachable/no-handler failure = " + failure.getClass().getSimpleName()
                    + ": " + text);
        } finally {
            vertxA.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    /** @return a port that was bound a moment ago and is now free */
    private static int closedPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port available", e);
        }
    }

    @Test
    @DisplayName("node info for a peer that never announced itself is null, not a stub")
    void unknownNodeHasNoNodeInfo() throws Exception {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("unknown");
        props.setMode("peer");
        props.setListen(List.of("tcp/127.0.0.1:" + freePort()));
        ZenohClusterManager manager = new ZenohClusterManager(props);
        Vertx vertx = build(manager, freePort());
        try {
            // A fabricated node id must resolve to null so connect() fails fast and loudly, instead
            // of dialling host 0 / port 0.
            assertEquals(null, manager.getNodeInfoOf("no-such-node-id"),
                    "an unknown node must not yield a placeholder NodeInfo");

            RecordingCompletable completable = new RecordingCompletable();
            manager.getNodeInfo("no-such-node-id", completable);

            assertNotNull(completable.failure,
                    "getNodeInfo(unknown) must fail so connect() stops early instead of dialling "
                            + "host 0 / port 0");
            assertTrue(String.valueOf(completable.failure.getMessage()).contains("no-such-node-id"),
                    "the failure should name the node it could not resolve; got: "
                            + completable.failure.getMessage());
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    /**
     * Captures how a {@code ClusterManager} callback completed. {@code Completable} exposes a single
     * abstract method — the {@code succeed}/{@code fail} helpers are defaults on top of it — so this
     * is the whole contract.
     */
    private static final class RecordingCompletable
            implements io.vertx.core.Completable<io.vertx.core.spi.cluster.NodeInfo> {

        private io.vertx.core.spi.cluster.NodeInfo result;
        private Throwable failure;

        @Override
        public void complete(io.vertx.core.spi.cluster.NodeInfo nodeInfo, Throwable cause) {
            this.result = nodeInfo;
            this.failure = cause;
        }
    }

    private static Vertx build(ZenohClusterManager manager, int eventBusPort) {        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost("127.0.0.1").setPort(eventBusPort);
        return Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
    }

    private static boolean await(BooleanSupplier condition) {
        return await(condition, TIMEOUT_MS);
    }

    private static boolean await(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(50L);
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
            throw new IllegalStateException("No free port available", e);
        }
    }
}
