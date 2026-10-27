package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.ZenohSubscribe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("@ZenohSubscribe annotation handling over real Zenoh")
class ZenohSubscribeAnnotationTest {

    private static final long WAIT = 8_000L;

    private final ApplicationContextRunner base = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class))
            .withBean("handlers", Handlers.class, Handlers::new);

    @Test
    @DisplayName("wildcard and exact broadcast subscriptions plus a direct handler")
    void annotationSubscriptions() throws IOException {
        int port = freePort();
        String app = "ann-" + UUID.randomUUID().toString().substring(0, 8);
        String endpoint = "tcp/127.0.0.1:" + port;

        ApplicationContextRunner nodeA = base.withPropertyValues(
                "zenoh.cluster.app-name=" + app,
                "zenoh.cluster.environment=test",
                "zenoh.cluster.node-id=node-a",
                "zenoh.cluster.mode=router",
                "zenoh.cluster.listen=" + endpoint,
                "zenoh.cluster.heartbeat-interval=200ms");
        ApplicationContextRunner nodeB = base.withPropertyValues(
                "zenoh.cluster.app-name=" + app,
                "zenoh.cluster.environment=test",
                "zenoh.cluster.node-id=node-b",
                "zenoh.cluster.mode=client",
                "zenoh.cluster.connect=" + endpoint,
                "zenoh.cluster.heartbeat-interval=200ms");

        nodeA.run(ctxA -> nodeB.run(ctxB -> {
            Handlers ha = ctxA.getBean(Handlers.class);
            Handlers hb = ctxB.getBean(Handlers.class);
            ClusterMessaging messagingA = ctxA.getBean(ClusterMessaging.class);
            ZenohCluster clusterA = ctxA.getBean(ZenohCluster.class);
            ZenohCluster clusterB = ctxB.getBean(ZenohCluster.class);
            await(() -> clusterA.members().size() == 2 && clusterB.members().size() == 2,
                    "both nodes discovered each other");

            // Subscriber declarations propagate asynchronously, so resend until delivered and keep
            // the assertions duplicate-tolerant.
            resendUntil(() -> messagingA.broadcast("orders/eu", "shipped".getBytes(StandardCharsets.UTF_8)),
                    () -> !ha.wildcard.isEmpty() && !hb.wildcard.isEmpty(), "wildcard broadcast reached both nodes");
            assertThat(ha.wildcard).contains("orders/eu");
            assertThat(hb.wildcard).contains("orders/eu");
            assertThat(ha.exact).contains("shipped");
            assertThat(hb.exact).contains("shipped");
            assertThat(ha.wildcardSource).contains("node-a");
            assertThat(ha.catchAll).anyMatch(bytes -> "shipped".equals(new String(bytes, StandardCharsets.UTF_8)));

            ha.clear();
            hb.clear();
            resendUntil(() -> messagingA.broadcast("orders/us", "x".getBytes(StandardCharsets.UTF_8)),
                    () -> !hb.wildcard.isEmpty(), "orders/us reached B wildcard");
            assertThat(hb.wildcard).contains("orders/us");
            assertThat(hb.exact).isEmpty();

            resendUntil(() -> messagingA.send("node-b", "ping".getBytes(StandardCharsets.UTF_8)),
                    () -> !hb.direct.isEmpty(), "direct A -> B");
            ClusterMessage direct = hb.direct.getFirst();
            assertThat(direct.payloadAsString()).isEqualTo("ping");
            assertThat(direct.sourceNodeId()).isEqualTo("node-a");
            assertThat(direct.targetNodeId()).isEqualTo("node-b");
            assertThat(ha.direct).isEmpty();
        }));
    }

    @Test
    @DisplayName("a mis-declared handler fails the context at startup")
    void invalidHandlerRejected() throws IOException {
        base.withBean("bad", BadHandlers.class, BadHandlers::new)
                .withPropertyValues(
                        "zenoh.cluster.app-name=bad",
                        "zenoh.cluster.node-id=node-bad",
                        "zenoh.cluster.mode=peer",
                        "zenoh.cluster.listen=tcp/127.0.0.1:" + freePort(),
                        "zenoh.cluster.heartbeat-interval=200ms")
                .run(context -> assertThat(context).hasFailed());
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void await(BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + WAIT * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("Not satisfied: " + description);
    }

    private static void resendUntil(Runnable send, BooleanSupplier delivered, String description) {
        long deadline = System.nanoTime() + WAIT * 1_000_000L;
        while (System.nanoTime() < deadline) {
            send.run();
            long until = System.nanoTime() + 200_000_000L;
            while (System.nanoTime() < until) {
                if (delivered.getAsBoolean()) {
                    return;
                }
                sleep(20);
            }
        }
        throw new AssertionError("Not delivered: " + description);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static class Handlers {

        final List<String> wildcard = new CopyOnWriteArrayList<>();
        final List<String> wildcardSource = new CopyOnWriteArrayList<>();
        final List<String> exact = new CopyOnWriteArrayList<>();
        final List<byte[]> catchAll = new CopyOnWriteArrayList<>();
        final List<ClusterMessage> direct = new CopyOnWriteArrayList<>();

        @ZenohSubscribe("orders/*")
        void onOrder(ClusterMessage message) {
            wildcard.add(message.address());
            wildcardSource.add(message.sourceNodeId());
        }

        @ZenohSubscribe("orders/eu")
        void onOrderEu(String payload) {
            exact.add(payload);
        }

        @ZenohSubscribe("**")
        void onAnything(byte[] payload) {
            catchAll.add(payload);
        }

        @ZenohSubscribe(direct = true)
        void onDirect(ClusterMessage message) {
            direct.add(message);
        }

        void clear() {
            wildcard.clear();
            wildcardSource.clear();
            exact.clear();
            catchAll.clear();
            direct.clear();
        }
    }

    static class BadHandlers {

        @ZenohSubscribe("x")
        void wrongArity(ClusterMessage message, String extra) {
        }
    }
}
