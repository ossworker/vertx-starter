package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.ZenohMembership;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.annotation.Order;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("@ZenohMembership annotation handling over real Zenoh")
class ZenohMembershipAnnotationTest {

    private static final long WAIT = 8_000L;

    private final ApplicationContextRunner withHandlers = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class))
            .withBean("membership", MembershipHandlers.class, MembershipHandlers::new);

    @Test
    @DisplayName("a peer joining and leaving is reported to the annotated handlers")
    void joinAndLeave() throws IOException {
        Topology topology = Topology.start("mem", withHandlers);
        topology.nodeA.run(ctxA -> topology.nodeB.run(ctxB -> {
            MembershipHandlers handlers = ctxA.getBean(MembershipHandlers.class);
            ZenohCluster clusterB = ctxB.getBean(ZenohCluster.class);

            await(() -> handlers.joined.stream().anyMatch(n -> "node-b".equals(n.nodeId())),
                    "A is told that node-b joined");
            NodeInfo joined = handlers.joined.stream()
                    .filter(n -> "node-b".equals(n.nodeId()))
                    .findFirst()
                    .orElseThrow();
            assertThat(joined.incarnation()).isNotBlank();
            assertThat(joined.startedAt()).isGreaterThan(0);
            assertThat(handlers.left).isEmpty();

            // A graceful leave withdraws the membership key, so the leave arrives without waiting
            // for the failure timeout.
            ((SmartLifecycle) clusterB).stop();
            await(() -> handlers.left.stream().anyMatch(n -> "node-b".equals(n.nodeId())),
                    "A is told that node-b left");
            assertThat(handlers.joined).noneMatch(n -> "node-a".equals(n.nodeId()));
            assertThat(handlers.left).noneMatch(n -> "node-a".equals(n.nodeId()));
        }));
    }

    @Test
    @DisplayName("a handler that selects one event never sees the other")
    void onlySelectedEvents() throws IOException {
        Topology topology = Topology.start("mem", withHandlers);
        topology.        nodeA.run(ctxA -> topology.nodeB.run(ctxB -> {
            MembershipHandlers handlers = ctxA.getBean(MembershipHandlers.class);
            await(() -> handlers.joined.stream().anyMatch(n -> "node-b".equals(n.nodeId())),
                    "join handler saw node-b join");
            int joinsBeforeLeave = handlers.joined.size();
            assertThat(handlers.left).isEmpty();

            ((SmartLifecycle) ctxB.getBean(ZenohCluster.class)).stop();
            await(() -> handlers.left.stream().anyMatch(n -> "node-b".equals(n.nodeId())),
                    "leave handler saw node-b leave");
            // The leave must not have reached the join-only handler.
            assertThat(handlers.joined).hasSize(joinsBeforeLeave);
        }));
    }

    @Test
    @DisplayName("@Order fixes the order in which handlers of the same event run")
    void orderedHandlers() throws IOException {
        Topology topology = Topology.start("mem", withHandlers);
        topology.nodeA.run(ctxA -> topology.nodeB.run(ctxB -> {
            MembershipHandlers handlers = ctxA.getBean(MembershipHandlers.class);
            await(() -> handlers.joinSequence.size() == 2, "both ordered join handlers ran");
            // Declared with @Order(2) before @Order(1): the lower order must still run first.
            assertThat(handlers.joinSequence).containsExactly("first-declared", "second-declared");
        }));
    }

    @Test
    @DisplayName("a mis-declared handler fails the context at startup")
    void invalidHandlerRejected() throws IOException {
        withHandlers.withBean("bad", BadHandlers.class, BadHandlers::new)
                .withPropertyValues(
                        "zenoh.cluster.app-name=bad",
                        "zenoh.cluster.node-id=node-bad",
                        "zenoh.cluster.mode=peer",
                        "zenoh.cluster.listen=tcp/127.0.0.1:" + freePort(),
                        "zenoh.cluster.heartbeat-interval=200ms")
                .run(context -> assertThat(context).hasFailed());
    }

    /** Node A carries the handlers and listens; node B is a plain client that connects to it. */
    private record Topology(ApplicationContextRunner nodeA, ApplicationContextRunner nodeB) {

        static Topology start(String appPrefix, ApplicationContextRunner withHandlers) throws IOException {
            int port = freePort();
            String app = appPrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
            String endpoint = "tcp/127.0.0.1:" + port;
            ApplicationContextRunner nodeA = withHandlers.withPropertyValues(
                    "zenoh.cluster.app-name=" + app,
                    "zenoh.cluster.environment=test",
                    "zenoh.cluster.node-id=node-a",
                    "zenoh.cluster.mode=peer",
                    "zenoh.cluster.listen=" + endpoint,
                    "zenoh.cluster.heartbeat-interval=200ms",
                    "zenoh.cluster.failure-timeout=1s");
            ApplicationContextRunner nodeB = new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class))
                    .withPropertyValues(
                            "zenoh.cluster.app-name=" + app,
                            "zenoh.cluster.environment=test",
                            "zenoh.cluster.node-id=node-b",
                            "zenoh.cluster.mode=client",
                            "zenoh.cluster.connect=" + endpoint,
                            "zenoh.cluster.heartbeat-interval=200ms",
                            "zenoh.cluster.failure-timeout=1s");
            return new Topology(nodeA, nodeB);
        }
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static class MembershipHandlers {

        final List<NodeInfo> joined = new CopyOnWriteArrayList<>();
        final List<NodeInfo> left = new CopyOnWriteArrayList<>();
        final List<String> joinSequence = new CopyOnWriteArrayList<>();

        @ZenohMembership(ZenohMembership.Event.NODE_JOINED)
        void onJoin(NodeInfo node) {
            joined.add(node);
        }

        @ZenohMembership(ZenohMembership.Event.NODE_LEFT)
        void onLeave(NodeInfo node) {
            left.add(node);
        }

        @ZenohMembership(ZenohMembership.Event.NODE_JOINED)
        @Order(2)
        void declaredSecondRunsSecond(NodeInfo node) {
            joinSequence.add("second-declared");
        }

        @ZenohMembership(ZenohMembership.Event.NODE_JOINED)
        @Order(1)
        void declaredFirstRunsFirst(NodeInfo node) {
            joinSequence.add("first-declared");
        }
    }

    static class BadHandlers {

        @ZenohMembership
        void wrongParameter(String notNodeInfo) {
        }
    }
}
