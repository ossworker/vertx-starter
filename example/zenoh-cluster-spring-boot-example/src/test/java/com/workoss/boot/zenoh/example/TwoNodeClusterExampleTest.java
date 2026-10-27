package com.workoss.boot.zenoh.example;

import com.workoss.boot.zenoh.cluster.ClusterLock;
import com.workoss.boot.zenoh.cluster.MembershipListener;
import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-node integration test: two independent Spring Boot application contexts (A and B) joined to
 * the same app/environment, proving that {@code spring-boot-zenoh-cluster} actually replicates and
 * routes across nodes when wired through the example application — A put reaches B, broadcast
 * reaches every node, point-to-point reaches only the target, counters and locks converge.
 */
@DisplayName("spring-boot-zenoh-cluster across two example nodes")
class TwoNodeClusterExampleTest {

    private static final long WAIT = 10_000L;
    private static final String APP = "ex-two";
    private static final String ENV = "test";

    private ConfigurableApplicationContext a;
    private ConfigurableApplicationContext b;
    private ConfigurableApplicationContext c;
    private int seedPort;

    @BeforeEach
    void setUp() {
        seedPort = freePort();
        String seed = "tcp/127.0.0.1:" + seedPort;
        a = startNode("node-a", "peer", seed, List.of());
        b = startNode("node-b", "client", null, List.of(seed));
        awaitMembers(a, "node-a", "node-b");
        awaitMembers(b, "node-a", "node-b");
    }

    @AfterEach
    void tearDown() {
        if (c != null) {
            c.close();
            c = null;
        }
        if (b != null) {
            b.close();
            b = null;
        }
        if (a != null) {
            a.close();
            a = null;
        }
    }

    @Test
    @DisplayName("a shared-map write on A is visible on B, and vice versa")
    void sharedMapReplicatesBothWays() {
        SharedMap<String, String> mapA = cluster(a).sharedMap("demo", ValueCodec.utf8());
        SharedMap<String, String> mapB = cluster(b).sharedMap("demo", ValueCodec.utf8());

        mapA.put("x", "1");
        await(() -> "1".equals(mapB.get("x")), "B sees x=1 written by A");

        mapB.put("y", "2");
        await(() -> "2".equals(mapA.get("y")), "A sees y=2 written by B");

        mapA.remove("x");
        await(() -> mapB.get("x") == null, "B sees x removed by A");
    }

    @Test
    @DisplayName("controller endpoints prove A put -> B get across the cluster")
    void sharedMapThroughControllers() {
        controller(a).mapSet("demo", "k", "v", 0L);
        await(() -> "v".equals(controller(b).mapGet("demo", "k").get("value")), "B controller reads A's write");
    }

    @Test
    @DisplayName("broadcast reaches the target node but not via point-to-point")
    void broadcastCrossNode() {
        controller(a).broadcast("hello-b", MessageCollector.DEMO_BROADCAST_ADDRESS);
        await(() -> collector(b).recent().stream()
                        .anyMatch(m -> "hello-b".equals(m.payload()) && "node-a".equals(m.sourceNodeId())
                                && MessageCollector.DEMO_BROADCAST_ADDRESS.equals(m.address())),
                "B received the broadcast from A");
        // The sender also receives its own broadcast.
        await(() -> collector(a).recent().stream()
                        .anyMatch(m -> "hello-b".equals(m.payload()) && "node-a".equals(m.sourceNodeId())),
                "A received its own broadcast");
    }

    @Test
    @DisplayName("point-to-point message reaches only the addressed node")
    void pointToPointCrossNode() {
        controller(a).send("node-b", "ping");
        await(() -> collector(b).recent().stream()
                        .anyMatch(m -> "ping".equals(m.payload())
                                && "node-a".equals(m.sourceNodeId())
                                && "node-b".equals(m.targetNodeId())),
                "B received the direct message addressed to it");
        // A never receives its own point-to-point message.
        assertFalse(collector(a).recent().stream()
                        .anyMatch(m -> "ping".equals(m.payload()) && "node-b".equals(m.targetNodeId())),
                "A must not receive the message it sent to B");
    }

    @Test
    @DisplayName("a @ZenohSubscribe wildcard handler on B fires for a broadcast published by A")
    void annotatedWildcardCrossNode() {
        controller(a).demoAlert("eu", "region-hot");
        await(() -> collector(b).recent().stream()
                        .anyMatch(m -> "alert".equals(m.kind()) && "alerts/eu".equals(m.address())
                                && "region-hot".equals(m.payload()) && "node-a".equals(m.sourceNodeId())),
                "B's @ZenohSubscribe(alerts/**) handler received A's broadcast");
        // The sending node also runs the annotated handler.
        await(() -> collector(a).recent().stream()
                        .anyMatch(m -> "alert".equals(m.kind()) && "region-hot".equals(m.payload())),
                "A's own @ZenohSubscribe handler received the broadcast");
    }

    @Test
    @DisplayName("@ZenohMembership handlers on both nodes observe each other joining")
    void annotatedMembershipCrossNode() {
        await(() -> collector(a).recentMembership().stream()
                        .anyMatch(e -> "join".equals(e.kind()) && "node-b".equals(e.nodeId())),
                "A's @ZenohMembership handler observed B joining");
        await(() -> collector(b).recentMembership().stream()
                        .anyMatch(e -> "join".equals(e.kind()) && "node-a".equals(e.nodeId())),
                "B's @ZenohMembership handler observed A joining");
    }

    @Test
    @DisplayName("@ZenohMembership handlers observe a graceful leave")
    void annotatedMembershipLeave() {
        await(() -> collector(a).recentMembership().stream()
                        .anyMatch(e -> "join".equals(e.kind()) && "node-b".equals(e.nodeId())),
                "A first observes B joining");

        b.close();
        b = null;
        await(() -> collector(a).recentMembership().stream()
                        .anyMatch(e -> "leave".equals(e.kind()) && "node-b".equals(e.nodeId())),
                "A's @ZenohMembership handler observed B leaving");
    }

    @Test
    @DisplayName("a replyTo handler answers a broadcast from another node")
    void replyToCrossNode() {
        controller(b).demoEcho("cross");
        await(() -> collector(a).recent().stream()
                        .anyMatch(m -> "reply".equals(m.kind()) && "echo:cross".equals(m.payload())
                                && "node-a".equals(m.sourceNodeId())),
                "A received the reply its own handler published");
        await(() -> collector(b).recent().stream()
                        .anyMatch(m -> "reply".equals(m.kind()) && "echo:cross".equals(m.payload())
                                && "node-b".equals(m.sourceNodeId())),
                "B received the reply its own handler published");
    }

    @Test
    @DisplayName("@Order holds on every node: first always precedes second")
    void orderedSequenceCrossNode() {
        controller(a).demoOrdered("x");
        await(() -> collector(b).recentOrderedSequence().containsAll(List.of("first:x", "second:x")),
                "B ran both ordered handlers for A's broadcast");
        List<String> sequence = collector(b).recentOrderedSequence();
        assertTrue(sequence.indexOf("first:x") < sequence.indexOf("second:x"),
                "@Order(1) must run before @Order(2) on B, got " + sequence);
    }

    @Test
    @DisplayName("concurrent counter increments on both nodes are all counted")
    void counterConvergesAcrossNodes() {
        for (int i = 0; i < 50; i++) {
            cluster(a).counter("hits").incrementAndGet();
            cluster(b).counter("hits").incrementAndGet();
        }
        await(() -> cluster(a).counter("hits").get() == 100L, "A counts 100");
        await(() -> cluster(b).counter("hits").get() == 100L, "B counts 100");
    }

    @Test
    @DisplayName("a lock held on one node excludes the other node")
    void lockIsExclusiveAcrossNodes() {
        ClusterLock lockA = cluster(a).tryLock("job").orElseThrow();
        try {
            assertTrue(lockA.isHeld());
            // B must not be able to take a lock A holds. Give the acquisition time to replicate
            // before B's retry window gives up; B returns empty only if it observes A as holder.
            assertTrue(cluster(b).tryLock("job", Duration.ofSeconds(3)).isEmpty(),
                    "B cannot acquire the lock held by A");
        } finally {
            lockA.release();
        }
        // After A released, B can take it.
        ClusterLock lockB = cluster(b).tryLock("job", Duration.ofSeconds(5)).orElseThrow();
        assertTrue(lockB.isHeld());
        lockB.release();
    }

    @Test
    @DisplayName("membership listener reports a late-joining node and its graceful leave")
    void membershipEvents() {
        List<String> events = new CopyOnWriteArrayList<>();
        cluster(a).addMembershipListener(new MembershipListener() {
            @Override
            public void nodeJoined(NodeInfo node) {
                events.add("joined:" + node.nodeId());
            }

            @Override
            public void nodeLeft(NodeInfo node) {
                events.add("left:" + node.nodeId());
            }
        });

        int seedPort = this.seedPort;
        c = startNode("node-c", "client", null, List.of("tcp/127.0.0.1:" + seedPort));
        awaitMembers(a, "node-a", "node-b", "node-c");
        await(() -> events.contains("joined:node-c"), "A is notified that C joined");

        c.close();
        c = null;
        await(() -> events.contains("left:node-c"), "A is notified that C left");
        awaitMembers(a, "node-a", "node-b");
    }

    // --- helpers ---

    private static ConfigurableApplicationContext startNode(String nodeId, String mode,
                                                             String listen, List<String> connect) {
        List<String> args = new ArrayList<>();
        args.add("--spring.profiles.active=testmulti");
        args.add("--spring.main.web-application-type=none");
        args.add("--zenoh.cluster.app-name=" + APP);
        args.add("--zenoh.cluster.environment=" + ENV);
        args.add("--zenoh.cluster.node-id=" + nodeId);
        args.add("--zenoh.cluster.mode=" + mode);
        args.add("--zenoh.cluster.heartbeat-interval=200ms");
        args.add("--zenoh.cluster.failure-timeout=1s");
        args.add("--zenoh.cluster.anti-entropy-interval=0");
        if (listen != null) {
            args.add("--zenoh.cluster.listen[0]=" + listen);
        }
        for (int i = 0; i < connect.size(); i++) {
            args.add("--zenoh.cluster.connect[" + i + "]=" + connect.get(i));
        }
        SpringApplication app = new SpringApplication(ZenohClusterExampleApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        return app.run(args.toArray(new String[0]));
    }

    private static ZenohCluster cluster(ConfigurableApplicationContext ctx) {
        return ctx.getBean(ZenohCluster.class);
    }

    private static ClusterDemoController controller(ConfigurableApplicationContext ctx) {
        return ctx.getBean(ClusterDemoController.class);
    }

    private static MessageCollector collector(ConfigurableApplicationContext ctx) {
        return ctx.getBean(MessageCollector.class);
    }

    private static void awaitMembers(ConfigurableApplicationContext ctx, String... ids) {
        Set<String> expected = Set.of(ids);
        await(() -> expected.equals(Set.copyOf(cluster(ctx).members().stream()
                .map(NodeInfo::nodeId).toList())), ctx.getId() + " sees members " + expected);
    }

    private static void await(java.util.function.BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + Duration.ofMillis(WAIT).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Not satisfied within " + WAIT + " ms: " + description);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot allocate free port", e);
        }
    }
}
