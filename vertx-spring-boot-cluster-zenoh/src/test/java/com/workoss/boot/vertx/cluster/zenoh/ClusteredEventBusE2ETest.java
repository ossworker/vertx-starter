package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.VertxBuilder;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.shareddata.ClusterSerializable;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.RegistrationInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双节点集群 E2E:证明原生 {@code ClusteredEventBus} 真的能跨节点投递。
 * <p>
 * 这是整套修复里唯一无法用单元测试替代的一环。单元测试
 * ({@link ClusterRegistrationTableTest})只验证了注册表的<em>数据</em>可见,而真正的链路还包含:
 * <ul>
 *   <li>{@code ClusteredEventBus.start()} 为每个节点自建 NetServer 并把端口发布成 NodeInfo;</li>
 *   <li>{@code DefaultNodeSelector} 用 {@code getRegistrations} 选出远端 nodeId;</li>
 *   <li>{@code connect()} 用 {@code getNodeInfo(remote)} 真正建立出站 TCP 连接;</li>
 *   <li>Vert.x 自带的 codec 把消息体序列化后跨节点传输。</li>
 * </ul>
 * 任何一环不通,publish 都只会静默地留在本机——这正是修复前的行为。
 * <p>
 * 两个真实 {@code Vertx.buildClustered()} 实例通过真实的 Zenoh session(peer + client)互联,
 * 与生产拓扑一致。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusteredEventBusE2ETest {

    /** Isolates this test from any other keyspace on the machine. */
    private static final String APP = "e2e-eventbus-" + UUID.randomUUID().toString().substring(0, 8);

    private static final long TIMEOUT_MS = 30_000L;

    private static Vertx vertxA;
    private static Vertx vertxB;
    private static ZenohClusterManager managerA;
    private static ZenohClusterManager managerB;

    @BeforeAll
    static void startCluster() throws Exception {
        int zenohPort = freePort();

        // Node A: Zenoh peer (listens), plus its own eventbus port.
        ZenohClusterProperties propsA = new ZenohClusterProperties();
        propsA.setAppName(APP);
        propsA.setEnvironment("test");
        propsA.setMode("peer");
        propsA.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        managerA = new ZenohClusterManager(propsA);

        // Node B: Zenoh client (connects to A).
        ZenohClusterProperties propsB = new ZenohClusterProperties();
        propsB.setAppName(APP);
        propsB.setEnvironment("test");
        propsB.setMode("client");
        propsB.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));
        managerB = new ZenohClusterManager(propsB);

        vertxA = buildClustered(managerA, freePort());
        // Give A a moment to declare its liveliness key before B looks for it.
        Thread.sleep(500L);
        vertxB = buildClustered(managerB, freePort());

        assertTrue(awaitUntil(() -> managerA.getNodes().size() >= 2 && managerB.getNodes().size() >= 2),
                "the two Zenoh-backed nodes never discovered each other");
    }

    private static Vertx buildClustered(ZenohClusterManager manager, int eventBusPort) {
        VertxOptions options = new VertxOptions();
        // A fixed, dialable host: a wildcard would be advertised verbatim in NodeInfo.
        options.getEventBusOptions()
                .setHost("127.0.0.1")
                .setPort(eventBusPort);
        Vertx vertx = Vertx.builder()
                .with(options)
                .withClusterManager(manager)
                .buildClustered()
                .toCompletionStage()
                .toCompletableFuture()
                .join();
        // ClusterSerializable only tells Vert.x how to ENCODE a body. The receiving side still has
        // to be able to rebuild it, which means every node must register the codec by name.
        vertx.eventBus().registerDefaultCodec(Payload.class, new PayloadCodec());
        return vertx;
    }

    @AfterAll
    static void stopCluster() {
        closeQuietly(vertxB);
        closeQuietly(vertxA);
    }

    private static void closeQuietly(Vertx vertx) {
        if (vertx != null) {
            try {
                vertx.close().toCompletionStage().toCompletableFuture().join();
            } catch (Exception e) {
                // Best effort: a leaked thread would only fail the next run, not this one.
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("each node reports two members in the cluster")
    void nodesDiscoverEachOther() {
        assertEquals(2, managerA.getNodes().size(), "node A: " + managerA.getNodes());
        assertEquals(2, managerB.getNodes().size(), "node B: " + managerB.getNodes());
        assertFalse(managerA.getNodeId().equals(managerB.getNodeId()), "node ids must differ");
    }

    @Test
    @Order(2)
    @DisplayName("getNodeInfo resolves the peer's dialable host:port")
    void nodeInfoIsResolvableAcrossNodes() {
        // NodeInfo reaches a peer on the liveliness heartbeat cadence, not during join: a put is an
        // instantaneous notification, so each side learns the other only once it has been announced.
        // @Order sequences the tests but does not sequence time, so wait here rather than rely on
        // an earlier test having slept.
        assertTrue(awaitUntil(() -> managerA.getNodeInfoOf(managerB.getNodeId()) != null
                        && managerB.getNodeInfoOf(managerA.getNodeId()) != null),
                "neither side resolved the other's NodeInfo");

        NodeInfo remoteFromA = managerA.getNodeInfoOf(managerB.getNodeId());
        assertEquals("127.0.0.1", remoteFromA.host(),
                "a wildcard bind must never be advertised as a dialable address");
        assertTrue(remoteFromA.port() > 0, "the advertised port must be the bound eventbus port");
        assertTrue(remoteFromA.port() != managerA.getNodeInfoOf(managerA.getNodeId()).port(),
                "each node must advertise its own eventbus listener port");
    }

    @Test
    @Order(3)
    @DisplayName("a consumer registered on B is visible in A's registration table")
    void registrationsAreVisibleClusterWide() {
        String address = "e2e.topic";
        // A handler is required: a handler-less consumer is only registered lazily, so nothing
        // reaches the cluster manager until someone subscribes.
        MessageConsumer<String> consumer = vertxB.eventBus().consumer(address, m -> {
        });

        assertTrue(awaitUntil(() -> !registrationsOn(managerA, address).isEmpty()),
                "node A never saw the registration made on node B — this is the exact defect that "
                        + "kept every clustered publish local");

        RegistrationInfo remote = registrationsOn(managerA, address).stream()
                .filter(r -> managerB.getNodeId().equals(r.nodeId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no registration attributed to node B"));

        assertEquals(managerB.getNodeId(), remote.nodeId());
        consumer.unregister();
    }

    @Test
    @Order(4)
    @DisplayName("publish on A reaches a consumer on B  ← the whole point of the fix")
    void publishCrossesNodes() {
        String address = "e2e.broadcast";
        List<String> receivedOnB = new ArrayList<>();
        MessageConsumer<String> consumer = vertxB.eventBus().consumer(address, m -> receivedOnB.add(m.body()));

        // The registration has to be visible on A before it can select node B as a target.
        assertTrue(awaitUntil(() -> !registrationsOn(managerA, address).isEmpty()),
                "node A never learned that node B serves " + address);

        // Publishing before the selector has resolved the peer is legitimately local-only, so retry
        // until it crosses rather than asserting on a single attempt.
        awaitUntil(() -> {
            vertxA.eventBus().publish(address, "from-a");
            return receivedOnB.contains("from-a");
        });

        assertTrue(receivedOnB.contains("from-a"),
                "the message never reached node B; publish stayed local to node A");

        consumer.unregister();
    }

    @Test
    @Order(5)
    @DisplayName("request/reply round-trips to node B  ← what the Zenoh bridge faked")
    void requestReplyCrossesNodes() throws InterruptedException {
        String address = "e2e.point-to-point";
        List<String> receivedOnB = new ArrayList<>();
        MessageConsumer<String> consumer = vertxB.eventBus().consumer(address, message -> {
            receivedOnB.add(message.body());
            message.reply("pong");
        });

        assertTrue(awaitUntil(() -> !registrationsOn(managerA, address).isEmpty()),
                "node A never learned about node B's consumer");

        // The Zenoh bridge used to hardcode "dummy-reply-id" and answer its own send with a
        // fabricated message. On the native path the reply must genuinely come back from B.
        // Retried for the same reason as publish: no routable peer yet means no cross-node answer.
        String answer = null;
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (answer == null && System.currentTimeMillis() < deadline) {
            try {
                answer = vertxA.eventBus()
                        .<String>request(address, "ping")
                        .map(io.vertx.core.eventbus.Message::body)
                        .toCompletionStage()
                        .toCompletableFuture()
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.sleep(200L);
            }
        }
        consumer.unregister();
        assertNotNull(answer, "no reply from node B within " + TIMEOUT_MS + "ms");
        assertEquals("pong", answer, "the reply must come back from node B");
        assertTrue(awaitUntil(() -> receivedOnB.contains("ping")),
                "node B never received the request body");
    }

    @Test
    @Order(6)
    @DisplayName("broadcast on A reaches consumers on both nodes")
    void broadcastReachesBothNodes() {
        String address = "e2e.both";
        List<String> onA = new ArrayList<>();
        List<String> onB = new ArrayList<>();
        MessageConsumer<String> consumerA = vertxA.eventBus().consumer(address, m -> onA.add(m.body()));
        MessageConsumer<String> consumerB = vertxB.eventBus().consumer(address, m -> onB.add(m.body()));

        assertTrue(awaitUntil(() -> !registrationsOn(managerA, address).isEmpty()
                        && !registrationsOn(managerB, address).isEmpty()),
                "the topic was not registered on both nodes");

        // A publish issued before DefaultNodeSelector has resolved node B is legitimately
        // local-only, so keep publishing until it crosses. Retrying is what a real caller has to do
        // too: there is no "flush" between registering a consumer and being routable.
        awaitUntil(() -> {
            vertxA.eventBus().publish(address, "everyone");
            return onB.contains("everyone");
        });

        assertTrue(awaitUntil(() -> onB.contains("everyone")), "node B missed the broadcast");
        assertTrue(awaitUntil(() -> onA.contains("everyone")), "node A missed its own broadcast");

        consumerA.unregister();
        consumerB.unregister();
    }

    @Test
    @Order(7)
    @DisplayName("a ClusterSerializable body survives the wire")
    void structuredBodyCrossesNodes() {
        String address = "e2e.structured";
        List<Payload> received = new ArrayList<>();
        MessageConsumer<Payload> consumer = vertxB.eventBus().consumer(address, m -> received.add(m.body()));

        assertTrue(awaitUntil(() -> !registrationsOn(managerA, address).isEmpty()),
                "node A never learned about the structured topic");

        vertxA.eventBus().publish(address, new Payload("hello", 42));

        assertTrue(awaitUntil(() -> !received.isEmpty()), "the structured body never arrived");
        assertEquals("hello", received.get(0).text());
        assertEquals(42, received.get(0).number());

        consumer.unregister();
    }

    // --- helpers ---

    private static List<RegistrationInfo> registrationsOn(ZenohClusterManager manager, String address) {
        return manager.getRegistrationsOf(address);
    }

    private static boolean awaitUntil(BooleanSupplier condition) {
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

    /**
     * A body that crosses the cluster the same way application messages must: by implementing
     * {@link ClusterSerializable} <em>and</em> having its codec registered on every node.
     * A plain POJO, or a {@code ClusterSerializable} with no registered codec, is rejected.
     */
    public static final class Payload implements ClusterSerializable {
        private String text;
        private int number;

        public Payload() {
        }

        public Payload(String text, int number) {
            this.text = text;
            this.number = number;
        }

        public String text() {
            return text;
        }

        public int number() {
            return number;
        }

        @Override
        public void writeToBuffer(Buffer buffer) {
            byte[] bytes = text == null ? new byte[0] : text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.appendInt(bytes.length).appendBytes(bytes).appendInt(number);
        }

        @Override
        public int readFromBuffer(int pos, Buffer buffer) {
            int length = buffer.getInt(pos);
            pos += 4;
            this.text = length == 0 ? null : new String(buffer.getBytes(pos, pos + length),
                    java.nio.charset.StandardCharsets.UTF_8);
            pos += length;
            this.number = buffer.getInt(pos);
            return pos + 4;
        }
    }

    /** Mirrors the {@link ClusterSerializable} framing so Vert.x can rebuild a {@link Payload}. */
    static final class PayloadCodec implements io.vertx.core.eventbus.MessageCodec<Payload, Payload> {

        @Override
        public void encodeToWire(Buffer buffer, Payload payload) {
            payload.writeToBuffer(buffer);
        }

        @Override
        public Payload decodeFromWire(int pos, Buffer buffer) {
            Payload payload = new Payload();
            int end = payload.readFromBuffer(pos, buffer);
            // readFromBuffer reads from an absolute position; re-run it over the exact slice so the
            // decode is independent of where the message body starts in the wire buffer.
            payload.readFromBuffer(0, buffer.getBuffer(pos, end));
            return payload;
        }

        @Override
        public Payload transform(Payload payload) {
            return payload;
        }

        @Override
        public String name() {
            return "e2e-payload";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
