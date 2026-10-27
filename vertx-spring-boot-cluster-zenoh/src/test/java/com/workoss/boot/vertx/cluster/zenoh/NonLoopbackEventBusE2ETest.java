package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.shareddata.ClusterSerializable;
import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 非回环网卡路径 E2E。
 * <p>
 * 其余 E2E 都用 {@code 127.0.0.1},它有一条不经过网络栈的短路路径,会掩盖真实部署才会暴露的问题:
 * 绑定地址不可达、通配地址被当作可拨号地址、interface 选择。本测试让 Zenoh 仍走回环(避免依赖外部
 * router),但要求 eventbus 监听并经由真实网卡地址通信 —— 与生产拓扑一致。
 * <p>
 * 没有可用的非回环地址时整体跳过,而不是伪造一个通过的结论。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NonLoopbackEventBusE2ETest {

    private static final String APP = "e2e-nonloop-" + UUID.randomUUID().toString().substring(0, 8);
    private static final long TIMEOUT_MS = 30_000L;

    private static String nicAddress;
    private static Vertx vertxA;
    private static Vertx vertxB;
    private static ZenohClusterManager managerA;
    private static ZenohClusterManager managerB;

    @BeforeAll
    static void startCluster() throws Exception {
        nicAddress = firstNonLoopbackAddress();
        assumeTrue(nicAddress != null,
                "no non-loopback IPv4 address available; skipping the real-NIC path test");

        int zenohPort = freePort();

        ZenohClusterProperties propsA = new ZenohClusterProperties();
        propsA.setAppName(APP);
        propsA.setEnvironment("test");
        propsA.setMode("peer");
        propsA.setListen(List.of("tcp/127.0.0.1:" + zenohPort));

        ZenohClusterProperties propsB = new ZenohClusterProperties();
        propsB.setAppName(APP);
        propsB.setEnvironment("test");
        propsB.setMode("client");
        propsB.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));

        // Node A binds the real interface and advertises it, so node B must dial a routable address
        // rather than a loopback shortcut.
        managerA = new ZenohClusterManager(propsA);
        vertxA = build(managerA, nicAddress, freePort());

        Thread.sleep(500);

        managerB = new ZenohClusterManager(propsB);
        vertxB = build(managerB, nicAddress, freePort());

        System.out.println("### NIC=" + nicAddress);
    }

    /**
     * @param host the address the event bus binds and advertises; peers dial exactly this
     */
    private static Vertx build(ZenohClusterManager manager, String host, int port) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost(host).setPort(port);
        Vertx vertx = Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
        // ClusterSerializable only tells Vert.x how to encode; the receiving node still needs a
        // codec to rebuild the body.
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
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("both nodes bind the real interface and advertise it verbatim")
    void nodeInfoAdvertisesRoutableAddress() {
        assertTrue(await(() -> managerA.getNodeInfoOf(managerB.getNodeId()) != null
                        && managerB.getNodeInfoOf(managerA.getNodeId()) != null),
                "NodeInfo never crossed the cluster");

        assertEquals(nicAddress, managerA.getNodeInfoOf(managerB.getNodeId()).host(),
                "node B must advertise the interface it actually bound");
        assertEquals(nicAddress, managerB.getNodeInfoOf(managerA.getNodeId()).host());
    }

    @Test
    @Order(2)
    @DisplayName("the advertised eventbus port is really listening on that interface")
    void advertisedPortIsReachableOnTheInterface() throws Exception {
        assertTrue(await(() -> managerB.getNodeInfoOf(managerA.getNodeId()) != null),
                "node B never learned node A's NodeInfo");

        int port = managerB.getNodeInfoOf(managerA.getNodeId()).port();
        assertTrue(port > 0, "no eventbus port was advertised");

        // Prove the port is bound on the NIC and not only reachable through loopback.
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(nicAddress, port), 5_000);
            assertTrue(socket.isConnected(), "could not reach the advertised port on " + nicAddress);
        } catch (IOException e) {
            throw new AssertionError("advertised " + nicAddress + ":" + port
                    + " is not reachable from a peer; a routable host must be advertised", e);
        }
    }

    @Test
    @Order(3)
    @DisplayName("publish crosses the cluster over the real interface")
    void publishCrossesNodesOverNic() {
        String address = "nonloop.broadcast";
        List<String> onB = Collections.synchronizedList(new java.util.ArrayList<>());
        MessageConsumer<String> consumer = vertxB.eventBus().consumer(address, m -> onB.add(m.body()));

        assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()),
                "node A never learned node B's consumer");

        // Retry: the selector legitimately stays local until the peer is resolved.
        await(() -> {
            vertxA.eventBus().publish(address, "over-nic");
            return onB.contains("over-nic");
        });

        assertTrue(onB.contains("over-nic"),
                "the message never crossed the network interface; clustered publish stayed local");
        consumer.unregister();
    }

    @Test
    @Order(4)
    @DisplayName("request/reply round-trips over the real interface")
    void requestReplyCrossesNodesOverNic() {
        String address = "nonloop.rpc";
        MessageConsumer<String> consumer = vertxB.eventBus().consumer(address, message ->
                message.reply("pong:" + message.body()));

        assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()),
                "node A never learned node B's consumer");

        String answer = null;
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (answer == null && System.currentTimeMillis() < deadline) {
            try {
                answer = vertxA.eventBus().<String>request(address, "ping", new DeliveryOptions())
                        .map(io.vertx.core.eventbus.Message::body)
                        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.onSpinWait();
            }
        }
        consumer.unregister();

        assertNotNull(answer, "no reply over the real interface within " + TIMEOUT_MS + "ms");
        assertEquals("pong:ping", answer);
    }

    @Test
    @Order(5)
    @DisplayName("a ClusterSerializable body crosses the real interface")
    void structuredBodyCrossesNodesOverNic() {
        String address = "nonloop.structured";
        List<Payload> onB = Collections.synchronizedList(new java.util.ArrayList<>());
        MessageConsumer<Payload> consumer = vertxB.eventBus().consumer(address, m -> onB.add(m.body()));

        assertTrue(await(() -> !managerA.getRegistrationsOf(address).isEmpty()),
                "node A never learned the structured topic");

        await(() -> {
            vertxA.eventBus().publish(address, new Payload("hello", 7));
            return !onB.isEmpty();
        });

        assertTrue(!onB.isEmpty(), "the structured body never arrived over the NIC");
        assertEquals("hello", onB.get(0).text());
        assertEquals(7, onB.get(0).number());
        consumer.unregister();
    }

    // --- helpers ---

    private static String firstNonLoopbackAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nic : Collections.list(interfaces)) {
                if (nic.isLoopback() || !nic.isUp()) {
                    continue;
                }
                for (InetAddress address : Collections.list(nic.getInetAddresses())) {
                    if (address.isLoopbackAddress() || address.getHostAddress().contains(":")) {
                        continue;
                    }
                    return address.getHostAddress();
                }
            }
        } catch (Exception e) {
            return null;
        }
        return null;
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
            throw new IllegalStateException("No free port available", e);
        }
    }

    /** Application-style body: proves the codec contract holds over a real socket. */
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

    /** Mirrors the {@link Payload} framing so Vert.x can rebuild it on the receiving node. */
    static final class PayloadCodec
            implements io.vertx.core.eventbus.MessageCodec<Payload, Payload> {

        @Override
        public void encodeToWire(Buffer buffer, Payload payload) {
            payload.writeToBuffer(buffer);
        }

        @Override
        public Payload decodeFromWire(int pos, Buffer buffer) {
            Payload payload = new Payload();
            int end = payload.readFromBuffer(pos, buffer);
            payload.readFromBuffer(0, buffer.getBuffer(pos, end));
            return payload;
        }

        @Override
        public Payload transform(Payload payload) {
            return payload;
        }

        @Override
        public String name() {
            return "nonloop-payload";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
