package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.boot.vertx.autoconfigure.VertxProperties;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.spi.cluster.NodeInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 通配绑定 + publicHost 的真实集群行为。
 * <p>
 * 生产上最常见的配置是「bind 0.0.0.0,对外宣告一个可拨号地址」。这条路径有两处容易出错:
 * <ul>
 *   <li>若把 {@code 0.0.0.0} 原样放进 {@code NodeInfo},对端会去连 {@code 0.0.0.0} 而失败;</li>
 *   <li>{@code publicHost} 只影响「对外宣告」,若 Vert.x 仍用 bind 地址建连,则宣告与实际不符。</li>
 * </ul>
 * 本测试用真实集群验证 Vert.x 5.2.0 的实际行为,而不是仅检查配置对象的取值。
 */
class WildcardBindE2ETest {

    private static final String APP = "e2e-wildcard-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String WILDCARD = "0.0.0.0";
    private static final long TIMEOUT_MS = 30_000L;

    private static String nicAddress;
    private static Vertx vertxA;
    private static Vertx vertxB;
    private static ZenohClusterManager managerA;
    private static ZenohClusterManager managerB;

    @BeforeAll
    static void startCluster() throws Exception {
        nicAddress = firstNonLoopbackAddress();
        assumeTrue(nicAddress != null, "no non-loopback IPv4 address available");

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

        managerA = new ZenohClusterManager(propsA);
        // A binds the wildcard and advertises the routable address, exactly like a deployment
        // behind NAT or on a multi-homed host.
        vertxA = buildWildcard(managerA, nicAddress, freePort());

        Thread.sleep(500);

        managerB = new ZenohClusterManager(propsB);
        vertxB = build(managerB, nicAddress, freePort());

        System.out.println("### wildcard-bind node advertises=" + nicAddress);
    }

    /**
     * Mirrors {@code VertxClusterAutoConfiguration}: a wildcard bind host is deliberately not copied
     * into {@code EventBusOptions.host}, only the advertised public host is set.
     */
    private static Vertx buildWildcard(ZenohClusterManager manager, String publicHost, int port) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions()
                .setClusterPublicHost(publicHost)
                .setPort(port);
        return Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
    }

    private static Vertx build(ZenohClusterManager manager, String host, int port) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost(host).setPort(port);
        return Vertx.builder().with(options).withClusterManager(manager)
                .buildClustered().toCompletionStage().toCompletableFuture().join();
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
    @DisplayName("the wildcard bind address never reaches NodeInfo")
    void wildcardIsNotAdvertised() {
        assertTrue(await(() -> managerB.getNodeInfoOf(managerA.getNodeId()) != null),
                "node B never learned the wildcard-bound node");

        NodeInfo advertised = managerB.getNodeInfoOf(managerA.getNodeId());
        assertNotEquals(WILDCARD, advertised.host(),
                "0.0.0.0 must never be advertised: peers would try to dial it and fail");
        assertEquals(nicAddress, advertised.host(),
                "the routable public host must be what peers are told to dial");
    }

    @Test
    @DisplayName("the advertised public host and port are actually dialable")
    void advertisedEndpointIsDialable() throws Exception {
        assertTrue(await(() -> managerB.getNodeInfoOf(managerA.getNodeId()) != null),
                "node B never learned the wildcard-bound node");
        NodeInfo advertised = managerB.getNodeInfoOf(managerA.getNodeId());

        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(advertised.host(), advertised.port()), 5_000);
            assertTrue(socket.isConnected());
        } catch (IOException e) {
            throw new AssertionError("advertised endpoint " + advertised.host() + ":" + advertised.port()
                    + " is not dialable — publicHost and the bound port disagree", e);
        }
    }

    @Test
    @DisplayName("messages reach the wildcard-bound node through its advertised address")
    void publishReachesWildcardBoundNode() {
        String address = "wildcard.broadcast";
        List<String> onA = Collections.synchronizedList(new ArrayList<>());
        MessageConsumer<String> consumer = vertxA.eventBus().consumer(address, m -> onA.add(m.body()));

        assertTrue(await(() -> !managerB.getRegistrationsOf(address).isEmpty()),
                "node B never learned that the wildcard-bound node serves " + address);

        // If the peer dialled 0.0.0.0 this would never arrive; it only works because the advertised
        // host is used for the outbound connection.
        await(() -> {
            vertxB.eventBus().publish(address, "via-public-host");
            return onA.contains("via-public-host");
        });

        assertTrue(onA.contains("via-public-host"),
                "the wildcard-bound node never received the message; publicHost is not being dialled");
        consumer.unregister();
    }

    @Test
    @DisplayName("request/reply reaches the wildcard-bound node")
    void requestReplyReachesWildcardBoundNode() {
        String address = "wildcard.rpc";
        MessageConsumer<String> consumer = vertxA.eventBus().consumer(address, m ->
                m.reply("pong:" + m.body()));

        assertTrue(await(() -> !managerB.getRegistrationsOf(address).isEmpty()),
                "node B never learned the RPC address");

        String answer = null;
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (answer == null && System.currentTimeMillis() < deadline) {
            try {
                answer = vertxB.eventBus().<String>request(address, "ping")
                        .map(io.vertx.core.eventbus.Message::body)
                        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                Thread.onSpinWait();
            }
        }
        consumer.unregister();

        assertTrue(answer != null && answer.equals("pong:ping"),
                "the wildcard-bound node never replied; got: " + answer);
    }

    @Test
    @DisplayName("the auto-configuration never copies a wildcard host into EventBusOptions")
    void autoConfigurationHoldsBackWildcardHost() {
        VertxProperties properties = new VertxProperties();
        properties.getCluster().setEnabled(true);
        properties.getCluster().setHost(WILDCARD);
        properties.getCluster().setPublicHost("203.0.113.9");

        VertxOptions options = new VertxOptions();
        new com.workoss.boot.vertx.autoconfigure.VertxClusterAutoConfiguration()
                .clusterManagerCustomizer(null, properties)
                .customize(options);

        assertEquals(null, options.getEventBusOptions().getHost(),
                "a wildcard bind must not become the advertised host");
        assertEquals("203.0.113.9", options.getEventBusOptions().getClusterPublicHost());
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
}
