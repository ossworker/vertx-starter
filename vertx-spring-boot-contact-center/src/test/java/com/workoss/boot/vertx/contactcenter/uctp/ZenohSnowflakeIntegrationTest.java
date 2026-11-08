package com.workoss.boot.vertx.contactcenter.uctp;

import com.workoss.boot.vertx.cluster.zenoh.ZenohClusterManager;
import com.workoss.boot.vertx.cluster.zenoh.ZenohClusterProperties;
import com.workoss.boot.vertx.contactcenter.uctp.ClusterSnowflakeService.Timing;
import com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator.Window;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Real JNI Zenoh + clustered Vert.x, never mocks: node numbers are leased through the generic cluster
 * AsyncMap, which the Zenoh cluster manager isolates under its {@code zenoh/{app}/{env}} prefix.
 * Opt-in with -Ductp.test.zenoh.enabled=true; without it JUnit records SKIP, with it failures fail.
 */
@Timeout(180)
class ZenohSnowflakeIntegrationTest {
    private static final String HOST = "127.0.0.1";
    private static final int IDS_PER_NODE = 10_000;
    private static final int TIMEOUT_SECONDS = 30;
    private static final Timing TIMING = new Timing(10_000, 2_000, 200, 1_000, 500, 20_000, 50, 200);

    private final String isolation = UUID.randomUUID().toString().replace("-", "");
    private ZenohClusterManager managerA;
    private ZenohClusterManager managerB;
    private Vertx vertxA;
    private Vertx vertxB;
    private ClusterSnowflakeService serviceA;
    private ClusterSnowflakeService serviceB;
    private MessageConsumer<JsonObject> remoteConsumer;
    private ExecutorService workers;
    private Path zenohConfig;

    @BeforeEach
    void startIsolatedRealCluster() throws Exception {
        assumeTrue(Boolean.getBoolean("uctp.test.zenoh.enabled"), "SKIP: requires -Ductp.test.zenoh.enabled=true");
        // A config file is necessary because cluster properties do not expose scouting controls.
        // Disable multicast/gossip so no discovery socket or auto-connected peer escapes loopback.
        zenohConfig = Files.createTempFile("uctp-zenoh-" + isolation + "-", ".json5");
        Files.writeString(zenohConfig, """
                {
                  listen: { endpoints: [] },
                  connect: { endpoints: [] },
                  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
                  transport: { link: { protocols: ["tcp"] } },
                  plugins_loading: { enabled: false }
                }
                """);
        int zenohPort = freeLoopbackPort();
        ZenohClusterProperties peer = properties("peer");
        peer.setListen(List.of("tcp/" + HOST + ":" + zenohPort));
        managerA = new ZenohClusterManager(peer);
        vertxA = buildClustered(managerA);

        ZenohClusterProperties client = properties("client");
        client.setConnect(List.of("tcp/" + HOST + ":" + zenohPort));
        client.setListen(List.of("tcp/" + HOST + ":" + freeLoopbackPort()));
        managerB = new ZenohClusterManager(client);
        vertxB = buildClustered(managerB);
        awaitUntil(() -> managerA.getNodes().size() == 2 && managerB.getNodes().size() == 2
                        && managerA.getNodeInfoOf(managerB.getNodeId()) != null
                        && managerB.getNodeInfoOf(managerA.getNodeId()) != null,
                "The real Zenoh nodes must discover each other and resolve remote EventBus listeners");
        assertNotEquals(managerA.getNodeId(), managerB.getNodeId());
        assertEquals(HOST, managerA.getNodeInfoOf(managerB.getNodeId()).host());
        assertEquals(HOST, managerB.getNodeInfoOf(managerA.getNodeId()).host());
        serviceA = new ClusterSnowflakeService(vertxA, 2, TIMING, System::currentTimeMillis);
        serviceB = new ClusterSnowflakeService(vertxB, 2, TIMING, System::currentTimeMillis);
    }

    @Test
    void realClusterLeasesDistinctNodesReplicatesLeasesAndRecoversFromOverwrite() throws Exception {
        assertTrue(vertxA.isClustered());
        assertTrue(vertxB.isClustered());
        Future<SnowflakeGenerator> startingA = serviceA.start();
        Future<SnowflakeGenerator> startingB = serviceB.start();
        SnowflakeGenerator generatorA = await(startingA);
        SnowflakeGenerator generatorB = await(startingB);
        Window windowA = generatorA.window();
        Window windowB = generatorB.window();
        assertNotEquals(windowA.owner(), windowB.owner());
        assertNotEquals(windowA.nodeId(), windowB.nodeId(), "Live leases must use distinct node bits");

        AsyncMap<String, String> mapA = await(vertxA.sharedData().getAsyncMap(ClusterSnowflakeService.MAP_NAME));
        AsyncMap<String, String> mapB = await(vertxB.sharedData().getAsyncMap(ClusterSnowflakeService.MAP_NAME));
        for (AsyncMap<String, String> map : List.of(mapA, mapB)) {
            awaitUntil(() -> recordedBy(map, windowA) && recordedBy(map, windowB),
                    "Both leases must be replicated to every node through the cluster map");
        }

        proveRemoteRequestReply();
        Set<Long> allIds = generateConcurrently(generatorA, generatorB);
        assertEquals(2 * IDS_PER_NODE, allIds.size(), "At least 20,000 concurrent IDs must be globally unique");

        long now = System.currentTimeMillis();
        String intruder = "intruder," + now + "," + (now + 600_000);
        await(mapB.put(String.valueOf(windowA.nodeId()), intruder));
        awaitUntil(() -> !generatorA.revoked() && generatorA.window().nodeId() != windowA.nodeId(),
                "A must notice the remote overwrite, revoke and lease another node");
        Window movedA = generatorA.window();
        assertNotEquals(generatorB.window().nodeId(), movedA.nodeId());
        assertEquals(intruder, await(mapB.get(String.valueOf(windowA.nodeId()))), "A must not overwrite back");
        awaitUntil(() -> recordedBy(mapB, movedA), "A's new lease must replicate to B");
        for (int index = 0; index < 1_000; index++) {
            long id = generatorA.nextId();
            assertIdInWindow(id, generatorA.window());
            assertTrue(allIds.add(id), "The re-leased generator must not reuse any earlier ID");
        }

        long lastA = generatorA.lastTimestamp();
        serviceA.close();
        assertThrows(IllegalStateException.class, generatorA::nextId);
        assertTrue(serviceA.start().failed(), "A closed service must not restart");
        awaitUntil(() -> {
            Window released = ClusterSnowflakeService.decode(movedA.nodeId(), await(mapB.get(String.valueOf(movedA.nodeId()))));
            return released != null && released.owner().equals(movedA.owner()) && released.until() >= lastA
                    && released.until() < generatorA.window().until();
        }, "B must observe A's lease shrunk to its last issued timestamp");
        await(vertxA.close());
        assertFalse(managerA.isActive());
        vertxA = null;

        for (int index = 0; index < 1_000; index++) {
            long id = generatorB.nextId(); // No cluster call on the local generation path.
            assertIdInWindow(id, generatorB.window());
            assertTrue(allIds.add(id), "The surviving generator must not reuse any earlier ID");
        }
        assertEquals(2 * IDS_PER_NODE + 2_000, allIds.size());
    }

    private Set<Long> generateConcurrently(SnowflakeGenerator generatorA, SnowflakeGenerator generatorB) throws Exception {
        workers = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        java.util.concurrent.Future<Set<Long>> idsA = workers.submit(() -> generate(generatorA, ready, start));
        java.util.concurrent.Future<Set<Long>> idsB = workers.submit(() -> generate(generatorB, ready, start));
        try {
            assertTrue(ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Both generators must be ready concurrently");
        } finally {
            start.countDown();
        }
        Set<Long> allIds = new HashSet<>(idsA.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertTrue(allIds.addAll(idsB.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
        return allIds;
    }

    private void proveRemoteRequestReply() throws Exception {
        String address = "uctp.zenoh.remote-only." + isolation;
        String nonce = UUID.randomUUID().toString();
        AtomicReference<String> receivedOnB = new AtomicReference<>();
        // Deliberately NO consumer on A at this address: local round-robin cannot satisfy the request.
        remoteConsumer = vertxB.eventBus().consumer(address, message -> {
            receivedOnB.set(message.body().getString("nonce"));
            message.reply(new JsonObject().put("node", managerB.getNodeId())
                    .put("nonce", message.body().getString("nonce")));
        });
        await(remoteConsumer.completion());
        awaitUntil(() -> managerA.getRegistrationsOf(address).stream()
                        .anyMatch(registration -> managerB.getNodeId().equals(registration.nodeId())),
                "A must see the remote-only consumer through the real Zenoh registration table");
        JsonObject reply = await(vertxA.eventBus().<JsonObject>request(address,
                new JsonObject().put("nonce", nonce), new DeliveryOptions().setSendTimeout(10_000))).body();
        assertEquals(managerB.getNodeId(), reply.getString("node"));
        assertEquals(nonce, reply.getString("nonce"));
        assertEquals(nonce, receivedOnB.get(), "The request handler must actually have executed on B");
        await(remoteConsumer.unregister());
        remoteConsumer = null;
    }

    /** True once the map holds this lease or a renewal of it (same node, owner and range origin). */
    private static boolean recordedBy(AsyncMap<String, String> map, Window lease) throws Exception {
        Window recorded = ClusterSnowflakeService.decode(lease.nodeId(), await(map.get(String.valueOf(lease.nodeId()))));
        return recorded != null && recorded.owner().equals(lease.owner()) && recorded.from() == lease.from();
    }

    private static Set<Long> generate(SnowflakeGenerator generator, CountDownLatch ready,
                                      CountDownLatch start) throws Exception {
        ready.countDown();
        assertTrue(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Set<Long> ids = new HashSet<>();
        for (int index = 0; index < IDS_PER_NODE; index++) {
            long id = generator.nextId();
            assertIdInWindow(id, generator.window());
            assertTrue(ids.add(id), "A single generator must not repeat IDs");
        }
        return ids;
    }

    private static void assertIdInWindow(long id, Window window) {
        assertTrue(id > 0);
        assertEquals(window.nodeId(), (int) ((id >>> 12) & 1023));
        long timestamp = (id >>> 22) + SnowflakeGenerator.EPOCH;
        assertTrue(timestamp >= window.from() && timestamp <= window.until(), "ID must stay inside its leased window");
    }

    @AfterEach
    void closeAllResources() {
        // assertAll executes every cleanup even if another close fails; no infrastructure failure is hidden.
        assertAll("Real integration resource cleanup",
                () -> { if (workers != null) {
                    workers.shutdownNow();
                    assertTrue(workers.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } },
                () -> { if (serviceA != null) serviceA.close(); },
                () -> { if (serviceB != null) serviceB.close(); },
                () -> { if (remoteConsumer != null) await(remoteConsumer.unregister()); },
                () -> closeCluster(vertxB, managerB),
                () -> closeCluster(vertxA, managerA),
                () -> { if (zenohConfig != null) Files.deleteIfExists(zenohConfig); });
    }

    private static void closeCluster(Vertx vertx, ZenohClusterManager manager) throws Exception {
        if (vertx != null) {
            await(vertx.close());
        } else if (manager != null && manager.getZenohSession() != null) {
            Promise<Void> left = Promise.promise();
            manager.leave(left);
            await(left.future());
        }
        if (manager != null) assertFalse(manager.isActive(), "Zenoh resources must leave the cluster");
    }

    private ZenohClusterProperties properties(String mode) {
        ZenohClusterProperties properties = new ZenohClusterProperties();
        properties.setAppName("uctp-snowflake-" + isolation);
        properties.setEnvironment("test-" + isolation);
        properties.setMode(mode);
        properties.setConfigFile(zenohConfig.toAbsolutePath().toString());
        return properties;
    }

    private static Vertx buildClustered(ZenohClusterManager manager) throws Exception {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost(HOST).setPort(0).setClusterPublicHost(HOST);
        return await(Vertx.builder().with(options).withClusterManager(manager).buildClustered());
    }

    private static int freeLoopbackPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(HOST))) {
            return socket.getLocalPort();
        }
    }

    private static void awaitUntil(Callable<Boolean> condition, String failureMessage) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.call() && System.nanoTime() < deadline) {
            Thread.sleep(50); // Bounded polling of asynchronous Zenoh discovery/replication.
        }
        assertTrue(condition.call(), failureMessage);
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
