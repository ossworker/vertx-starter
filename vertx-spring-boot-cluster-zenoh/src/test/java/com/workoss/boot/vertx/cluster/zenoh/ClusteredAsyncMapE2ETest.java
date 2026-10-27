package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双节点集群 E2E:证明通过 Vert.x 标准 API {@code vertx.sharedData().getAsyncMap(...)} 取得的
 * AsyncMap 真正跨节点共享。
 * <p>
 * 这一层是 {@link ZenohAsyncMapTest} 没覆盖到的地方——后者只在两个裸 {@code ZenohMapStore}+原生
 * {@code Session} 上验证了复制,而生产代码是通过集群管理器走 {@code sharedData} 拿 map 的。那条链路还包含:
 * <ul>
 *   <li>{@code ZenohClusterManager.getAsyncMap} 返回绑定本节点 {@code ZenohMapStore} 的 {@code ZenohAsyncMap};</li>
 *   <li>两个节点必须用相同的 appName+environment,才能落到同一个 {@code zenoh/{app}/{env}/cluster/**} 键空间;</li>
 *   <li>每个节点的 store 在 {@code join()} 里用真实 Zenoh session 起 Subscriber(收写)+ Queryable(供晚到节点 bootstrap);</li>
 *   <li>写入先落到本地副本,再由 store 经 {@code session.put/delete} 异步复制给对端。</li>
 * </ul>
 * 任一环不通,A 节点 put 的值 B 节点就永远读不到。本测试用两个真实 {@code buildClustered()} 实例(与生产拓扑一致)
 * 把整条链路端到端跑通。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusteredAsyncMapE2ETest {

    /** Isolates this test from any other keyspace on the machine. */
    private static final String APP = "e2e-asyncmap-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String MAP_NAME = "multi-node-shared";
    private static final String STREAM_MAP = "multi-node-stream";
    private static final long TIMEOUT_MS = 30_000L;

    private static Vertx vertxA;
    private static Vertx vertxB;
    private static ZenohClusterManager managerA;
    private static ZenohClusterManager managerB;
    private static int zenohPort;

    @BeforeAll
    static void startCluster() throws Exception {
        zenohPort = freePort();

        // Node A: Zenoh peer (listens).
        ZenohClusterProperties propsA = new ZenohClusterProperties();
        propsA.setAppName(APP);
        propsA.setEnvironment("test");
        propsA.setMode("peer");
        propsA.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        managerA = new ZenohClusterManager(propsA);

        // Node B: Zenoh client (connects to A). Same app+env => same keyspace => shared map.
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
        options.getEventBusOptions()
                .setHost("127.0.0.1")
                .setPort(eventBusPort);
        return Vertx.builder()
                .with(options)
                .withClusterManager(manager)
                .buildClustered()
                .toCompletionStage()
                .toCompletableFuture()
                .join();
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
                // Best effort.
            }
        }
    }

    // ========== Tests ==========

    @Test
    @Order(1)
    @DisplayName("both nodes report two members in the cluster")
    void nodesDiscoverEachOther() {
        assertEquals(2, managerA.getNodes().size(), "node A: " + managerA.getNodes());
        assertEquals(2, managerB.getNodes().size(), "node B: " + managerB.getNodes());
        assertFalse(managerA.getNodeId().equals(managerB.getNodeId()), "node ids must differ");
    }

    @Test
    @Order(2)
    @DisplayName("put on node A becomes visible on node B  ← the core claim")
    void putPropagatesToPeer() {
        AsyncMap<String, String> mapA = vertxA.sharedData().<String, String>getAsyncMap(MAP_NAME).await();
        AsyncMap<String, String> mapB = vertxB.sharedData().<String, String>getAsyncMap(MAP_NAME).await();

        mapA.put("key1", "value1").await();

        assertTrue(awaitUntil(() -> "value1".equals(mapB.get("key1").await())),
                "node B never observed the value that node A put into the shared async map");
    }

    @Test
    @Order(3)
    @DisplayName("remove on node A becomes visible on node B")
    void removePropagatesToPeer() {
        AsyncMap<String, String> mapA = vertxA.sharedData().<String, String>getAsyncMap(MAP_NAME).await();
        AsyncMap<String, String> mapB = vertxB.sharedData().<String, String>getAsyncMap(MAP_NAME).await();

        mapA.put("key2", "value2").await();
        assertTrue(awaitUntil(() -> "value2".equals(mapB.get("key2").await())));

        mapA.remove("key2").await();
        assertTrue(awaitUntil(() -> mapB.get("key2").await() == null),
                "node B never observed the delete that node A issued on the shared async map");
    }

    @Test
    @Order(4)
    @DisplayName("putIfAbsent on node A is visible on node B")
    void putIfAbsentPropagatesToPeer() {
        AsyncMap<String, String> mapA = vertxA.sharedData().<String, String>getAsyncMap(MAP_NAME).await();
        AsyncMap<String, String> mapB = vertxB.sharedData().<String, String>getAsyncMap(MAP_NAME).await();

        assertNull(mapA.putIfAbsent("key3", "value3").await());
        assertTrue(awaitUntil(() -> "value3".equals(mapB.get("key3").await())),
                "node B never observed the putIfAbsent value from node A");
    }

    @Test
    @Order(5)
    @DisplayName("a structured (Serializable) value survives the cross-node read")
    void structuredValueSurvivesWire() {
        AsyncMap<String, Record> mapA = vertxA.sharedData().<String, Record>getAsyncMap(MAP_NAME).await();
        AsyncMap<String, Record> mapB = vertxB.sharedData().<String, Record>getAsyncMap(MAP_NAME).await();

        Record written = new Record("alice", 42, List.of("x", "y"));
        mapA.put("person", written).await();

        assertTrue(awaitUntil(() -> {
            Record r = mapB.get("person").await();
            return r != null && "alice".equals(r.name()) && r.age() == 42 && r.tags().equals(List.of("x", "y"));
        }), "the structured value put on node A did not round-trip to node B intact");
    }

    @Test
    @Order(6)
    @DisplayName("a late-joining node bootstraps the existing map via a peer queryable")
    void lateJoinerBootstrapsSharedMap() throws Exception {
        AsyncMap<String, String> mapA = vertxA.sharedData().<String, String>getAsyncMap(MAP_NAME).await();
        mapA.put("early", "state").await();
        assertTrue(awaitUntil(() -> "state".equals(
                vertxB.sharedData().<String, String>getAsyncMap(MAP_NAME).await().get("early").await())));

        // A third node comes up after the write already happened. Live replication cannot help it,
        // so the only way it can see "early" is by querying a peer's queryable during join().
        ZenohClusterProperties propsC = new ZenohClusterProperties();
        propsC.setAppName(APP);
        propsC.setEnvironment("test");
        propsC.setMode("client");
        propsC.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));

        ZenohClusterManager managerC = new ZenohClusterManager(propsC);
        Vertx vertxC = null;
        try {
            vertxC = buildClustered(managerC, freePort());
            assertTrue(awaitUntil(() -> managerC.getNodes().size() >= 3),
                    "the late-joining node never discovered the existing cluster");

            AsyncMap<String, String> mapC =
                    vertxC.sharedData().<String, String>getAsyncMap(MAP_NAME).await();
            assertEquals("state", mapC.get("early").await(),
                    "the late-joining node did not bootstrap the shared map from a peer queryable");
        } finally {
            closeQuietly(vertxC);
        }
    }

    @Test
    @Order(7)
    @DisplayName("getCounter on one node is observed on the peer  ← shared state beyond the map")
    void counterIsSharedAcrossNodes() {
        Counter counterA = vertxA.sharedData().getCounter("hits").await();
        Counter counterB = vertxB.sharedData().getCounter("hits").await();

        assertEquals(1L, counterA.incrementAndGet().await());
        assertTrue(awaitUntil(() -> {
            try {
                return counterB.get().await() == 1L;
            } catch (Exception e) {
                return false;
            }
        }), "node B never observed the counter increment that node A issued");
    }

    @Test
    @Order(8)
    @DisplayName("node A continuously puts a stream; node B polls until it converges to the latest value")
    void continuousPutConvergesOnPeer() throws Exception {
        AsyncMap<String, String> mapA = vertxA.sharedData().<String, String>getAsyncMap(STREAM_MAP).await();
        AsyncMap<String, String> mapB = vertxB.sharedData().<String, String>getAsyncMap(STREAM_MAP).await();

        final int TOTAL = 50;
        final String finalValue = String.format("%03d", TOTAL - 1);
        final boolean[] writeFailed = {false};

        // Node A streams TOTAL sequential values onto one key as fast as it can,
        // concurrently with B's polling below. A 5ms gap between writes lets the
        // store flush each update to the peer instead of coalescing the whole burst.
        Thread writer = new Thread(() -> {
            try {
                for (int i = 0; i < TOTAL; i++) {
                    mapA.put("stream", String.format("%03d", i)).await();
                    Thread.sleep(5L);
                }
            } catch (Exception e) {
                writeFailed[0] = true;
            }
        });
        writer.start();

        // Node B polls the key until it observes the final value A wrote. We track the
        // highest sequence seen and watch for any regression (observing a lower value than
        // one already seen). Under a single-writer key, Zenoh last-write-wins must never
        // let a stale value overtake a newer one, so regression would be a real replica bug.
        int[] maxSeen = {-1};
        String[] lastRead = {null};
        boolean[] regressed = {false};
        boolean converged = awaitUntil(() -> {
            try {
                String v = mapB.get("stream").await();
                if (v != null) {
                    int n = Integer.parseInt(v);
                    maxSeen[0] = Math.max(maxSeen[0], n);
                    if (lastRead[0] != null && !lastRead[0].equals(v)
                            && n < Integer.parseInt(lastRead[0])) {
                        regressed[0] = true;
                    }
                    lastRead[0] = v;
                }
                return finalValue.equals(v);
            } catch (Exception e) {
                return false;
            }
        });

        writer.join();
        assertFalse(writeFailed[0], "node A failed while streaming values into the stream async map");
        assertFalse(regressed[0],
                "node B observed the stream value regress (stale value overtook a newer one) — replica not monotonic");
        assertTrue(converged,
                "node B never converged to the latest value A streamed (last observed = "
                        + maxSeen[0] + ", expected = " + (TOTAL - 1) + ")");
        assertEquals(TOTAL - 1, maxSeen[0],
                "node B did not observe the final value of the stream; replication dropped the tail");
    }

    // --- helpers ---

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
            throw new IllegalStateException("No free port for the clustered async-map E2E", e);
        }
    }

    /** A value that crosses the cluster the same way application state must: via Java serialization
     *  performed inside {@link ZenohAsyncMap} (no Vert.x codec required). */
    public static final class Record implements Serializable {
        private final String name;
        private final int age;
        private final List<String> tags;

        public Record(String name, int age, List<String> tags) {
            this.name = name;
            this.age = age;
            this.tags = tags;
        }

        public String name() {
            return name;
        }

        public int age() {
            return age;
        }

        public List<String> tags() {
            return tags;
        }
    }
}
