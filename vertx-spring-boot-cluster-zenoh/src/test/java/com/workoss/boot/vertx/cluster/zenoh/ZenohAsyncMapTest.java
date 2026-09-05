package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.Session;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ZenohAsyncMap 测试。
 * <p>
 * 不依赖任何 storage backend 插件：本地用例验证内存副本语义，复制用例用两个真实 Zenoh session
 * 验证 Subscriber 复制与 Queryable bootstrap 两条链路。
 */
class ZenohAsyncMapTest {

    private static final KeyPrefixStrategy KEY_PREFIX = new KeyPrefixStrategy("test-app", "test-env");

    // ========== Local replica (no Zenoh session) ==========

    @Nested
    @DisplayName("Local replica without a session")
    class LocalStoreTests {

        private Vertx vertx;
        private ZenohMapStore store;
        private ZenohAsyncMap<String, String> map;

        @BeforeEach
        void setUp() {
            vertx = Vertx.vertx();
            store = new ZenohMapStore(vertx, KEY_PREFIX, "node-local");
            store.start(null);
            map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, store);
        }

        @AfterEach
        void tearDown() {
            store.stop();
            vertx.close().await();
        }

        @Test
        @DisplayName("put then get round-trips")
        void putThenGet() {
            map.put("key1", "value1").await();
            assertEquals("value1", map.get("key1").await());
        }

        @Test
        @DisplayName("get on a missing key returns null")
        void getMissing() {
            assertNull(map.get("missing").await());
        }

        @Test
        @DisplayName("put overwrites and returns the old value on remove")
        void putOverwrites() {
            map.put("key1", "v1").await();
            map.put("key1", "v2").await();
            assertEquals("v2", map.get("key1").await());
            assertEquals("v2", map.remove("key1").await());
            assertNull(map.get("key1").await());
            assertNull(map.remove("key1").await());
        }

        @Test
        @DisplayName("putIfAbsent keeps the first writer")
        void putIfAbsent() {
            assertNull(map.putIfAbsent("key1", "first").await());
            assertEquals("first", map.putIfAbsent("key1", "second").await());
            assertEquals("first", map.get("key1").await());
        }

        @Test
        @DisplayName("removeIfPresent only removes on an exact value match")
        void removeIfPresent() {
            map.put("key1", "value1").await();
            assertFalse(map.removeIfPresent("key1", "other").await());
            assertTrue(map.removeIfPresent("key1", "value1").await());
            assertNull(map.get("key1").await());
            assertFalse(map.removeIfPresent("missing", "value").await());
        }

        @Test
        @DisplayName("replace only touches an existing key")
        void replace() {
            assertNull(map.replace("missing", "value").await());
            assertNull(map.get("missing").await());

            map.put("key1", "v1").await();
            assertEquals("v1", map.replace("key1", "v2").await());
            assertEquals("v2", map.get("key1").await());
        }

        @Test
        @DisplayName("replaceIfPresent requires the old value to match")
        void replaceIfPresent() {
            map.put("key1", "v1").await();
            assertFalse(map.replaceIfPresent("key1", "wrong", "v2").await());
            assertEquals("v1", map.get("key1").await());
            assertTrue(map.replaceIfPresent("key1", "v1", "v2").await());
            assertEquals("v2", map.get("key1").await());
            assertFalse(map.replaceIfPresent("missing", "a", "b").await());
        }

        @Test
        @DisplayName("size, keys, values and entries see every entry")
        void bulkAccessors() {
            map.put("a", "1").await();
            map.put("b", "2").await();

            assertEquals(2, map.size().await());
            assertEquals(java.util.Set.of("a", "b"), map.keys().await());
            assertTrue(map.values().await().containsAll(List.of("1", "2")));
            assertEquals(java.util.Map.of("a", "1", "b", "2"), map.entries().await());
        }

        @Test
        @DisplayName("clear empties this map only")
        void clearOnlyAffectsOwnMap() {
            ZenohAsyncMap<String, String> other = new ZenohAsyncMap<>("other-map", KEY_PREFIX, store);
            map.put("a", "1").await();
            other.put("b", "2").await();

            map.clear().await();

            assertEquals(0, map.size().await());
            assertEquals(1, other.size().await());
        }

        @Test
        @DisplayName("entry with a ttl disappears once it expires")
        void ttlExpiry() throws InterruptedException {
            map.put("key1", "value1", 100L).await();
            assertEquals("value1", map.get("key1").await());

            Thread.sleep(250L);

            assertNull(map.get("key1").await());
            assertEquals(0, map.size().await());
        }

        @Test
        @DisplayName("putIfAbsent treats an expired entry as absent")
        void ttlExpiryFreesTheKey() throws InterruptedException {
            map.put("key1", "value1", 100L).await();
            Thread.sleep(250L);

            assertNull(map.putIfAbsent("key1", "value2").await());
            assertEquals("value2", map.get("key1").await());
        }

        @Test
        @DisplayName("non-String keys are encoded into the zenoh key")
        void nonStringKeys() {
            ZenohAsyncMap<Integer, String> intMap = new ZenohAsyncMap<>("int-map", KEY_PREFIX, store);
            intMap.put(42, "answer").await();
            assertEquals("answer", intMap.get(42).await());
            assertEquals(1, intMap.size().await());
        }
    }

    // ========== Replication across two Zenoh sessions ==========

    @Nested
    @DisplayName("Replication between two nodes")
    class ReplicationTests {

        private Vertx vertx;
        private int port;
        private Session sessionA;
        private Session sessionB;
        private ZenohMapStore storeA;
        private ZenohMapStore storeB;
        private ZenohAsyncMap<String, String> mapA;
        private ZenohAsyncMap<String, String> mapB;

        @BeforeEach
        void setUp() {
            vertx = Vertx.vertx();
            port = freePort();

            Config configA = Config.defaultConfig();
            configA.setMode("peer");
            configA.setListenEndpoints(List.of("tcp/127.0.0.1:" + port));
            sessionA = new Session(configA);
            configA.close();

            storeA = new ZenohMapStore(vertx, KEY_PREFIX, "node-aaaaaaaa");
            storeA.start(sessionA);
            mapA = new ZenohAsyncMap<>("shared", KEY_PREFIX, storeA);

            Config configB = Config.defaultConfig();
            configB.setMode("client");
            configB.setConnectEndpoints(List.of("tcp/127.0.0.1:" + port));
            sessionB = new Session(configB);
            configB.close();

            storeB = new ZenohMapStore(vertx, KEY_PREFIX, "node-bbbbbbbb");
            storeB.start(sessionB);
            mapB = new ZenohAsyncMap<>("shared", KEY_PREFIX, storeB);
        }

        @AfterEach
        void tearDown() {
            if (storeB != null) storeB.stop();
            if (storeA != null) storeA.stop();
            if (sessionB != null) {
                try { sessionB.closeSession(); } catch (Exception ignored) {}
            }
            if (sessionA != null) {
                try { sessionA.closeSession(); } catch (Exception ignored) {}
            }
            if (vertx != null) vertx.close().await();
        }

        @Test
        @DisplayName("a write on node A becomes visible on node B")
        void putPropagates() {
            mapA.put("key1", "value1").await();
            assertTrue(awaitUntil(() -> "value1".equals(mapB.get("key1").await())),
                    "node B never observed the write from node A");
        }

        @Test
        @DisplayName("a remove on node A becomes visible on node B")
        void removePropagates() {
            mapA.put("key1", "value1").await();
            assertTrue(awaitUntil(() -> "value1".equals(mapB.get("key1").await())));

            mapA.remove("key1").await();
            assertTrue(awaitUntil(() -> mapB.get("key1").await() == null),
                    "node B never observed the delete from node A");
        }

        @Test
        @DisplayName("a node that joins later bootstraps from a peer queryable")
        void lateJoinerBootstrapsFromQueryable() {
            mapA.put("early", "state").await();
            assertTrue(awaitUntil(() -> "state".equals(mapB.get("early").await())));

            // A third node comes up after the write already happened. Live replication cannot help
            // it, so the only way it can see the value is by querying a peer during start().
            Session sessionC = null;
            ZenohMapStore storeC = new ZenohMapStore(vertx, KEY_PREFIX, "node-cccccccc");
            try {
                Config configC = Config.defaultConfig();
                configC.setMode("client");
                configC.setConnectEndpoints(List.of("tcp/127.0.0.1:" + port));
                sessionC = new Session(configC);
                configC.close();

                storeC.start(sessionC);
                ZenohAsyncMap<String, String> mapC = new ZenohAsyncMap<>("shared", KEY_PREFIX, storeC);

                assertEquals("state", mapC.get("early").await(),
                        "the joining node did not bootstrap existing state from a peer queryable");
            } finally {
                storeC.stop();
                if (sessionC != null) {
                    try { sessionC.closeSession(); } catch (Exception ignored) {}
                }
            }
        }

        @Test
        @DisplayName("putIfAbsent on one node is visible to the other")
        void putIfAbsentPropagates() {
            assertNull(mapA.putIfAbsent("key1", "value1").await());
            assertTrue(awaitUntil(() -> "value1".equals(mapB.get("key1").await())));
        }

        @Test
        @DisplayName("counters increment across nodes")
        void counterIsShared() {
            ZenohCounter counterA = new ZenohCounter("hits", KEY_PREFIX, storeA);
            ZenohCounter counterB = new ZenohCounter("hits", KEY_PREFIX, storeB);

            assertEquals(1L, counterA.incrementAndGet().await());
            assertTrue(awaitUntil(() -> counterB.get().await() == 1L),
                    "node B never observed the counter increment");
        }

        private static boolean awaitUntil(BooleanSupplier condition) {
            long deadline = System.currentTimeMillis() + 15_000L;
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
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port available for the replication test", e);
        }
    }
}
