package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Future;
import io.zenoh.Config;
import io.zenoh.Session;
import io.zenoh.Zenoh;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ZenohAsyncMap 单元测试 — 直查 Zenoh 模式。
 * <p>
 * 所有读写操作直接通过 Zenoh session 查询 storage backend。
 * 测试环境无 storage backend 插件，仅验证代码不崩溃、不抛异常。
 */
class ZenohAsyncMapTest {

    private static final KeyPrefixStrategy KEY_PREFIX = new KeyPrefixStrategy("test-app", "test-env");

    // ========== Null Session (graceful degradation) ==========

    @Nested
    @DisplayName("Null Session (graceful degradation)")
    class NullSessionTests {

        @Test
        @DisplayName("get returns null")
        void getReturnsNull() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test", KEY_PREFIX, null);
            assertNull(map.get("key1").await());
        }

        @Test
        @DisplayName("put succeeds silently")
        void putSucceeds() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test", KEY_PREFIX, null);
            map.put("key1", "value1").await();
        }

        @Test
        @DisplayName("remove returns null")
        void removeReturnsNull() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test", KEY_PREFIX, null);
            assertNull(map.remove("key1").await());
        }

        @Test
        @DisplayName("size returns 0")
        void sizeReturnsZero() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test", KEY_PREFIX, null);
            assertEquals(0, map.size().await());
        }

        @Test
        @DisplayName("entries returns empty map")
        void entriesEmpty() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test", KEY_PREFIX, null);
            assertTrue(map.entries().await().isEmpty());
        }
    }

    // ========== Zenoh Session (no storage backend, graceful) ==========

    @Nested
    @DisplayName("Zenoh Session (no storage backend)")
    class SessionTests {

        private Session session;

        @BeforeEach
        void setUp() throws Exception {
            Config config = Config.loadDefault();
            config.insertJson5("mode", "\"peer\"");
            session = Zenoh.open(config);
        }

        @AfterEach
        void tearDown() {
            if (session != null) session.close();
        }

        @Test
        @DisplayName("put does not throw")
        void putNoException() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertDoesNotThrow(() -> map.put("key1", "value1").await());
        }

        @Test
        @DisplayName("get non-existent returns null")
        void getNonExistent() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertNull(map.get("missing").await());
        }

        @Test
        @DisplayName("remove does not throw")
        void removeNoException() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            map.put("key1", "value1").await();
            assertDoesNotThrow(() -> map.remove("key1").await());
        }

        @Test
        @DisplayName("putIfAbsent does not throw")
        void putIfAbsentNoException() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertDoesNotThrow(() -> map.putIfAbsent("key1", "value1").await());
        }

        @Test
        @DisplayName("replace non-existent returns null")
        void replaceNonExistent() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertNull(map.replace("missing", "value").await());
        }

        @Test
        @DisplayName("removeIfPresent non-existent returns false")
        void removeIfPresentNonExistent() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertFalse(map.removeIfPresent("missing", "value").await());
        }

        @Test
        @DisplayName("replaceIfPresent non-existent returns false")
        void replaceIfPresentNonExistent() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertFalse(map.replaceIfPresent("missing", "old", "new").await());
        }

        @Test
        @DisplayName("size returns 0 when no storage")
        void sizeEmpty() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertEquals(0, map.size().await());
        }

        @Test
        @DisplayName("keys returns empty set")
        void keysEmpty() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertTrue(map.keys().await().isEmpty());
        }

        @Test
        @DisplayName("values returns empty list")
        void valuesEmpty() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertTrue(map.values().await().isEmpty());
        }

        @Test
        @DisplayName("entries returns empty map")
        void entriesEmpty() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertTrue(map.entries().await().isEmpty());
        }

        @Test
        @DisplayName("clear does not throw")
        void clearNoException() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertDoesNotThrow(() -> map.clear().await());
        }

        @Test
        @DisplayName("put with TTL does not throw")
        void putWithTtl() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertDoesNotThrow(() -> map.put("key1", "value1", 5000L).await());
        }

        @Test
        @DisplayName("putIfAbsent with TTL does not throw")
        void putIfAbsentWithTtl() {
            ZenohAsyncMap<String, String> map = new ZenohAsyncMap<>("test-map", KEY_PREFIX, session);
            assertDoesNotThrow(() -> map.putIfAbsent("key1", "value1", 5000L).await());
        }
    }
}
