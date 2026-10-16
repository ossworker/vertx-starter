package com.workoss.boot.zenoh.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ZenohSharedMap} semantics on a local store (no session). No JNI involved.
 */
@DisplayName("ZenohSharedMap local (no session)")
class ZenohSharedMapLocalTest {

    private ScheduledExecutorService scheduler;
    private ZenohMapStore store;
    private ZenohSharedMap<String, String> map;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        store = new ZenohMapStore(new KeyPrefixStrategy("app", "test"), "node-x", scheduler,
                Executors.newFixedThreadPool(2));
        store.start(null);
        map = new ZenohSharedMap<>("shared", new KeyPrefixStrategy("app", "test"), store,
                ValueCodec.jdk());
    }

    @AfterEach
    void tearDown() {
        store.stop();
        scheduler.shutdownNow();
    }

    @Test
    @DisplayName("basic put/get/remove")
    void basic() {
        map.put("a", "1");
        assertEquals("1", map.get("a"));
        assertTrue(map.containsKey("a"));
        assertEquals("1", map.remove("a"));
        assertNull(map.get("a"));
        assertFalse(map.containsKey("a"));
    }

    @Test
    @DisplayName("putIfAbsent returns the prior value")
    void putIfAbsent() {
        assertNull(map.putIfAbsent("a", "1"));
        assertEquals("1", map.putIfAbsent("a", "2"));
        assertEquals("1", map.get("a"));
    }

    @Test
    @DisplayName("size / isEmpty / keys / values / entries")
    void views() {
        map.put("a", "1");
        map.put("b", "2");
        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
        assertEquals(Set.of("a", "b"), map.keys());
        assertTrue(map.values().containsAll(Set.of("1", "2")));
        assertEquals(Map.of("a", "1", "b", "2"), map.entries());
    }

    @Test
    @DisplayName("clear empties only this map")
    void clearIsolation() {
        ZenohSharedMap<String, String> other = new ZenohSharedMap<>("other",
                new KeyPrefixStrategy("app", "test"), store, ValueCodec.jdk());
        map.put("a", "1");
        other.put("b", "2");
        map.clear();
        assertEquals(0, map.size());
        assertEquals(1, other.size());
    }

    @Test
    @DisplayName("TTL entry disappears after expiry")
    void ttlExpiry() throws InterruptedException {
        map.put("k", "v", 200L);
        assertEquals("v", map.get("k"));
        Thread.sleep(400L);
        assertNull(map.get("k"));
        assertEquals(0, map.size());
    }

    @Test
    @DisplayName("non-String keys are encoded into the zenoh key")
    void nonStringKeys() {
        ZenohSharedMap<Integer, String> intMap = new ZenohSharedMap<>("int-map",
                new KeyPrefixStrategy("app", "test"), store, ValueCodec.jdk());
        intMap.put(42, "answer");
        assertEquals("answer", intMap.get(42));
        assertEquals(1, intMap.size());
        assertEquals(Set.of(42), intMap.keys());
    }
}
