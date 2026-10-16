package com.workoss.boot.zenoh.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link ZenohMapStore} local semantics with no Zenoh session ({@code start(null)}): an ordinary
 * in-process map. These tests never touch JNI, so they run without the native library.
 */
@DisplayName("ZenohMapStore local (no session)")
class ZenohMapStoreLocalTest {

    private ScheduledExecutorService scheduler;
    private ZenohMapStore store;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        store = new ZenohMapStore(new KeyPrefixStrategy("app", "test"), "node-x", scheduler,
                Executors.newFixedThreadPool(2));
        store.start(null);
    }

    @AfterEach
    void tearDown() {
        store.stop();
        scheduler.shutdownNow();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("put then get round-trips the value")
    void putThenGet() {
        store.putRaw("k1", bytes("v1"), 0L);
        assertArrayEquals(bytes("v1"), store.getRaw("k1"));
    }

    @Test
    @DisplayName("putRawIfAbsent returns the existing value when present")
    void putIfAbsentExisting() {
        store.putRaw("k1", bytes("v1"), 0L);
        assertArrayEquals(bytes("v1"), store.putRawIfAbsent("k1", bytes("v2"), 0L));
        assertArrayEquals(bytes("v1"), store.getRaw("k1"));
    }

    @Test
    @DisplayName("putRawIfAbsent stores and returns null when absent")
    void putIfAbsentAbsent() {
        assertNull(store.putRawIfAbsent("k2", bytes("v2"), 0L));
        assertArrayEquals(bytes("v2"), store.getRaw("k2"));
    }

    @Test
    @DisplayName("removeRaw returns the removed value")
    void removeRaw() {
        store.putRaw("k1", bytes("v1"), 0L);
        assertArrayEquals(bytes("v1"), store.removeRaw("k1"));
        assertNull(store.getRaw("k1"));
    }

    @Test
    @DisplayName("computeRaw derives and stores a new value")
    void computeRaw() {
        assertNull(store.getRaw("counter"));
        store.computeRaw("counter", cur -> (cur == null ? bytes("1") : bytes(Integer.toString(
                Integer.parseInt(new String(cur, StandardCharsets.UTF_8)) + 1))));
        assertArrayEquals(bytes("1"), store.getRaw("counter"));
        store.computeRaw("counter", cur -> bytes(Integer.toString(
                Integer.parseInt(new String(cur, StandardCharsets.UTF_8)) + 1)));
        assertArrayEquals(bytes("2"), store.getRaw("counter"));
    }

    @Test
    @DisplayName("TTL entry disappears after it expires")
    void ttlExpiry() throws InterruptedException {
        store.putRaw("ttl", bytes("v"), 200L);
        assertArrayEquals(bytes("v"), store.getRaw("ttl"));
        Thread.sleep(400L);
        assertNull(store.getRaw("ttl"));
    }

    @Test
    @DisplayName("clearRange removes only entries under the prefix")
    void clearRange() {
        store.putRaw("scope/a", bytes("1"), 0L);
        store.putRaw("scope/b", bytes("2"), 0L);
        store.putRaw("other/c", bytes("3"), 0L);
        store.clearRange("scope/");
        assertNull(store.getRaw("scope/a"));
        assertNull(store.getRaw("scope/b"));
        assertArrayEquals(bytes("3"), store.getRaw("other/c"));
    }

    @Test
    @DisplayName("scanRange returns only the in-prefix live entries")
    void scanRange() {
        store.putRaw("scope/a", bytes("1"), 0L);
        store.putRaw("scope/b", bytes("2"), 0L);
        store.putRaw("other/c", bytes("3"), 0L);
        assertEquals(2, store.scanRange("scope/").size());
        assertEquals(1, store.scanRange("other/").size());
    }
}
