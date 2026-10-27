package com.workoss.boot.zenoh.cluster.internal.store;

import com.workoss.boot.zenoh.cluster.ClusterCounter;
import com.workoss.boot.zenoh.cluster.ClusterLock;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.internal.KeySpace;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Shared map, counter and lock on a local replica")
class StructuresLocalTest {

    private static final String PREFIX = new KeySpace("app", "test").storePrefix();

    private static ReplicatedStore newStore(String nodeId) {
        return new ReplicatedStore(PREFIX, nodeId, new HybridClock(), 60_000L);
    }

    /**
     * Two replicas wired to each other synchronously, standing in for the network.
     */
    private static ReplicatedStore[] linkedPair() {
        ReplicatedStore a = newStore("node-a");
        ReplicatedStore b = newStore("node-b");
        a.setSink(b::applyRemote);
        b.setSink(a::applyRemote);
        return new ReplicatedStore[]{a, b};
    }

    // --- Shared map ---

    @Test
    @DisplayName("keys containing Zenoh separators and wildcards stay distinct and round-trip")
    void specialCharacterKeys() {
        ReplicatedStore store = newStore("n");
        SharedMap<String, String> map = new DefaultSharedMap<>("m", () -> store, ValueCodec.utf8());
        List<String> keys = List.of("a/b", "a", "*", "**", "?x", "$*", "a%2Fb", "", "中文 key");
        keys.forEach(key -> map.put(key, "v:" + key));

        assertEquals(Set.copyOf(keys), map.keySet());
        keys.forEach(key -> assertEquals("v:" + key, map.get(key)));
    }

    @Test
    @DisplayName("map names are isolated even when one looks like a path prefix of another")
    void mapNameIsolation() {
        ReplicatedStore store = newStore("n");
        SharedMap<String, String> parent = new DefaultSharedMap<>("a", () -> store, ValueCodec.utf8());
        SharedMap<String, String> child = new DefaultSharedMap<>("a/b", () -> store, ValueCodec.utf8());
        parent.put("k", "parent");
        child.put("k", "child");
        child.put("x", "child");

        parent.clear();
        assertTrue(parent.isEmpty());
        assertEquals(Map.of("k", "child", "x", "child"), child.entries());
    }

    @Test
    @DisplayName("non-String keys are supported")
    void nonStringKeys() {
        ReplicatedStore store = newStore("n");
        SharedMap<Long, String> map = new DefaultSharedMap<>("m", () -> store, ValueCodec.jdk());
        map.put(42L, "answer");
        map.put(-1L, "minus");
        assertEquals("answer", map.get(42L));
        assertEquals(Set.of(42L, -1L), map.keySet());
    }

    @Test
    @DisplayName("conditional operations follow ConcurrentMap semantics")
    void conditionalOperations() {
        ReplicatedStore store = newStore("n");
        SharedMap<String, String> map = new DefaultSharedMap<>("m", () -> store, ValueCodec.utf8());
        assertNull(map.putIfAbsent("k", "1"));
        assertEquals("1", map.putIfAbsent("k", "2"));
        assertFalse(map.replace("k", "x", "3"));
        assertTrue(map.replace("k", "1", "3"));
        assertNull(map.replace("missing", "v"));
        assertFalse(map.remove("k", "1"));
        assertTrue(map.remove("k", "3"));
        assertNull(map.remove("k"));
        assertEquals(0, map.size());
    }

    @Test
    @DisplayName("concurrent putIfAbsent on one node has exactly one winner")
    void putIfAbsentSingleWinner() throws Exception {
        ReplicatedStore store = newStore("n");
        SharedMap<String, String> map = new DefaultSharedMap<>("m", () -> store, ValueCodec.utf8());
        AtomicInteger winners = new AtomicInteger();
        runConcurrently(16, i -> {
            if (map.putIfAbsent("k", "t" + i) == null) {
                winners.incrementAndGet();
            }
        });
        assertEquals(1, winners.get());
    }

    // --- Counter ---

    @Test
    @DisplayName("concurrent increments on two nodes are never lost")
    void counterNoLostIncrements() throws Exception {
        ReplicatedStore[] pair = linkedPair();
        ClusterCounter a = new DefaultClusterCounter("c", "node-a", () -> pair[0]);
        ClusterCounter b = new DefaultClusterCounter("c", "node-b", () -> pair[1]);
        runConcurrently(8, i -> {
            ClusterCounter counter = i % 2 == 0 ? a : b;
            for (int n = 0; n < 500; n++) {
                counter.incrementAndGet();
            }
        });
        assertEquals(4_000L, a.get());
        assertEquals(4_000L, b.get());
    }

    @Test
    @DisplayName("counter arithmetic and compareAndSet")
    void counterOperations() {
        ReplicatedStore[] pair = linkedPair();
        ClusterCounter a = new DefaultClusterCounter("c", "node-a", () -> pair[0]);
        ClusterCounter b = new DefaultClusterCounter("c", "node-b", () -> pair[1]);
        assertEquals(5L, a.addAndGet(5));
        assertEquals(5L, b.getAndIncrement());
        assertEquals(5L, b.decrementAndGet());
        assertFalse(a.compareAndSet(4, 100));
        assertTrue(a.compareAndSet(5, 100));
        assertEquals(100L, b.get());
    }

    // --- Lock ---

    @Test
    @DisplayName("a lock excludes other threads of the same node and is not re-entrant")
    void lockExcludesThreadsOfSameNode() throws Exception {
        ReplicatedStore store = newStore("node-a");
        LockService locks = new LockService("node-a", "inc-1", (node, inc) -> true, store, () -> true);
        AtomicInteger acquired = new AtomicInteger();
        runConcurrently(8, i -> locks.tryAcquire("job", Duration.ZERO).ifPresent(lock -> acquired.incrementAndGet()));
        assertEquals(1, acquired.get());
        assertTrue(locks.tryAcquire("job", Duration.ZERO).isEmpty(), "not re-entrant");
    }

    @Test
    @DisplayName("release is conditional and frees the lock for the next caller")
    void lockRelease() {
        ReplicatedStore[] pair = linkedPair();
        LockService a = new LockService("node-a", "inc-a", (node, inc) -> true, pair[0], () -> true);
        LockService b = new LockService("node-b", "inc-b", (node, inc) -> true, pair[1], () -> true);
        ClusterLock lock = a.tryAcquire("job", Duration.ZERO).orElseThrow();
        assertTrue(lock.isHeld());
        assertTrue(b.tryAcquire("job", Duration.ofMillis(50)).isEmpty());

        lock.release();
        assertFalse(lock.isHeld());
        try (ClusterLock next = b.tryAcquire("job", Duration.ZERO).orElseThrow()) {
            assertTrue(next.isHeld());
            lock.release(); // a stale handle must not release someone else's lock
            assertTrue(next.isHeld());
        }
    }

    @Test
    @DisplayName("a lock held by a dead incarnation is reclaimed")
    void deadHolderReclaimed() {
        ReplicatedStore[] pair = linkedPair();
        AtomicBoolean aAlive = new AtomicBoolean(true);
        LockService a = new LockService("node-a", "inc-a", (node, inc) -> true, pair[0], () -> true);
        LockService b = new LockService("node-b", "inc-b",
                (node, inc) -> !"node-a".equals(node) || aAlive.get(), pair[1], () -> true);
        a.tryAcquire("job", Duration.ZERO).orElseThrow();
        assertTrue(b.tryAcquire("job", Duration.ZERO).isEmpty());

        aAlive.set(false);
        Optional<ClusterLock> taken = b.tryAcquire("job", Duration.ZERO);
        assertTrue(taken.isPresent());
        assertTrue(taken.get().isHeld());
    }

    @Test
    @DisplayName("tryAcquire with a timeout waits for the holder to release")
    void lockWaitsForRelease() throws Exception {
        ReplicatedStore store = newStore("node-a");
        LockService locks = new LockService("node-a", "inc", (node, inc) -> true, store, () -> true);
        ClusterLock first = locks.tryAcquire("job", Duration.ZERO).orElseThrow();
        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            Future<Optional<ClusterLock>> waiter = pool.submit(() -> locks.tryAcquire("job", Duration.ofSeconds(5)));
            Thread.sleep(100);
            first.release();
            assertTrue(waiter.get().isPresent());
        }
    }

    @Test
    @DisplayName("locks of a finished run report not held")
    void inactiveRunLocks() {
        ReplicatedStore store = newStore("node-a");
        AtomicBoolean active = new AtomicBoolean(true);
        LockService locks = new LockService("node-a", "inc", (node, inc) -> true, store, active::get);
        ClusterLock lock = locks.tryAcquire("job", Duration.ZERO).orElseThrow();
        active.set(false);
        assertFalse(lock.isHeld());
        assertTrue(locks.tryAcquire("other", Duration.ZERO).isEmpty(), "no acquisition once inactive");
    }

    private interface IndexedTask {
        void run(int index) throws Exception;
    }

    private static void runConcurrently(int threads, IndexedTask task) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            Future<?>[] futures = new Future<?>[threads];
            for (int i = 0; i < threads; i++) {
                int index = i;
                futures[i] = pool.submit(() -> {
                    start.await();
                    task.run(index);
                    return null;
                });
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }
}
