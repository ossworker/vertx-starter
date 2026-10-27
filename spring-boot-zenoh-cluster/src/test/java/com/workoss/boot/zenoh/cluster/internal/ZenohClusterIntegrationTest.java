package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.ClusterCounter;
import com.workoss.boot.zenoh.cluster.ClusterLock;
import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.MembershipListener;
import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.internal.store.HybridClock;
import com.workoss.boot.zenoh.cluster.internal.store.LockService;
import com.workoss.boot.zenoh.cluster.internal.store.ReplicatedStore;
import com.workoss.zenoh.Config;
import com.workoss.zenoh.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static com.workoss.boot.zenoh.cluster.internal.ClusterTestSupport.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Zenoh sessions over loopback TCP: node A is a peer listening on a free port, the others
 * connect to it as clients. Every test uses its own app name so leftover sessions cannot interfere.
 */
@DisplayName("Zenoh cluster across real sessions")
class ZenohClusterIntegrationTest {

    private static final long WAIT = 8_000L;

    private final List<DefaultZenohCluster> started = new ArrayList<>();
    private String app;
    private int port;
    private DefaultZenohCluster a;
    private DefaultZenohCluster b;

    @BeforeEach
    void setUp() throws Exception {
        app = "it-" + UUID.randomUUID().toString().substring(0, 8);
        port = ClusterTestSupport.freePort();
        a = start(new DefaultZenohCluster(ClusterTestSupport.peer(app, "node-a", port)));
        b = start(new DefaultZenohCluster(ClusterTestSupport.client(app, "node-b", port)));
        awaitMembers(a, "node-a", "node-b");
        awaitMembers(b, "node-a", "node-b");
    }

    @AfterEach
    void tearDown() {
        Collections.reverse(started);
        started.forEach(DefaultZenohCluster::stop);
    }

    @Test
    @DisplayName("nodes discover each other and report graceful leave")
    void membership() {
        List<String> events = new CopyOnWriteArrayList<>();
        a.addMembershipListener(recording(events));
        DefaultZenohCluster c = start(new DefaultZenohCluster(ClusterTestSupport.client(app, "node-c", port)));
        awaitMembers(a, "node-a", "node-b", "node-c");
        await(() -> events.contains("joined:node-c"), WAIT, "A notified of C joining");

        c.stop();
        await(() -> events.contains("left:node-c"), WAIT, "A notified of C leaving");
        awaitMembers(a, "node-a", "node-b");
    }

    @Test
    @DisplayName("map writes, deletes and ttl replicate in both directions")
    void mapReplication() {
        SharedMap<String, String> mapA = a.sharedMap("shared", ValueCodec.utf8());
        SharedMap<String, String> mapB = b.sharedMap("shared", ValueCodec.utf8());
        mapA.put("x", "1");
        await(() -> "1".equals(mapB.get("x")), WAIT, "B sees x=1");
        mapB.put("y", "2");
        await(() -> "2".equals(mapA.get("y")), WAIT, "A sees y=2");

        mapA.remove("x");
        await(() -> mapB.get("x") == null, WAIT, "B sees x removed");

        mapA.put("ttl", "v", Duration.ofMillis(800));
        await(() -> "v".equals(mapB.get("ttl")), WAIT, "B sees ttl entry");
        await(() -> mapB.get("ttl") == null, WAIT, "ttl entry expires on B");
    }

    @Test
    @DisplayName("escaped map names and keys survive the trip through Zenoh")
    void escapedKeysOverTheWire() {
        String mapName = "orders/eu*?";
        String key = "a/b c%2F?*$";
        a.sharedMap(mapName, ValueCodec.utf8()).put(key, "value");
        a.sharedMap("orders", ValueCodec.utf8()).put("k", "other");
        SharedMap<String, String> mapB = b.sharedMap(mapName, ValueCodec.utf8());
        await(() -> "value".equals(mapB.get(key)), WAIT, "B sees the escaped entry");
        await(() -> "other".equals(b.sharedMap("orders", ValueCodec.utf8()).get("k")), WAIT, "B sees sibling map");
        assertEquals(Set.of(key), mapB.keySet());
    }

    @Test
    @DisplayName("multi-segment node ids, lock and counter names work across nodes and stay isolated")
    void multiSegmentNames() {
        DefaultZenohCluster c = start(new DefaultZenohCluster(ClusterTestSupport.client(app, "edge/node-c", port)));
        awaitMembers(a, "node-a", "node-b", "edge/node-c");
        List<ClusterMessage> directC = new CopyOnWriteArrayList<>();
        c.messaging().onDirectMessage(directC::add);
        resendUntil(() -> a.messaging().send("edge/node-c", "hi".getBytes(StandardCharsets.UTF_8)),
                () -> !directC.isEmpty(), "A -> edge/node-c");

        ClusterLock nested = a.tryLock("jobs/nightly").orElseThrow();
        assertTrue(b.tryLock("jobs/nightly", Duration.ofMillis(300)).isEmpty(), "B sees A's nested lock");
        ClusterLock parent = b.tryLock("jobs").orElseThrow();
        await(() -> "node-b".equals(lockHolder(a, "jobs")), WAIT, "A sees B holding the parent lock");
        assertEquals("node-a", lockHolder(a, "jobs/nightly"));
        nested.release();
        parent.release();

        a.counter("hits/eu").addAndGet(5);
        b.counter("hits").incrementAndGet();
        await(() -> b.counter("hits/eu").get() == 5L, WAIT, "B sees the nested counter");
        await(() -> a.counter("hits").get() == 1L, WAIT, "A sees the parent counter");
    }

    @Test
    @DisplayName("a late joiner bootstraps live entries and tombstones")
    void lateJoinerBootstrap() {
        SharedMap<String, String> mapA = a.sharedMap("boot", ValueCodec.utf8());
        mapA.put("k1", "v1");
        mapA.put("k2", "v2");
        mapA.put("gone", "x");
        mapA.remove("gone");
        await(() -> "v2".equals(b.sharedMap("boot", ValueCodec.utf8()).get("k2")), WAIT, "B caught up");

        DefaultZenohCluster c = start(new DefaultZenohCluster(ClusterTestSupport.client(app, "node-c", port)));
        SharedMap<String, String> mapC = c.sharedMap("boot", ValueCodec.utf8());
        await(() -> mapC.size() == 2, WAIT, "C bootstrapped");
        assertEquals("v1", mapC.get("k1"));
        assertNull(mapC.get("gone"));
    }

    @Test
    @DisplayName("concurrent writes to one key converge on every node")
    void concurrentWritesConverge() throws Exception {
        SharedMap<String, String> mapA = a.sharedMap("race", ValueCodec.utf8());
        SharedMap<String, String> mapB = b.sharedMap("race", ValueCodec.utf8());
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<?> fa = pool.submit(() -> {
                for (int i = 0; i < 200; i++) {
                    mapA.put("k", "a" + i);
                }
            });
            Future<?> fb = pool.submit(() -> {
                for (int i = 0; i < 200; i++) {
                    mapB.put("k", "b" + i);
                }
            });
            fa.get();
            fb.get();
        }
        await(() -> mapA.get("k") != null && mapA.get("k").equals(mapB.get("k")), WAIT, "replicas agree");
        String winner = mapA.get("k");
        Thread.sleep(500);
        assertEquals(winner, mapA.get("k"));
        assertEquals(winner, mapB.get("k"));
        assertTrue(winner.endsWith("199"), "the last write of one of the nodes wins: " + winner);
    }

    @Test
    @DisplayName("concurrent counter increments on two nodes are all counted")
    void counterAcrossNodes() throws Exception {
        ClusterCounter counterA = a.counter("hits");
        ClusterCounter counterB = b.counter("hits");
        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                ClusterCounter counter = t % 2 == 0 ? counterA : counterB;
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < 250; i++) {
                        counter.incrementAndGet();
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
        await(() -> counterA.get() == 2_000L && counterB.get() == 2_000L, WAIT, "both nodes count 2000");
    }

    @Test
    @DisplayName("a lock is exclusive across nodes and handed over on release")
    void lockAcrossNodes() {
        ClusterLock lockA = a.tryLock("job").orElseThrow();
        await(() -> "node-a".equals(lockHolder(b, "job")), WAIT, "B sees A's lock");
        assertTrue(b.tryLock("job", Duration.ofMillis(300)).isEmpty());

        lockA.release();
        ClusterLock lockB = b.tryLock("job", Duration.ofSeconds(5)).orElseThrow();
        assertTrue(lockB.isHeld());
        await(() -> "node-b".equals(lockHolder(a, "job")), WAIT, "A sees B's lock");
        lockB.release();
    }

    @Test
    @DisplayName("locks of a node that left are reclaimed")
    void lockOfDepartedNodeReclaimed() {
        DefaultZenohCluster c = start(new DefaultZenohCluster(ClusterTestSupport.client(app, "node-c", port)));
        awaitMembers(a, "node-a", "node-b", "node-c");
        c.tryLock("job").orElseThrow();
        await(() -> "node-c".equals(lockHolder(a, "job")), WAIT, "A sees C's lock");
        assertTrue(a.tryLock("job").isEmpty());

        c.stop();
        assertTrue(a.tryLock("job", Duration.ofSeconds(5)).isPresent(), "A takes over after C left");
    }

    @Test
    @DisplayName("a crashed node is detected by heartbeat timeout and its locks are reclaimed")
    void crashedNodeDetectedAndLockReclaimed() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        a.addMembershipListener(recording(events));
        KeySpace keys = new KeySpace(app, "test");
        AtomicBoolean beating = new AtomicBoolean(true);
        try (Session ghost = openClient(port)) {
            byte[] info = ("1\nghost\nghost-inc\n" + System.currentTimeMillis() + "\nnowhere")
                    .getBytes(StandardCharsets.UTF_8);
            Thread heart = Thread.ofVirtual().start(() -> {
                while (beating.get()) {
                    ghost.put(keys.memberKey("ghost"), info);
                    sleep(ClusterTestSupport.HEARTBEAT_MILLIS);
                }
            });
            await(() -> a.member("ghost").isPresent(), WAIT, "A sees the ghost");

            // The ghost takes a lock through a real replica publishing on its own session.
            ReplicatedStore ghostStore = new ReplicatedStore(keys.storePrefix(), "ghost", new HybridClock(), 60_000L);
            ghostStore.setSink(ghost::put);
            new LockService("ghost", "ghost-inc", (n, i) -> true, ghostStore, () -> true)
                    .tryAcquire("job", Duration.ZERO).orElseThrow();
            await(() -> "ghost".equals(lockHolder(a, "job")), WAIT, "A sees the ghost's lock");
            assertTrue(a.tryLock("job").isEmpty(), "the live ghost's lock is respected");

            beating.set(false); // crash: heartbeats stop, no leave is sent
            heart.join();
            await(() -> events.contains("left:ghost"), WAIT, "A declares the ghost gone");
            assertTrue(a.member("ghost").isEmpty());
            assertTrue(a.tryLock("job", Duration.ofSeconds(5)).isPresent(), "A reclaims the ghost's lock");
            ghost.closeSession();
        }
    }

    @Test
    @DisplayName("broadcast reaches every node including the sender; send reaches only the target")
    void messaging() {
        List<ClusterMessage> onA = new CopyOnWriteArrayList<>();
        List<ClusterMessage> onB = new CopyOnWriteArrayList<>();
        List<ClusterMessage> directA = new CopyOnWriteArrayList<>();
        List<ClusterMessage> directB = new CopyOnWriteArrayList<>();
        a.messaging().subscribe("news/eu", onA::add);
        b.messaging().subscribe("news/eu", onB::add);
        a.messaging().onDirectMessage(directA::add);
        b.messaging().onDirectMessage(directB::add);

        resendUntil(() -> a.messaging().broadcast("news/eu", "hello".getBytes(StandardCharsets.UTF_8)),
                () -> !onA.isEmpty() && !onB.isEmpty(), "broadcast delivered to A and B");
        ClusterMessage received = onB.getFirst();
        assertEquals("node-a", received.sourceNodeId());
        assertEquals("news/eu", received.address());
        assertEquals("hello", received.payloadAsString());

        resendUntil(() -> a.messaging().send("node-b", "ping".getBytes(StandardCharsets.UTF_8)),
                () -> !directB.isEmpty(), "A -> B");
        assertEquals("node-b", directB.getFirst().targetNodeId());
        resendUntil(() -> b.messaging().send("node-a", "pong".getBytes(StandardCharsets.UTF_8)),
                () -> !directA.isEmpty(), "B -> A");
        assertTrue(directA.stream().allMatch(m -> m.payloadAsString().equals("pong")), "A never got its own ping");
        assertEquals("node-b", directA.getFirst().sourceNodeId());
    }

    @Test
    @DisplayName("a stopped node restarts with a new incarnation and keeps its handles")
    void restart() {
        List<ClusterMessage> onB = new CopyOnWriteArrayList<>();
        b.messaging().subscribe("events", onB::add);
        SharedMap<String, String> mapB = b.sharedMap("persist", ValueCodec.utf8());
        ClusterCounter counterB = b.counter("restarts");
        mapB.put("k", "before");
        counterB.incrementAndGet();
        await(() -> "before".equals(a.sharedMap("persist", ValueCodec.utf8()).get("k")), WAIT, "A has k");
        await(() -> a.counter("restarts").get() == 1L, WAIT, "A has the count");
        String firstIncarnation = a.member("node-b").orElseThrow().incarnation();
        List<String> eventsOnA = new CopyOnWriteArrayList<>();
        a.addMembershipListener(recording(eventsOnA));

        b.stop();
        assertFalse(b.isActive());
        assertTrue(b.members().isEmpty());
        await(() -> eventsOnA.contains("left:node-b"), WAIT, "A saw B leave");

        b.start();
        awaitMembers(a, "node-a", "node-b");
        assertFalse(firstIncarnation.equals(a.member("node-b").orElseThrow().incarnation()));
        await(() -> "before".equals(mapB.get("k")), WAIT, "B re-bootstrapped its data from A");
        await(() -> counterB.get() == 1L, WAIT, "B recovered its own counter slot");
        assertEquals(2L, counterB.incrementAndGet());

        resendUntil(() -> a.messaging().broadcast("events", new byte[]{1}), () -> !onB.isEmpty(),
                "subscription survived the restart");
        mapB.put("k", "after");
        await(() -> "after".equals(a.sharedMap("persist", ValueCodec.utf8()).get("k")), WAIT, "A sees B's new write");
    }

    // --- Helpers ---

    private DefaultZenohCluster start(DefaultZenohCluster cluster) {
        cluster.start();
        started.add(cluster);
        return cluster;
    }

    /**
     * Holder node of a lock as seen by {@code cluster}'s replica, without trying to acquire it.
     */
    private static String lockHolder(DefaultZenohCluster cluster, String name) {
        byte[] value = cluster.requireStore().get(KeySpace.lockNamespace(name), KeySpace.LOCK_ENTRY);
        return value == null ? null : new String(value, StandardCharsets.UTF_8).split("\n")[0];
    }

    private static void awaitMembers(DefaultZenohCluster cluster, String... ids) {
        Set<String> expected = Set.of(ids);
        await(() -> expected.equals(Set.copyOf(cluster.members().stream().map(NodeInfo::nodeId).toList())),
                WAIT, cluster.nodeId() + " sees members " + expected);
    }

    private static MembershipListener recording(List<String> events) {
        return new MembershipListener() {
            @Override
            public void nodeJoined(NodeInfo node) {
                events.add("joined:" + node.nodeId());
            }

            @Override
            public void nodeLeft(NodeInfo node) {
                events.add("left:" + node.nodeId());
            }
        };
    }

    /**
     * Subscriber declarations propagate asynchronously, so a message sent right after subscribing
     * may legitimately be missed; keep sending until it lands.
     */
    private static void resendUntil(Runnable send, BooleanSupplier delivered, String description) {
        long deadline = System.nanoTime() + Duration.ofMillis(WAIT).toNanos();
        while (System.nanoTime() < deadline) {
            send.run();
            long until = System.nanoTime() + Duration.ofMillis(200).toNanos();
            while (System.nanoTime() < until) {
                if (delivered.getAsBoolean()) {
                    return;
                }
                sleep(20);
            }
        }
        throw new AssertionError("Not delivered: " + description);
    }

    private static Session openClient(int port) {
        Config config = Config.defaultConfig();
        try {
            config.setMode("client");
            config.setConnectEndpoints(List.of(ClusterTestSupport.endpoint(port)));
            return new Session(config);
        } finally {
            config.close();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
