package com.workoss.boot.zenoh.cluster;

import com.workoss.zenoh.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Two real Zenoh sessions (peer + client) replicating a shared map: A puts, B gets; both directions;
 * delete and TTL are reflected across nodes.
 */
@DisplayName("Shared map replication across two Zenoh nodes")
class ZenohClusterReplicationTest {

    private static final String APP = "repl";
    private static final KeyPrefixStrategy KEY_PREFIX = new KeyPrefixStrategy(APP, "test");

    private int port;
    private Session sessionA;
    private Session sessionB;
    private ScheduledExecutorService schedulerA;
    private ScheduledExecutorService schedulerB;
    private ZenohMapStore storeA;
    private ZenohMapStore storeB;
    private ZenohSharedMap<String, String> mapA;
    private ZenohSharedMap<String, String> mapB;

    @BeforeEach
    void setUp() throws Exception {
        port = ZenohTestSupport.freePort();
        sessionA = ZenohTestSupport.openPeer(port);
        sessionB = ZenohTestSupport.openClient(port);

        schedulerA = Executors.newSingleThreadScheduledExecutor();
        schedulerB = Executors.newSingleThreadScheduledExecutor();
        storeA = new ZenohMapStore(KEY_PREFIX, "node-a", schedulerA, Executors.newFixedThreadPool(2));
        storeB = new ZenohMapStore(KEY_PREFIX, "node-b", schedulerB, Executors.newFixedThreadPool(2));
        storeA.start(sessionA);
        storeB.start(sessionB);

        mapA = new ZenohSharedMap<>("shared", KEY_PREFIX, storeA, ValueCodec.jdk());
        mapB = new ZenohSharedMap<>("shared", KEY_PREFIX, storeB, ValueCodec.jdk());
    }

    @AfterEach
    void tearDown() {
        storeB.stop();
        storeA.stop();
        sessionB.closeSession();
        sessionA.closeSession();
        schedulerB.shutdownNow();
        schedulerA.shutdownNow();
    }

    @Test
    @DisplayName("A puts, B gets (and the reverse)")
    void aPutBGet() {
        mapA.put("a", "1");
        ZenohTestSupport.await(() -> "1".equals(mapB.get("a")), 5000, "B sees A's put of a=1");
        assertEquals("1", mapB.get("a"));

        mapB.put("b", "2");
        ZenohTestSupport.await(() -> "2".equals(mapA.get("b")), 5000, "A sees B's put of b=2");
        assertEquals("2", mapA.get("b"));
    }

    @Test
    @DisplayName("delete on A is reflected on B")
    void deletePropagation() {
        mapA.put("a", "1");
        ZenohTestSupport.await(() -> "1".equals(mapB.get("a")), 5000, "B sees a=1");
        mapA.remove("a");
        ZenohTestSupport.await(() -> mapB.get("a") == null, 5000, "B sees a removed");
        assertNull(mapB.get("a"));
    }

    @Test
    @DisplayName("TTL entry expires on both nodes")
    void ttlPropagation() throws InterruptedException {
        mapA.put("t", "v", 1000L);
        ZenohTestSupport.await(() -> "v".equals(mapB.get("t")), 5000, "B sees t=v");
        Thread.sleep(1500L);
        assertNull(mapB.get("t"));
        assertNull(mapA.get("t"));
    }

    @Test
    @DisplayName("a node joining after a write bootstraps the existing entry")
    void lateJoinBootstrap() {
        mapA.put("late", "value");
        ZenohTestSupport.await(() -> "value".equals(mapA.get("late")), 2000, "A has late=value");

        // A third node joins after the write and must catch up via the queryable bootstrap.
        int port2 = port;
        Session sessionC = ZenohTestSupport.openClient(port2);
        ScheduledExecutorService schedulerC = Executors.newSingleThreadScheduledExecutor();
        ZenohMapStore storeC = new ZenohMapStore(KEY_PREFIX, "node-c", schedulerC,
                Executors.newFixedThreadPool(2));
        storeC.start(sessionC);
        ZenohSharedMap<String, String> mapC = new ZenohSharedMap<>("shared", KEY_PREFIX, storeC,
                ValueCodec.jdk());

        try {
            ZenohTestSupport.await(() -> "value".equals(mapC.get("late")), 5000,
                    "C bootstraps late=value from peers");
            assertEquals("value", mapC.get("late"));
        } finally {
            storeC.stop();
            sessionC.closeSession();
            schedulerC.shutdownNow();
        }
    }
}
