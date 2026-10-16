package com.workoss.boot.zenoh.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two {@link ZenohClusterManager}s: membership discovery/events, and shared counter and lock
 * semantics across nodes.
 */
@DisplayName("Cluster manager: membership, counter, lock")
class ZenohClusterManagerTest {

    private static final String APP = "manager";

    private int port;
    private ZenohClusterManager managerA;
    private ZenohClusterManager managerB;

    private final List<String> joinedOnA = new CopyOnWriteArrayList<>();
    private final AtomicInteger joinEventsA = new AtomicInteger(0);

    @BeforeEach
    void setUp() throws Exception {
        port = ZenohTestSupport.freePort();
        managerA = new ZenohClusterManager(ZenohTestSupport.peerProperties(APP, "node-a", port));
        managerA.addMembershipListener(new MembershipListener() {
            @Override
            public void nodeJoined(String nodeId) {
                joinedOnA.add(nodeId);
                joinEventsA.incrementAndGet();
            }

            @Override
            public void nodeLeft(String nodeId) {
            }
        });
        managerA.start();

        managerB = new ZenohClusterManager(ZenohTestSupport.clientProperties(APP, "node-b", port));
        managerB.start();
    }

    @AfterEach
    void tearDown() {
        managerB.stop();
        managerA.stop();
    }

    @Test
    @DisplayName("both nodes discover each other")
    void mutualDiscovery() {
        ZenohTestSupport.await(() -> managerA.getNodes().contains("node-b"), 8000,
                "A discovers B");
        ZenohTestSupport.await(() -> managerB.getNodes().contains("node-a"), 8000,
                "B discovers A");
        assertTrue(managerA.getNodes().contains("node-a"));
        assertTrue(managerB.getNodes().contains("node-b"));
    }

    @Test
    @DisplayName("membership listener fires for the joining node")
    void membershipEvent() {
        ZenohTestSupport.await(() -> joinEventsA.get() >= 1, 8000, "A fired a nodeJoined event");
        assertTrue(joinedOnA.contains("node-b"), "A's listener recorded node-b join");
    }

    @Test
    @DisplayName("shared counter increments are visible on the peer")
    void counterCrossNode() {
        ZenohClusterCounter counterA = managerA.getCounter("hits");
        ZenohClusterCounter counterB = managerB.getCounter("hits");
        for (int i = 0; i < 5; i++) {
            counterA.incrementAndGet();
        }
        ZenohTestSupport.await(() -> counterB.get() == 5, 8000, "B sees counter == 5");
        // A's local view stays authoritative for its own increments.
        assertEquals(5, counterA.get());
    }

    @Test
    @DisplayName("lock is held by one node and denied to the other")
    void lockExclusion() {
        ZenohClusterLock lockA = managerA.tryLock("mutex", 0L);
        assertTrue(lockA.isHeld(), "A acquires the lock first");

        ZenohClusterLock lockB = managerB.tryLock("mutex", 500L);
        assertFalse(lockB.isHeld(), "B is denied while A holds the lock");

        lockA.release();
        ZenohTestSupport.await(() -> managerB.tryLock("mutex", 2000L).isHeld(), 4000,
                "B acquires the lock after A releases");
    }
}
