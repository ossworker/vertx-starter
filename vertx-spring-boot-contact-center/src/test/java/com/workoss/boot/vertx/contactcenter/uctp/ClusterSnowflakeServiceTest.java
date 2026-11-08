package com.workoss.boot.vertx.contactcenter.uctp;

import com.workoss.boot.vertx.contactcenter.uctp.ClusterSnowflakeService.Timing;
import com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator.Window;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static com.workoss.boot.vertx.contactcenter.journal.SqlPoolTestSupport.await;
import static com.workoss.boot.vertx.contactcenter.journal.SqlPoolTestSupport.failure;
import static com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator.EPOCH;
import static org.junit.jupiter.api.Assertions.*;

/** Runs against the real local Vert.x AsyncMap; the clustered path is covered by ZenohSnowflakeIntegrationTest. */
@Timeout(30)
class ClusterSnowflakeServiceTest {
    private static final Timing FAST = new Timing(2_000, 500, 50, 100, 200, 2_000, 10, 30);
    private static final int MALFORMED = 100;
    private static final int WITHIN_GRACE = 200;
    private static final int EXPIRED = 300;
    private final List<ClusterSnowflakeService> services = new ArrayList<>();
    private Vertx vertx;
    private AsyncMap<String, String> map;

    @BeforeEach
    void createVertx() throws Exception {
        vertx = Vertx.vertx();
        map = await(vertx.sharedData().getAsyncMap(ClusterSnowflakeService.MAP_NAME));
    }

    @AfterEach
    void closeVertx() throws Exception {
        services.forEach(ClusterSnowflakeService::close);
        await(vertx.close());
    }

    @Test
    void servicesLeaseDistinctNodesRecordTheirLeaseAndIssueUniqueIds() throws Exception {
        ClusterSnowflakeService first = service(1, FAST, System::currentTimeMillis);
        ClusterSnowflakeService second = service(1, FAST, System::currentTimeMillis);
        Future<SnowflakeGenerator> starting = first.start();
        assertSame(starting, first.start());
        SnowflakeGenerator left = await(starting);
        SnowflakeGenerator right = await(second.start());
        assertNotEquals(left.window().nodeId(), right.window().nodeId());
        assertEquals(first.owner(), left.window().owner());
        assertEquals(second.owner(), right.window().owner());
        assertNotEquals(first.owner(), second.owner());
        assertEquals(36, first.owner().length());
        assertEquals(FAST.leaseMillis(), left.window().until() - left.window().from());
        assertEquals(ClusterSnowflakeService.encode(left.window()), await(map.get(key(left.window()))));
        assertEquals(ClusterSnowflakeService.encode(right.window()), await(map.get(key(right.window()))));
        HashSet<Long> ids = new HashSet<>();
        for (int index = 0; index < 10_000; index++) {
            assertTrue(ids.add(left.nextId()));
            assertTrue(ids.add(right.nextId()));
        }
        assertEquals(left.window().nodeId(), node(left.nextId()));
    }

    @Test
    void onlyAnExpiredSlotIsTakenWhileMalformedAndGraceRecordsStayUntouched() throws Exception {
        long now = EPOCH + 10_000_000;
        for (int node = 0; node < ClusterSnowflakeService.NODES; node++) {
            String value = switch (node) {
                case MALFORMED -> "garbage";
                case WITHIN_GRACE -> "other," + (now - 1_000) + "," + (now - 100);
                case EXPIRED -> "other," + (now - 5_000) + "," + (now - 1_000);
                default -> "other," + now + "," + (now + 600_000);
            };
            await(map.put(String.valueOf(node), value));
        }
        SnowflakeGenerator generator = await(service(1, FAST, () -> now).start());
        Window window = generator.window();
        assertEquals(EXPIRED, window.nodeId());
        assertTrue(window.from() > now - 1_000, "The new range must start after the expired owner's range");
        assertEquals("garbage", await(map.get(String.valueOf(MALFORMED))));
        assertEquals("other," + (now - 1_000) + "," + (now - 100), await(map.get(String.valueOf(WITHIN_GRACE))));
        assertEquals(ClusterSnowflakeService.encode(window), await(map.get(String.valueOf(EXPIRED))));
    }

    @Test
    void foreignOverwriteRevokesTheGeneratorAndLeasesAnotherNode() throws Exception {
        SnowflakeGenerator generator = await(service(1, FAST, System::currentTimeMillis).start());
        Window original = generator.window();
        long now = System.currentTimeMillis();
        String intruder = "intruder," + now + "," + (now + 600_000);
        await(map.put(key(original), intruder));
        awaitUntil(() -> !generator.revoked() && generator.window().nodeId() != original.nodeId(),
                "An overwritten lease must be revoked and replaced by a new node");
        assertEquals(intruder, await(map.get(key(original))), "The foreign record must be left intact");
        assertEquals(generator.window().nodeId(), node(generator.nextId()));
        assertEquals(ClusterSnowflakeService.encode(generator.window()), await(map.get(key(generator.window()))));
    }

    @Test
    void renewalExtendsTheLeaseInPlace() throws Exception {
        SnowflakeGenerator generator = await(service(1, FAST, System::currentTimeMillis).start());
        Window original = generator.window();
        awaitUntil(() -> generator.window().until() > original.until(), "The renewal timer must extend the lease");
        assertEquals(original.nodeId(), generator.window().nodeId());
        assertEquals(original.from(), generator.window().from());
        awaitUntil(() -> ClusterSnowflakeService.encode(generator.window()).equals(await(map.get(key(original)))),
                "The renewed lease must be the recorded one");
    }

    @Test
    void lapsedLeaseIsNeverResurrected() throws Exception {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        SnowflakeGenerator generator = await(service(1, FAST, clock::get).start());
        Window original = generator.window();
        clock.set(original.until() + 1);
        awaitUntil(() -> !generator.revoked() && generator.window().nodeId() != original.nodeId(),
                "A lapsed lease must be replaced, not renewed");
        assertTrue(generator.window().from() > original.until());
        assertEquals(ClusterSnowflakeService.encode(original), await(map.get(key(original))),
                "The lapsed record must not be extended");
    }

    @Test
    void missingClusterMembersBlockLeasingUntilTimeout() {
        Throwable failure = failure(service(2, FAST, System::currentTimeMillis).start());
        assertInstanceOf(IllegalStateException.class, failure);
        assertTrue(failure.getMessage().contains("cluster members 1 < required 2"), failure.getMessage());
    }

    @Test
    void exhaustedNodeSpaceTimesOutWithTheObstacle() throws Exception {
        long now = System.currentTimeMillis();
        for (int node = 0; node < ClusterSnowflakeService.NODES; node++) {
            await(map.put(String.valueOf(node), "other," + now + "," + (now + 600_000)));
        }
        ClusterSnowflakeService service = service(1, FAST, System::currentTimeMillis);
        Throwable failure = failure(service.start());
        assertTrue(failure.getMessage().contains("free=0 of 1024 nodes"), failure.getMessage());
        assertInstanceOf(IllegalStateException.class, failure(service.start()), "A failed service stays closed");
    }

    @Test
    void closeShrinksTheRecordAndStopsIssuing() throws Exception {
        ClusterSnowflakeService service = service(1, FAST, System::currentTimeMillis);
        SnowflakeGenerator generator = await(service.start());
        Window original = generator.window();
        generator.nextId();
        long issued = generator.lastTimestamp();
        service.close();
        assertThrows(IllegalStateException.class, generator::nextId);
        assertInstanceOf(IllegalStateException.class, failure(service.start()));
        awaitUntil(() -> {
            Window released = ClusterSnowflakeService.decode(original.nodeId(), await(map.get(key(original))));
            return released != null && released.until() < original.until() && released.until() >= issued
                    && released.from() == original.from() && released.owner().equals(original.owner());
        }, "Closing must shrink the lease to the last issued timestamp");
    }

    @Test
    void closeDuringSettleFailsStartAndReleasesTheClaim() throws Exception {
        Timing slowSettle = new Timing(5_000, 1_000, 50, 1_000, 200, 5_000, 10, 30);
        ClusterSnowflakeService service = service(1, slowSettle, System::currentTimeMillis);
        Future<SnowflakeGenerator> starting = service.start();
        awaitUntil(() -> await(map.entries()).values().stream().anyMatch(v -> v.startsWith(service.owner() + ",")),
                "The claim must be written before settling");
        service.close();
        assertInstanceOf(IllegalStateException.class, failure(starting));
        awaitUntil(() -> await(map.entries()).values().stream()
                        .map(v -> v.split(","))
                        .filter(parts -> parts[0].equals(service.owner()))
                        .allMatch(parts -> Long.parseLong(parts[2]) - Long.parseLong(parts[1]) < slowSettle.leaseMillis()),
                "A claim confirmed after close must be released");
    }

    @Test
    void timingsThatCannotRenewBeforeExpiryAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Timing(1_000, 900, 50, 100, 0, 1_000, 10, 30));
        assertThrows(IllegalArgumentException.class, () -> new Timing(1_000, 100, 50, 100, 0, 1_000, 30, 10));
        assertThrows(IllegalArgumentException.class, () -> new ClusterSnowflakeService(vertx, 0));
    }

    private ClusterSnowflakeService service(int minMembers, Timing timing, LongSupplier clock) {
        ClusterSnowflakeService service = new ClusterSnowflakeService(vertx, minMembers, timing, clock);
        services.add(service);
        return service;
    }

    private static String key(Window window) {
        return String.valueOf(window.nodeId());
    }

    private static int node(long id) {
        return (int) ((id >>> 12) & 1023);
    }

    private static void awaitUntil(Callable<Boolean> condition, String failureMessage) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.call() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(condition.call(), failureMessage);
    }
}
