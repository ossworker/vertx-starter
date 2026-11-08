package com.workoss.boot.vertx.contactcenter.uctp;

import com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator.Window;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator.EPOCH;
import static org.junit.jupiter.api.Assertions.*;

class SnowflakeGeneratorTest {

    @Test
    void sameMillisecondSequenceOverflowsWithoutWaitingForClock() {
        long now = EPOCH + 100;
        SnowflakeGenerator generator = new SnowflakeGenerator(new Window(37, "owner", now, now + 5), () -> now);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            Set<Long> ids = new HashSet<>();
            long previous = 0;
            for (int index = 0; index < 10_000; index++) {
                long id = generator.nextId();
                assertTrue(ids.add(id));
                assertTrue(id > previous);
                previous = id;
            }
            assertEquals(now + 2, timestamp(previous));
            assertEquals(10_000, ids.size());
        });
    }

    @Test
    void clockRollbackAndClockBeforeWindowUseMonotonicLogicalTime() {
        AtomicLong clock = new AtomicLong(EPOCH + 100);
        SnowflakeGenerator generator = new SnowflakeGenerator(new Window(10, "owner", EPOCH + 90, EPOCH + 200), clock::get);
        long first = generator.nextId();
        clock.set(EPOCH - 1000);
        long second = generator.nextId();
        assertTrue(second > first);
        assertEquals(timestamp(first), timestamp(second));
        clock.set(EPOCH + 150);
        assertEquals(EPOCH + 150, timestamp(generator.nextId()));
        SnowflakeGenerator future = new SnowflakeGenerator(new Window(11, "owner", EPOCH + 160, EPOCH + 200), clock::get);
        assertEquals(EPOCH + 160, timestamp(future.nextId()));
    }

    @Test
    void sequenceOverflowCannotEscapeAnInclusiveWindow() {
        long now = EPOCH + 100;
        SnowflakeGenerator generator = new SnowflakeGenerator(new Window(1, "owner", now, now), () -> now);
        for (int index = 0; index < 4096; index++) {
            assertEquals(now, timestamp(generator.nextId()));
        }
        assertThrows(IllegalStateException.class, generator::nextId);
        assertThrows(IllegalStateException.class, generator::nextId);
        generator.extend(new Window(1, "owner", now, now + 1));
        assertEquals(now + 1, timestamp(generator.nextId()));
    }

    @Test
    void wallClockOutsideWindowIsRejectedWithoutFallback() {
        AtomicLong clock = new AtomicLong(EPOCH + 101);
        Window window = new Window(1, "owner", EPOCH + 90, EPOCH + 100);
        SnowflakeGenerator generator = new SnowflakeGenerator(window, clock::get);
        assertThrows(IllegalStateException.class, generator::nextId);
        assertEquals(window, generator.window());
    }

    @Test
    void datacenterAndWorkerBitsAreEncodedForEveryNode() {
        for (int node = 0; node < 1024; node++) {
            Window window = new Window(node, "owner", EPOCH + 1, EPOCH + 1);
            long id = new SnowflakeGenerator(window, () -> EPOCH + 1).nextId();
            assertEquals(node >>> 5, window.datacenterId());
            assertEquals(node & 31, window.workerId());
            assertEquals(window.datacenterId(), (id >>> 17) & 31);
            assertEquals(window.workerId(), (id >>> 12) & 31);
            assertEquals(EPOCH + 1, timestamp(id));
            assertTrue(id > 0);
        }
    }

    @Test
    void epochZeroIsSkippedAndMaximumTimestampRemainsPositive() {
        SnowflakeGenerator first = new SnowflakeGenerator(new Window(0, "owner", EPOCH, EPOCH), () -> EPOCH);
        assertEquals(1, first.nextId());
        assertEquals(2, first.nextId());
        long lastMillisecond = EPOCH + (1L << 41) - 1;
        SnowflakeGenerator last = new SnowflakeGenerator(new Window(1023, "owner", lastMillisecond, lastMillisecond), () -> lastMillisecond);
        long id = 0;
        for (int index = 0; index < 4096; index++) {
            id = last.nextId();
        }
        assertEquals(Long.MAX_VALUE, id);
        assertThrows(IllegalStateException.class, last::nextId);
    }

    @Test
    void onlySameOwnerNodeAndOriginMayExtendWithoutShrinking() {
        Window original = new Window(1, "owner", EPOCH + 10, EPOCH + 20);
        SnowflakeGenerator generator = new SnowflakeGenerator(original, () -> EPOCH + 15);
        assertThrows(IllegalArgumentException.class, () -> generator.extend(new Window(2, "owner", original.from(), original.until())));
        assertThrows(IllegalArgumentException.class, () -> generator.extend(new Window(1, "other", original.from(), original.until())));
        assertThrows(IllegalArgumentException.class, () -> generator.extend(new Window(1, "owner", original.from() + 1, original.until())));
        assertThrows(IllegalArgumentException.class, () -> generator.extend(new Window(1, "owner", original.from(), original.until() - 1)));
        assertEquals(original, generator.window());
        generator.extend(original);
        Window extended = new Window(1, "owner", original.from(), EPOCH + 30);
        generator.extend(extended);
        assertEquals(extended, generator.window());
    }

    @Test
    void invalidNodesAndTimestampWindowsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Window(-1, "owner", EPOCH, EPOCH));
        assertThrows(IllegalArgumentException.class, () -> new Window(1024, "owner", EPOCH, EPOCH));
        assertThrows(IllegalArgumentException.class, () -> new Window(0, "owner", EPOCH - 1, EPOCH));
        assertThrows(IllegalArgumentException.class, () -> new Window(0, "owner", EPOCH + 1, EPOCH));
        assertThrows(IllegalArgumentException.class, () -> new Window(0, "owner", EPOCH, EPOCH + (1L << 41)));
    }

    @Test
    void differentNodesAndSharedGeneratorsAreUniqueAcrossThreads() throws Exception {
        long now = EPOCH + 100;
        List<SnowflakeGenerator> generators = new ArrayList<>();
        for (int node = 0; node < 8; node++) {
            generators.add(new SnowflakeGenerator(new Window(node, "owner-" + node, now, now + 10), () -> now));
        }
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int index = 0; index < 32; index++) {
            SnowflakeGenerator generator = generators.get(index % generators.size());
            tasks.add(() -> {
                for (int count = 0; count < 5000; count++) {
                    assertTrue(ids.add(generator.nextId()));
                }
                return null;
            });
        }
        try (var executor = Executors.newFixedThreadPool(16)) {
            for (var result : executor.invokeAll(tasks)) {
                result.get();
            }
        }
        assertEquals(160_000, ids.size());
    }

    @Test
    void revokedGeneratorRefusesUntilReassignedAndStaysMonotonicOnTheSameNode() {
        AtomicLong clock = new AtomicLong(EPOCH + 100);
        SnowflakeGenerator generator = new SnowflakeGenerator(new Window(5, "owner", EPOCH + 100, EPOCH + 200), clock::get);
        assertEquals(EPOCH - 1, generator.lastTimestamp());
        long before = generator.nextId();
        assertEquals(EPOCH + 100, generator.lastTimestamp());
        generator.revoke();
        assertTrue(generator.revoked());
        assertThrows(IllegalStateException.class, generator::nextId);
        assertThrows(IllegalArgumentException.class, () -> generator.reassign(new Window(6, "other", EPOCH + 100, EPOCH + 200)));
        assertTrue(generator.revoked());
        clock.set(EPOCH + 50);
        generator.reassign(new Window(5, "owner", EPOCH + 90, EPOCH + 300));
        assertFalse(generator.revoked());
        long after = generator.nextId();
        assertTrue(after > before, "A re-leased node must never reissue an earlier (timestamp, sequence)");
        generator.reassign(new Window(9, "owner", EPOCH + 150, EPOCH + 300));
        long moved = generator.nextId();
        assertEquals(9, (int) ((moved >>> 12) & 1023));
        assertEquals(EPOCH + 150, timestamp(moved));
    }

    private static long timestamp(long id) {
        return EPOCH + (id >>> 22);
    }
}
