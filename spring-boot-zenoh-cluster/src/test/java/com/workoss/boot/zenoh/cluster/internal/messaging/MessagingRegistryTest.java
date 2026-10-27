package com.workoss.boot.zenoh.cluster.internal.messaging;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.Subscription;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Local message dispatch")
class MessagingRegistryTest {

    private static ClusterMessage message(String address, int sequence) {
        return new ClusterMessage(address, "src", null, ByteBuffer.allocate(4).putInt(sequence).array(), 0L);
    }

    @Test
    @DisplayName("each handler sees its messages in arrival order")
    void orderedPerHandler() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        int total = 2_000;
        List<Integer> seen = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(total);
        registry.subscribe("seq", m -> {
            seen.add(ByteBuffer.wrap(m.payload()).getInt());
            done.countDown();
        });
        for (int i = 0; i < total; i++) {
            registry.dispatchBroadcast(message("seq", i));
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        for (int i = 0; i < total; i++) {
            assertEquals(i, seen.get(i));
        }
    }

    @Test
    @DisplayName("a blocked handler does not delay other handlers")
    void handlersAreIsolated() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch fastGotIt = new CountDownLatch(1);
        registry.subscribe("a", m -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        registry.subscribe("a", m -> fastGotIt.countDown());
        registry.dispatchBroadcast(message("a", 1));
        assertTrue(fastGotIt.await(5, TimeUnit.SECONDS));
        release.countDown();
    }

    @Test
    @DisplayName("cancel stops delivery and drops the address once unused")
    void cancel() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<Integer> seen = Collections.synchronizedList(new ArrayList<>());
        Subscription subscription = registry.subscribe("a", m -> seen.add(1));
        assertEquals(Set.of("a"), registry.addresses());
        subscription.cancel();
        subscription.cancel();
        assertEquals(Set.of(), registry.addresses());
        registry.dispatchBroadcast(message("a", 1));
        Thread.sleep(100);
        assertTrue(seen.isEmpty());
    }

    @Test
    @DisplayName("the wire envelope round-trips")
    void envelope() {
        byte[] wire = MessagingChannel.encode("node-1", new byte[]{1, 2, 3});
        ClusterMessage decoded = MessagingChannel.decode("addr", null, wire);
        assertEquals("node-1", decoded.sourceNodeId());
        assertEquals("addr", decoded.address());
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.payload());
        assertTrue(decoded.timestamp() > 0);
        assertNull(MessagingChannel.decode("addr", null, new byte[]{7}));
    }

    @Test
    @DisplayName("a wildcard subscription receives every matching address")
    void wildcardDispatch() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> star = Collections.synchronizedList(new ArrayList<>());
        List<String> doubleStar = Collections.synchronizedList(new ArrayList<>());
        List<String> exact = Collections.synchronizedList(new ArrayList<>());
        // news/eu -> star + doubleStar + exact (3); news/eu/west -> doubleStar (1); orders/eu -> none.
        CountDownLatch all = new CountDownLatch(4);
        registry.subscribe("news/*", m -> {
            star.add(m.address());
            all.countDown();
        });
        registry.subscribe("news/**", m -> {
            doubleStar.add(m.address());
            all.countDown();
        });
        registry.subscribe("news/eu", m -> {
            exact.add(m.address());
            all.countDown();
        });

        registry.dispatchBroadcast(message("news/eu", 1));
        registry.dispatchBroadcast(message("news/eu/west", 2));
        registry.dispatchBroadcast(message("orders/eu", 3));
        assertTrue(all.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("news/eu"), star);
        assertEquals(List.of("news/eu", "news/eu/west"), doubleStar);
        assertEquals(List.of("news/eu"), exact);
    }

    @Test
    @DisplayName("addresses() reports literal and wildcard subscriptions; cancel drops a pattern")
    void wildcardAddressesAndCancel() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        registry.subscribe("news/eu", m -> seen.add("exact"));
        Subscription pattern = registry.subscribe("news/*", m -> seen.add("pattern"));
        assertEquals(Set.of("news/eu", "news/*"), registry.addresses());

        pattern.cancel();
        assertEquals(Set.of("news/eu"), registry.addresses());
        registry.dispatchBroadcast(message("news/eu", 1));
        Thread.sleep(100);
        assertEquals(List.of("exact"), seen, "cancelled pattern no longer receives");
    }

    @Test
    @DisplayName("ordered handlers of one address run one at a time, in order")
    void orderedHandlersRunSequentially() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch firstRunning = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        registry.subscribe("a", m -> {
            firstRunning.countDown();
            awaitUninterruptibly(releaseFirst);
            seen.add("first");
        }, 1);
        registry.subscribe("a", m -> seen.add("second"), 2);

        registry.dispatchBroadcast(message("a", 1));
        assertTrue(firstRunning.await(5, TimeUnit.SECONDS));
        // The second handler is queued behind the first one, it must not overtake it.
        Thread.sleep(100);
        assertEquals(List.of(), seen);

        releaseFirst.countDown();
        awaitValue(List.of("first", "second"), seen);
    }

    @Test
    @DisplayName("a lower order wins even when it is registered later")
    void lowerOrderWinsRegardlessOfRegistrationOrder() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        registry.subscribe("a", m -> seen.add("registered-first-but-later"), 2);
        registry.subscribe("a", m -> seen.add("registered-second-but-earlier"), 1);

        registry.dispatchBroadcast(message("a", 1));
        awaitValue(List.of("registered-second-but-earlier", "registered-first-but-later"), seen);
    }

    @Test
    @DisplayName("equal orders keep registration order")
    void equalOrdersKeepRegistrationOrder() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        registry.subscribe("a", m -> seen.add("x"), 5);
        registry.subscribe("a", m -> seen.add("y"), 5);

        registry.dispatchBroadcast(message("a", 1));
        awaitValue(List.of("x", "y"), seen);
    }

    @Test
    @DisplayName("direct handlers are ordered as a group")
    void orderedDirectHandlers() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        registry.onDirectMessage(m -> seen.add("second"), 2);
        registry.onDirectMessage(m -> seen.add("first"), 1);

        registry.dispatchDirect(new ClusterMessage(null, "src", "self", new byte[]{1}, 0L));
        awaitValue(List.of("first", "second"), seen);
    }

    @Test
    @DisplayName("a pattern and a literal subscription each keep their own ordered sequence")
    void orderedPatternAndLiteral() throws InterruptedException {
        MessagingRegistry registry = new MessagingRegistry();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        registry.subscribe("news/*", m -> seen.add("pattern"), 1);
        registry.subscribe("news/eu", m -> seen.add("literal"), 1);

        registry.dispatchBroadcast(message("news/eu", 1));
        // Different addresses, so different executors: both run, the relative order is undefined.
        awaitValue(2, () -> seen.size());
        assertEquals(List.of("literal", "pattern"), seen.stream().sorted().toList());
    }

    private static void awaitValue(Object expected, List<String> seen) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (seen.equals(expected)) {
                return;
            }
            Thread.sleep(10);
        }
        assertEquals(expected, seen);
    }

    private static void awaitValue(int expectedSize, java.util.function.IntSupplier size)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (size.getAsInt() == expectedSize) {
                return;
            }
            Thread.sleep(10);
        }
        assertEquals(expectedSize, size.getAsInt());
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch was not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
