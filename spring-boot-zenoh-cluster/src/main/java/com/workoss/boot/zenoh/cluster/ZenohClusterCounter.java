package com.workoss.boot.zenoh.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Cluster-wide counter held in the replicated {@link ZenohMapStore}.
 * <p>
 * Read-modify-write runs inside a single {@code ConcurrentHashMap} compute block, so increments are
 * atomic against every other thread on this node and no retry loop is needed. Replication to peers
 * is asynchronous, which means two nodes incrementing at the same instant can each apply their own
 * delta locally and converge on the write that arrives last — losing one of the two increments.
 * Counters that must not lose increments need a single arbiter (an external KV with atomic
 * operations, or one elected writer per counter).
 */
public class ZenohClusterCounter {

    private static final Logger logger = LoggerFactory.getLogger(ZenohClusterCounter.class);

    private final String counterName;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMapStore store;

    public ZenohClusterCounter(String counterName, KeyPrefixStrategy keyPrefix, ZenohMapStore store) {
        this.counterName = counterName;
        this.keyPrefix = keyPrefix;
        this.store = store;
    }

    public String getCounterName() {
        return counterName;
    }

    public long get() {
        return toLong(store.getRaw(key()));
    }

    public long incrementAndGet() {
        return add(1L);
    }

    public long getAndIncrement() {
        return add(1L) - 1L;
    }

    public long decrementAndGet() {
        return add(-1L);
    }

    public long getAndDecrement() {
        return add(-1L) + 1L;
    }

    public long addAndGet(long amount) {
        return add(amount);
    }

    public long getAndAdd(long amount) {
        return add(amount) - amount;
    }

    public boolean compareAndSet(long expected, long update) {
        boolean[] swapped = new boolean[1];
        store.computeRaw(key(), current -> {
            if (toLong(current) != expected) {
                return current;
            }
            swapped[0] = true;
            return fromLong(update);
        });
        return swapped[0];
    }

    private long add(long delta) {
        long[] result = new long[1];
        store.computeRaw(key(), current -> {
            result[0] = toLong(current) + delta;
            return fromLong(result[0]);
        });
        return result[0];
    }

    private String key() {
        return keyPrefix.counterKey(counterName);
    }

    private static long toLong(byte[] value) {
        if (value == null || value.length != Long.BYTES) {
            if (value != null) {
                logger.warn("Ignoring malformed counter value of {} byte(s)", value.length);
            }
            return 0L;
        }
        return ByteBuffer.wrap(value).getLong();
    }

    private static byte[] fromLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }
}
