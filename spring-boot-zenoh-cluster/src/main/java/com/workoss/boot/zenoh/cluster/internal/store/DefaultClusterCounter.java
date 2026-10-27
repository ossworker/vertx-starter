package com.workoss.boot.zenoh.cluster.internal.store;

import com.workoss.boot.zenoh.cluster.ClusterCounter;
import com.workoss.boot.zenoh.cluster.internal.KeyEscaper;
import com.workoss.boot.zenoh.cluster.internal.KeySpace;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Grow/shrink counter CRDT: one slot per node id, value = sum of slots. Only the owning node
 * writes a slot, so slots never conflict and no increment is lost across nodes.
 */
public final class DefaultClusterCounter implements ClusterCounter {

    private final String name;
    private final String namespace;
    private final String ownSlot;
    private final Supplier<ReplicatedStore> store;

    public DefaultClusterCounter(String name, String nodeId, Supplier<ReplicatedStore> store) {
        this.name = name;
        this.namespace = KeySpace.counterNamespace(name);
        this.ownSlot = KeyEscaper.escape(nodeId);
        this.store = store;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long get() {
        long total = 0L;
        for (byte[] slot : store.get().entries(namespace).values()) {
            total += toLong(slot);
        }
        return total;
    }

    @Override
    public long incrementAndGet() {
        return addAndGet(1L);
    }

    @Override
    public long getAndIncrement() {
        return addAndGet(1L) - 1L;
    }

    @Override
    public long decrementAndGet() {
        return addAndGet(-1L);
    }

    @Override
    public long getAndDecrement() {
        return addAndGet(-1L) + 1L;
    }

    @Override
    public long getAndAdd(long delta) {
        return addAndGet(delta) - delta;
    }

    @Override
    public long addAndGet(long delta) {
        ReplicatedStore current = store.get();
        long others = othersTotal(current);
        long[] mine = new long[1];
        current.update(namespace, ownSlot, slot -> {
            mine[0] = toLong(slot) + delta;
            return Change.put(fromLong(mine[0]));
        });
        // Own slot taken from inside the atomic block, so concurrent local callers see distinct
        // results.
        return others + mine[0];
    }

    @Override
    public boolean compareAndSet(long expected, long update) {
        ReplicatedStore current = store.get();
        long others = othersTotal(current);
        return current.update(namespace, ownSlot, slot -> others + toLong(slot) == expected
                ? Change.put(fromLong(update - others)) : Change.KEEP).changed();
    }

    private long othersTotal(ReplicatedStore current) {
        long total = 0L;
        for (Map.Entry<String, byte[]> slot : current.entries(namespace).entrySet()) {
            if (!slot.getKey().equals(ownSlot)) {
                total += toLong(slot.getValue());
            }
        }
        return total;
    }

    private static long toLong(byte[] value) {
        return value == null || value.length != Long.BYTES ? 0L : ByteBuffer.wrap(value).getLong();
    }

    private static byte[] fromLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }
}
