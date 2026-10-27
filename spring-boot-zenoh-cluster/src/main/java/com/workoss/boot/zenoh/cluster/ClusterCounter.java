package com.workoss.boot.zenoh.cluster;

/**
 * Cluster-wide counter.
 * <p>
 * Each node only ever writes its own slot and the value is the sum of all slots, so increments
 * from different nodes never overwrite each other. Values returned by the {@code ...AndGet}
 * methods are unique among threads of this node, but are not linearizable across nodes.
 * {@link #compareAndSet} compares against this node's view of the total and is therefore only
 * atomic locally.
 */
public interface ClusterCounter {

    String name();

    long get();

    long incrementAndGet();

    long getAndIncrement();

    long decrementAndGet();

    long getAndDecrement();

    long addAndGet(long delta);

    long getAndAdd(long delta);

    boolean compareAndSet(long expected, long update);
}
