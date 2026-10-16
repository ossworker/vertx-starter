package com.workoss.boot.zenoh.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Distributed lock held in the replicated {@link ZenohMapStore}.
 * <p>
 * Acquisition is a {@code putIfAbsent} on the lock key: it is atomic against every other thread on
 * this node, then replicated to peers. Because replication is asynchronous, two nodes contending at
 * the same instant can both observe an absent key and both believe they hold the lock, converging
 * on whichever claim arrives last. That is the same window the storage-plugin version had between
 * its {@code get} and {@code put}; it is best-effort, not strict mutual exclusion. Use an external
 * coordination service (etcd, ZooKeeper) when strict exclusion is required.
 */
public class ZenohClusterLock {

    private static final Logger logger = LoggerFactory.getLogger(ZenohClusterLock.class);

    private final String lockName;
    private final String nodeId;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMapStore store;
    private volatile boolean held;

    public ZenohClusterLock(String lockName, String nodeId, KeyPrefixStrategy keyPrefix, ZenohMapStore store) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.keyPrefix = keyPrefix;
        this.store = store;
        this.held = tryAcquire();
    }

    /**
     * Try to acquire, retrying until {@code timeoutMs} elapses.
     *
     * @return a lock whose {@link #isHeld()} reports whether the acquisition succeeded
     */
    public static ZenohClusterLock tryAcquire(String lockName, String nodeId, KeyPrefixStrategy keyPrefix,
                                              ZenohMapStore store, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long retryInterval = Math.clamp(timeoutMs / 10, 10, 100);

        while (System.currentTimeMillis() < deadline) {
            ZenohClusterLock lock = new ZenohClusterLock(lockName, nodeId, keyPrefix, store);
            if (lock.held) {
                return lock;
            }
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // Return a lock that reports not held (caller checks held status)
        return new ZenohClusterLock(lockName, nodeId, keyPrefix, store);
    }

    public String getLockName() {
        return lockName;
    }

    /**
     * Check if this lock was successfully acquired.
     */
    public boolean isHeld() {
        return held;
    }

    public void release() {
        if (!held) {
            return;
        }
        held = false;
        // Conditional so that a lock already taken over by another node is not released by us.
        boolean released = store.removeRawIf(keyPrefix.lockKey(lockName), this::heldByThisNode);
        logger.debug("Lock '{}' released by node '{}': {}", lockName, nodeId, released);
    }

    private boolean tryAcquire() {
        String key = keyPrefix.lockKey(lockName);
        byte[] holder = store.getRaw(key);
        if (holder != null) {
            boolean ours = heldByThisNode(holder);
            if (!ours) {
                logger.debug("Lock '{}' already held by another node, cannot acquire for '{}'",
                        lockName, nodeId);
            }
            // Re-entrant when this node already holds it.
            return ours;
        }
        return store.putRawIfAbsent(key, nodeId.getBytes(StandardCharsets.UTF_8), 0L) == null;
    }

    private boolean heldByThisNode(byte[] holder) {
        return nodeId.equals(new String(holder, StandardCharsets.UTF_8));
    }
}
