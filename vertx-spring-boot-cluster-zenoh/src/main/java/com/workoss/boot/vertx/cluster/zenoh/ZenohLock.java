package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import io.vertx.core.shareddata.Lock;

/**
 * Distributed Lock implementation backed by Zenoh via zenoh-jni.
 * Uses a "check-before-acquire" pattern:
 * 1. GET the lock key to check if another node holds it
 * 2. If unheld or held by this node, PUT nodeId to acquire
 * 3. On release, DELETE the key to allow other nodes to acquire
 * <p>
 * Note: This is a best-effort distributed lock. Under high contention,
 * race conditions between GET and PUT are possible. For strict mutual
 * exclusion, an external coordination service (e.g. etcd) is recommended.
 */
public class ZenohLock implements Lock {

    private static final Logger logger = LoggerFactory.getLogger(ZenohLock.class);

    private final String lockName;
    private final String nodeId;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;
    private volatile boolean held;

    public ZenohLock(String lockName, String nodeId, KeyPrefixStrategy keyPrefix, Session session) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.keyPrefix = keyPrefix;
        this.session = session;
        this.held = tryAcquire();
    }

    /**
     * Package-private factory method with timeout retry.
     * Called by ZenohClusterManager.getLockWithTimeout().
     */
    static ZenohLock tryAcquireWithTimeout(String name, String nodeId, KeyPrefixStrategy keyPrefix,
                                           Session session, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long retryInterval = Math.clamp(timeoutMs / 10, 10, 100);

        while (System.currentTimeMillis() < deadline) {
            ZenohLock lock = new ZenohLock(name, nodeId, keyPrefix, session);
            if (lock.held) {
                return lock;
            }
            // Wait before retry
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // Return a lock that reports not held (caller checks held status)
        return new ZenohLock(name, nodeId, keyPrefix, session);
    }

    @Override
    public void release() {
        if (!held) {
            return;
        }
        held = false;
        if (session == null) {
            return;
        }

        String key = keyPrefix.lockKey(lockName);
        try {
            session.delete(key);
            logger.debug("Released lock '{}' by node '{}'", lockName, nodeId);
        } catch (Exception e) {
            logger.warn("Failed to release lock '{}': {}", lockName, e.getMessage());
        }


    }

    /**
     * Check if this lock was successfully acquired.
     */
    public boolean isHeld() {
        return held;
    }

    private boolean tryAcquire() {
        if (session == null) {
            return true; // local-only mode, always succeeds
        }
        String key = keyPrefix.lockKey(lockName);
        try {
            // Check if another node already holds this lock
            List<Reply> replies = session.get(key);
            for (Reply reply : replies) {
                if (reply.success() && reply.samplePayload().isPresent()) {
                    String holder = new String(reply.samplePayload().get(), StandardCharsets.UTF_8);
                    if (!holder.equals(nodeId)) {
                        logger.debug("Lock '{}' already held by node '{}', cannot acquire for '{}'",
                                lockName, holder, nodeId);
                        return false;
                    }
                    // We already hold it (re-entrant)
                    logger.debug("Lock '{}' already held by this node '{}'", lockName, nodeId);
                    return true;
                }
            }
            // Lock is free, acquire it
            session.put(key, nodeId.getBytes(StandardCharsets.UTF_8));
            logger.debug("Acquired lock '{}' by node '{}'", lockName, nodeId);
            return true;
        } catch (Exception e) {
            logger.warn("Failed to acquire lock '{}': {}", lockName, e.getMessage());
            return false;
        }
    }
}
