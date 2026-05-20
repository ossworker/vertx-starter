package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.shareddata.Lock;
import io.zenoh.Session;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed Lock implementation backed by Zenoh.
 * Uses a "first-writer-wins" pattern:
 * 1. Put nodeId at key vertx/{app}/{env}/cluster/locks/{name}
 * 2. Lock is held as long as this node is alive (tied to liveliness)
 * 3. On release, delete the key to allow other nodes to acquire
 *
 * Key: vertx/{app}/{env}/cluster/locks/{lockName}
 */
public class ZenohLock implements Lock {

    private static final Logger logger = LoggerFactory.getLogger(ZenohLock.class);

    private final String lockName;
    private final String nodeId;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;
    private volatile boolean held = true;

    public ZenohLock(String lockName, String nodeId, KeyPrefixStrategy keyPrefix, Session session) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.keyPrefix = keyPrefix;
        this.session = session;
        acquireLock();
    }

    @Override
    public void release() {
        if (held) {
            held = false;
            if (session != null) {
                String key = keyPrefix.lockKey(lockName);
                try {
                    KeyExpr keyExpr = KeyExpr.tryFrom(key);
                    session.delete(keyExpr);
                    keyExpr.close();
                    logger.debug("Released lock '{}' by node '{}'", lockName, nodeId);
                } catch (ZError e) {
                    logger.warn("Failed to release lock '{}': {}", lockName, e.getMessage());
                }
            }
        }
    }

    private void acquireLock() {
        if (session != null) {
            String key = keyPrefix.lockKey(lockName);
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                session.put(keyExpr, nodeId);
                keyExpr.close();
                logger.debug("Acquired lock '{}' by node '{}'", lockName, nodeId);
            } catch (ZError e) {
                logger.warn("Failed to acquire lock '{}': {}", lockName, e.getMessage());
            }
        }
    }
}
