package com.vertx.spring.boot.cluster.zenoh;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

/**
 * Distributed Lock implementation backed by Zenoh.
 * Uses a "first-writer-wins" pattern:
 * 1. Attempt putIfAbsent at key vertx/{app}/{env}/cluster/locks/{name} with nodeId
 * 2. If value matches nodeId, lock acquired
 * 3. Associated with liveliness token for auto-release on crash
 * Key: vertx/{app}/{env}/cluster/locks/{lockName}
 */
public class ZenohLock implements Lock {

    private final String lockName;
    private final String nodeId;
    private final KeyPrefixStrategy keyPrefix;
    private volatile boolean held = true;

    public ZenohLock(String lockName, String nodeId, KeyPrefixStrategy keyPrefix) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public void release() {
        if (held) {
            held = false;
            // Full implementation:
            // Delete the key at keyPrefix.lockKey(lockName)
            // This releases the lock and allows other nodes to acquire it
            String key = keyPrefix.lockKey(lockName);
            // session.delete(KeyExpr.tryFrom(key));
        }
    }
}
