package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Session;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Counter implementation backed by Zenoh key-value store via zenoh-jni.
 * Uses {@code vertx.executeBlocking()} for all blocking Zenoh calls.
 * No custom thread pool needed — Vert.x manages the worker pool.
 * <p>
 * Each mutating operation performs read-modify-write with retry:
 * 1. Read current value from Zenoh (session.get)
 * 2. Compute new value locally
 * 3. Write new value to Zenoh (session.put)
 */
public class ZenohCounter implements Counter {

    private static final Logger logger = LoggerFactory.getLogger(ZenohCounter.class);
    private static final int MAX_RETRIES = 3;

    private final String counterName;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;
    private final Vertx vertx;

    public ZenohCounter(String counterName, KeyPrefixStrategy keyPrefix, Session session, Vertx vertx) {
        this.counterName = counterName;
        this.keyPrefix = keyPrefix;
        this.session = session;
        this.vertx = vertx;
    }

    @Override
    public Future<Long> get() {
        if (session == null) return Future.succeededFuture(0L);
        return vertx.executeBlocking(this::readFromZenoh);
    }

    @Override
    public Future<Long> incrementAndGet() {
        if (session == null) return Future.succeededFuture(1L);
        return vertx.executeBlocking(() -> readModifyWrite(1));
    }

    @Override
    public Future<Long> getAndIncrement() {
        if (session == null) return Future.succeededFuture(0L);
        return vertx.executeBlocking(() -> readModifyWrite(1) - 1);
    }

    @Override
    public Future<Long> decrementAndGet() {
        if (session == null) return Future.succeededFuture(-1L);
        return vertx.executeBlocking(() -> readModifyWrite(-1));
    }

    @Override
    public Future<Long> addAndGet(long amount) {
        if (session == null) return Future.succeededFuture(amount);
        return vertx.executeBlocking(() -> readModifyWrite(amount));
    }

    @Override
    public Future<Long> getAndAdd(long amount) {
        if (session == null) return Future.succeededFuture(0L);
        return vertx.executeBlocking(() -> readModifyWrite(amount) - amount);
    }

    @Override
    public Future<Boolean> compareAndSet(long expected, long update) {
        if (session == null) return Future.succeededFuture(false);
        return vertx.executeBlocking(() -> {
            String key = keyPrefix.counterKey(counterName);
            try {
                long current = readFromZenoh();
                if (current == expected) {
                    writeToZenoh(key, update);
                    return true;
                }
                return false;
            } catch (Exception e) {
                logger.warn("compareAndSet failed: {}", e.getMessage());
                return false;
            }
        });
    }

    private long readModifyWrite(long delta) {
        String key = keyPrefix.counterKey(counterName);
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                long current = readFromZenoh();
                long newValue = current + delta;
                writeToZenoh(key, newValue);
                return newValue;
            } catch (Exception e) {
                logger.debug("readModifyWrite attempt {} failed: {}", attempt + 1, e.getMessage());
            }
        }
        logger.warn("readModifyWrite exhausted {} retries for counter '{}'", MAX_RETRIES, counterName);
        return delta;
    }

    private long readFromZenoh() {
        String key = keyPrefix.counterKey(counterName);
        try {
            List<Reply> replies = session.get(key);
            for (Reply reply : replies) {
                if (reply.success() && reply.samplePayload().isPresent()) {
                    byte[] data = reply.samplePayload().get();
                    if (data.length == 8) {
                        return bytesToLong(data);
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to read counter from Zenoh: {}", e.getMessage());
        }
        return 0;
    }

    private void writeToZenoh(String key, long val) {
        try {
            session.put(key, longToBytes(val));
        } catch (Exception e) {
            throw new RuntimeException("Failed to write counter to Zenoh: " + e.getMessage(), e);
        }
    }

    private static byte[] longToBytes(long val) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(val);
        return buffer.array();
    }

    private static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }
}
