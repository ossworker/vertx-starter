package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Future;
import io.vertx.core.shareddata.Counter;
import io.zenoh.Session;
import io.zenoh.bytes.ZBytes;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.query.Reply;
import io.zenoh.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counter implementation backed by Zenoh key-value store.
 * Uses local AtomicLong with Zenoh synchronization.
 * Key: vertx/{app}/{env}/cluster/counters/{counterName}
 *
 * For distributed consistency, the counter value is published to Zenoh
 * after each local update, allowing other nodes to read the latest value.
 */
public class ZenohCounter implements Counter {

    private static final Logger logger = LoggerFactory.getLogger(ZenohCounter.class);

    private final String counterName;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;
    private final AtomicLong value = new AtomicLong(0);

    public ZenohCounter(String counterName, KeyPrefixStrategy keyPrefix, Session session) {
        this.counterName = counterName;
        this.keyPrefix = keyPrefix;
        this.session = session;
        // Try to load current value from Zenoh on creation
        loadFromZenoh();
    }

    @Override
    public Future<Long> get() {
        return Future.succeededFuture(value.get());
    }

    @Override
    public Future<Long> incrementAndGet() {
        long result = value.incrementAndGet();
        publishToZenoh(result);
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Long> getAndIncrement() {
        long result = value.getAndIncrement();
        publishToZenoh(value.get());
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Long> decrementAndGet() {
        long result = value.decrementAndGet();
        publishToZenoh(result);
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Long> addAndGet(long amount) {
        long result = value.addAndGet(amount);
        publishToZenoh(result);
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Long> getAndAdd(long amount) {
        long result = value.getAndAdd(amount);
        publishToZenoh(value.get());
        return Future.succeededFuture(result);
    }

    @Override
    public Future<Boolean> compareAndSet(long expected, long update) {
        boolean success = value.compareAndSet(expected, update);
        if (success) {
            publishToZenoh(update);
        }
        return Future.succeededFuture(success);
    }

    private void publishToZenoh(long val) {
        if (session == null) {
            return;
        }
        String key = keyPrefix.counterKey(counterName);
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            byte[] data = longToBytes(val);
            session.put(keyExpr, ZBytes.from(data));
            keyExpr.close();
        } catch (ZError e) {
            logger.warn("Failed to publish counter to Zenoh: {}", e.getMessage());
        }
    }

    private void loadFromZenoh() {
        if (session == null) {
            return;
        }
        String key = keyPrefix.counterKey(counterName);
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            BlockingQueue<Optional<Reply>> replies = session.get(keyExpr.into());
            keyExpr.close();
            while (true) {
                Optional<Reply> wrapper = replies.take();
                if (wrapper.isEmpty()) {
                    break;
                }
                Reply reply = wrapper.get();
                if (reply instanceof Reply.Success success) {
                    Sample sample = success.getSample();
                    byte[] data = sample.getPayload().toBytes();
                    if (data.length == 8) {
                        long stored = bytesToLong(data);
                        value.set(stored);
                        logger.debug("Loaded counter '{}' value from Zenoh: {}", counterName, stored);
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to load counter from Zenoh, starting at 0: {}", e.getMessage());
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
