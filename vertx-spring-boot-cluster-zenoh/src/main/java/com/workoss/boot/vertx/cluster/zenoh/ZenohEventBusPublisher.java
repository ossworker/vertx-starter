package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.Message;
import io.zenoh.Session;
import io.zenoh.bytes.ZBytes;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.pubsub.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles EventBus publish operations via Zenoh Pub/Sub.
 * <p>
 * When eventBus.publish(address, message) is called:
 * 1. Message is serialized to JSON
 * 2. Published to Zenoh key: vertx/{app}/{env}/eventbus/pub/{address}
 * 3. All nodes subscribed to that address receive the message
 * <p>
 * Performance optimizations:
 * - Caches both Publisher and KeyExpr instances to avoid repeated creation
 * - Uses ConcurrentHashMap for thread-safe lazy initialization
 */
public class ZenohEventBusPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZenohEventBusPublisher.class);

    private final Session session;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMessageCodec codec;
    private final Map<String, Publisher> publishers = new ConcurrentHashMap<>();
    private final Map<String, KeyExpr> keyExprs = new ConcurrentHashMap<>();

    public ZenohEventBusPublisher(Session session, KeyPrefixStrategy keyPrefix, ZenohMessageCodec codec) {
        this.session = session;
        this.keyPrefix = keyPrefix;
        this.codec = codec;
    }

    /**
     * Publish a message to all nodes listening on the given address.
     */
    public void publish(String address, Message<?> message) {
        try {
            String key = keyPrefix.eventBusPubKey(address);
            Publisher publisher = getOrCreatePublisher(key);
            byte[] data = codec.encode(message);
            publisher.put(ZBytes.from(data));
            logger.debug("Published message to address: {} via key: {}", address, key);
        } catch (Exception e) {
            logger.error("Failed to publish message to address: {}", address, e);
        }
    }

    /**
     * Publish a message with body and optional headers to all nodes.
     * This is a simpler alternative to publish(Message) for use with the bridge.
     */
    public void publish(String address, Object body, MultiMap headers) {
        try {
            String key = keyPrefix.eventBusPubKey(address);
            Publisher publisher = getOrCreatePublisher(key);
            byte[] data = codec.encodeSimple(address, body, headers);
            publisher.put(ZBytes.from(data));
            logger.debug("Published message to address: {} via key: {}", address, key);
        } catch (Exception e) {
            logger.error("Failed to publish message to address: {}", address, e);
        }
    }

    /**
     * Close all publishers and release resources.
     */
    public void close() {
        // Close all publishers
        publishers.values().forEach(publisher -> {
            try {
                publisher.close();
            } catch (Exception e) {
                logger.warn("Failed to close publisher", e);
            }
        });
        publishers.clear();

        // Close all KeyExpr instances
        keyExprs.values().forEach(keyExpr -> {
            try {
                keyExpr.close();
            } catch (Exception e) {
                logger.warn("Failed to close KeyExpr", e);
            }
        });
        keyExprs.clear();

        logger.info("All publishers closed");
    }

    private Publisher getOrCreatePublisher(String key) throws ZError {
        return publishers.computeIfAbsent(key, k -> {
            try {
                KeyExpr keyExpr = getOrCreateKeyExpr(k);
                Publisher publisher = session.declarePublisher(keyExpr);
                logger.debug("Created publisher for key: {}", k);
                return publisher;
            } catch (ZError e) {
                logger.error("Failed to create publisher for key: {}", k, e);
                throw new RuntimeException("Failed to create publisher", e);
            }
        });
    }

    private KeyExpr getOrCreateKeyExpr(String key) throws ZError {
        return keyExprs.computeIfAbsent(key, k -> {
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(k);
                logger.debug("Created KeyExpr for key: {}", k);
                return keyExpr;
            } catch (ZError e) {
                logger.error("Failed to create KeyExpr for key: {}", k, e);
                throw new RuntimeException("Failed to create KeyExpr", e);
            }
        });
    }
}
