package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Publisher;
import com.workoss.zenoh.Session;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles EventBus publish operations via Zenoh Pub/Sub using zenoh-jni.
 * <p>
 * When eventBus.publish(address, message) is called:
 * 1. Message is serialized to JSON
 * 2. Published to Zenoh key: vertx/{app}/{env}/eventbus/pub/{address}
 * 3. All nodes subscribed to that address receive the message
 */
public class ZenohEventBusPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZenohEventBusPublisher.class);

    private final Session session;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMessageCodec codec;
    private final Map<String, Publisher> publishers = new ConcurrentHashMap<>();

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
            publisher.put(data);
            logger.debug("Published message to address: {} via key: {}", address, key);
        } catch (Exception e) {
            logger.error("Failed to publish message to address: {}", address, e);
        }
    }

    /**
     * Publish a message with body and optional headers to all nodes.
     */
    public void publish(String address, Object body, MultiMap headers) {
        try {
            String key = keyPrefix.eventBusPubKey(address);
            Publisher publisher = getOrCreatePublisher(key);
            byte[] data = codec.encodeSimple(address, body, headers);
            publisher.put(data);
            logger.debug("Published message to address: {} via key: {}", address, key);
        } catch (Exception e) {
            logger.error("Failed to publish message to address: {}", address, e);
        }
    }

    /**
     * Close all publishers and release resources.
     */
    public void close() {
        publishers.values().forEach(publisher -> {
            try {
                publisher.close();
            } catch (Exception e) {
                logger.warn("Failed to close publisher", e);
            }
        });
        publishers.clear();
        logger.info("All publishers closed");
    }

    private Publisher getOrCreatePublisher(String key) {
        return publishers.computeIfAbsent(key, k -> {
            try {
                Publisher publisher = new Publisher(session, k,
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
                logger.debug("Created publisher for key: {}", k);
                return publisher;
            } catch (Exception e) {
                logger.error("Failed to create publisher for key: {}", k, e);
                throw new RuntimeException("Failed to create publisher", e);
            }
        });
    }
}
