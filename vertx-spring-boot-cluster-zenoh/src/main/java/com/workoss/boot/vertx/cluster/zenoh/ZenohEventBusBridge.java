package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges local Vert.x EventBus operations to Zenoh for cross-node communication.
 * <p>
 * This wrapper intercepts EventBus publish operations and routes them through
 * Zenoh to reach remote nodes, while keeping local deliveries on the native EventBus.
 * <p>
 * Usage: Instead of calling vertx.eventBus().publish(), call bridge.publish()
 * to ensure messages are delivered to all nodes in the cluster.
 */
public class ZenohEventBusBridge {

    private static final Logger logger = LoggerFactory.getLogger(ZenohEventBusBridge.class);

    private final EventBus eventBus;
    private final ZenohClusterManager clusterManager;

    public ZenohEventBusBridge(EventBus eventBus, ZenohClusterManager clusterManager) {
        this.eventBus = eventBus;
        this.clusterManager = clusterManager;
    }

    /**
     * Publish a message to all nodes subscribed to the address (both local and remote).
     * This is the main method for cross-node broadcast.
     */
    public void publish(String address, Object message) {
        publish(address, message, new DeliveryOptions());
    }

    /**
     * Publish a message with delivery options to all nodes.
     */
    public void publish(String address, Object message, DeliveryOptions options) {
        logger.debug("Publishing message to address: {}", address);

        // First, deliver locally via native EventBus
        eventBus.publish(address, message, options);

        // Then, publish via Zenoh for remote nodes (lazy load publisher)
        ZenohEventBusPublisher publisher = clusterManager.getEventBusPublisher();
        if (publisher != null) {
            publisher.publish(address, message, options.getHeaders());
        } else {
            logger.warn("ZenohEventBusPublisher not initialized yet, skipping Zenoh publish for address: {}", address);
        }
    }

    /**
     * Register a consumer for an address on the local EventBus.
     * Delegates to the native EventBus.
     */
    public <T> MessageConsumer<T> consumer(String address) {
        return eventBus.consumer(address);
    }

    /**
     * Register a consumer with a handler on the local EventBus.
     */
    public <T> MessageConsumer<T> consumer(String address, io.vertx.core.Handler<Message<T>> handler) {
        return eventBus.consumer(address, handler);
    }

    /**
     * Get the underlying native EventBus for direct access.
     */
    public EventBus getEventBus() {
        return eventBus;
    }
}
