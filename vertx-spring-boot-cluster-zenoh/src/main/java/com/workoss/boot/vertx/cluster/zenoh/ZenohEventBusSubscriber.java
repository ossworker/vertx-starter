package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Subscribes to EventBus messages from remote nodes via Zenoh Pub/Sub.
 * Uses zenoh-jni Subscriber with polling threads.
 */
public class ZenohEventBusSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(ZenohEventBusSubscriber.class);

    private final Session session;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMessageCodec codec;
    private final Vertx vertx;
    private final String localNodeId;

    private Subscriber pubSubscriber;
    private Subscriber inboxSubscriber;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pubPollThread;
    private Thread inboxPollThread;

    public ZenohEventBusSubscriber(Session session, KeyPrefixStrategy keyPrefix, ZenohMessageCodec codec,
                                   Vertx vertx, String localNodeId) {
        this.session = session;
        this.keyPrefix = keyPrefix;
        this.codec = codec;
        this.vertx = vertx;
        this.localNodeId = localNodeId;
    }

    /**
     * Start subscribing to remote messages.
     */
    public void start() {
        running.set(true);
        subscribeToPubMessages();
        subscribeToInboxMessages();
        logger.info("EventBus subscriber started for nodeId: {}", localNodeId);
    }

    /**
     * Stop subscribing and release resources.
     * Order: undeclare subscribers first (makes blocking recv() return empty),
     * then interrupt poll threads, then join to ensure clean exit.
     */
    public void stop() {
        running.set(false);

        // Step 1: Undeclare subscribers first to make blocking recv() return
        if (pubSubscriber != null) {
            try {
                pubSubscriber.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare pub subscriber");
            }
            pubSubscriber = null;
        }
        if (inboxSubscriber != null) {
            try {
                inboxSubscriber.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare inbox subscriber");
            }
            inboxSubscriber = null;
        }

        // Step 2: Interrupt and join poll threads
        if (pubPollThread != null) {
            pubPollThread.interrupt();
            try { pubPollThread.join(2000); } catch (InterruptedException ignored) {}
            pubPollThread = null;
        }
        if (inboxPollThread != null) {
            inboxPollThread.interrupt();
            try { inboxPollThread.join(2000); } catch (InterruptedException ignored) {}
            inboxPollThread = null;
        }

        logger.info("EventBus subscriber stopped for nodeId: {}", localNodeId);
    }

    /**
     * Subscribe to publish (broadcast) messages.
     */
    private void subscribeToPubMessages() {
        try {
            String pubPattern = keyPrefix.eventBusPubPattern();
            this.pubSubscriber = new Subscriber(session, pubPattern);

            pubPollThread = new Thread(() -> {
                while (running.get() && pubSubscriber != null && !pubSubscriber.isUndeclared()) {
                    try {
                        Optional<com.workoss.zenoh.Sample> sampleOpt = pubSubscriber.recv();
                        if (sampleOpt.isPresent()) {
                            com.workoss.zenoh.Sample sample = sampleOpt.get();
                            byte[] data = sample.payload();
                            ZenohMessageCodec.DecodedMessage decoded = codec.decode(data);

                            String address = decoded.getAddress();
                            Object body = decoded.getBody();
                            DeliveryOptions options = decoded.getDeliveryOptions();

                            vertx.eventBus().publish(address, body, options);
                            logger.debug("Received published message for address: {} from remote node", address);
                        }
                    } catch (Exception e) {
                        if (Thread.currentThread().isInterrupted()) break;
                        logger.debug("Pub poll error: {}", e.getMessage());
                    }
                }
            }, "zenoh-pub-poll-" + localNodeId.substring(0, 8));
            pubPollThread.setDaemon(true);
            pubPollThread.start();

            logger.debug("Subscribed to publish messages with pattern: {}", pubPattern);
        } catch (Exception e) {
            logger.error("Failed to subscribe to publish messages", e);
        }
    }

    /**
     * Subscribe to inbox (point-to-point) messages for this node.
     */
    private void subscribeToInboxMessages() {
        try {
            String inboxKey = keyPrefix.eventBusInboxKey(localNodeId);
            this.inboxSubscriber = new Subscriber(session, inboxKey);

            inboxPollThread = new Thread(() -> {
                while (running.get() && inboxSubscriber != null && !inboxSubscriber.isUndeclared()) {
                    try {
                        Optional<com.workoss.zenoh.Sample> sampleOpt = inboxSubscriber.recv();
                        if (sampleOpt.isPresent()) {
                            com.workoss.zenoh.Sample sample = sampleOpt.get();
                            byte[] payload = sample.payload();
                            ZenohMessageCodec.SendRequest sendRequest = codec.decodeSendPayload(payload);

                            String address = sendRequest.getAddress();
                            Object body = sendRequest.getBody();
                            DeliveryOptions options = sendRequest.getDeliveryOptions();

                            logger.debug("Received send request for address: {}", address);
                            vertx.eventBus().send(address, body, options);
                        }
                    } catch (Exception e) {
                        if (Thread.currentThread().isInterrupted()) break;
                        logger.debug("Inbox poll error: {}", e.getMessage());
                    }
                }
            }, "zenoh-inbox-poll-" + localNodeId.substring(0, 8));
            inboxPollThread.setDaemon(true);
            inboxPollThread.start();

            logger.debug("Subscribed to inbox messages for key: {}", inboxKey);
        } catch (Exception e) {
            logger.error("Failed to subscribe to inbox messages", e);
        }
    }

    public io.vertx.core.Vertx getVertx() {
        return vertx;
    }
}
