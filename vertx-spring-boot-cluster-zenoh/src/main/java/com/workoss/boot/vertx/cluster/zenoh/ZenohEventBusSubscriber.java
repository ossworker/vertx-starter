package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.zenoh.Session;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.pubsub.CallbackSubscriber;
import io.zenoh.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribes to EventBus messages from remote nodes via Zenoh Pub/Sub.
 */
public class ZenohEventBusSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(ZenohEventBusSubscriber.class);

    private final Session session;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMessageCodec codec;
    private final Vertx vertx;
    private final String localNodeId;

    private CallbackSubscriber pubSubscriber;
    private CallbackSubscriber inboxSubscriber;

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
        subscribeToPubMessages();
        subscribeToInboxMessages();
        logger.info("EventBus subscriber started for nodeId: {}", localNodeId);
    }

    /**
     * Stop subscribing and release resources.
     */
    public void stop() {
        if (pubSubscriber != null) {
            pubSubscriber.undeclare();
            pubSubscriber = null;
        }
        if (inboxSubscriber != null) {
            inboxSubscriber.undeclare();
            inboxSubscriber = null;
        }
        logger.info("EventBus subscriber stopped for nodeId: {}", localNodeId);
    }

    /**
     * Subscribe to publish (broadcast) messages.
     */
    private void subscribeToPubMessages() {
        try {
            String pubPattern = keyPrefix.eventBusPubPattern();
            KeyExpr keyExpr = KeyExpr.tryFrom(pubPattern);

            this.pubSubscriber = session.declareSubscriber(keyExpr, (Sample sample) -> {
                try {
                    byte[] data = sample.getPayload().toBytes();
                    ZenohMessageCodec.DecodedMessage decoded = codec.decode(data);

                    String address = decoded.getAddress();
                    Object body = decoded.getBody();
                    DeliveryOptions options = decoded.getDeliveryOptions();

                    // Use publish() to broadcast to all local consumers of this address
                    vertx.eventBus().publish(address, body, options);

                    logger.debug("Received published message for address: {} from remote node", address);
                } catch (Exception e) {
                    logger.error("Failed to process published message", e);
                }
            });

            logger.debug("Subscribed to publish messages with pattern: {}", pubPattern);
        } catch (ZError e) {
            logger.error("Failed to subscribe to publish messages", e);
        }
    }

    /**
     * Subscribe to inbox (point-to-point) messages for this node.
     */
    private void subscribeToInboxMessages() {
        try {
            String inboxKey = keyPrefix.eventBusInboxKey(localNodeId);
            KeyExpr keyExpr = KeyExpr.tryFrom(inboxKey);

            this.inboxSubscriber = session.declareSubscriber(keyExpr, (Sample sample) -> {
                try {
                    byte[] payload = sample.getPayload().toBytes();
                    ZenohMessageCodec.SendRequest sendRequest = codec.decodeSendPayload(payload);

                    String address = sendRequest.getAddress();
                    Object body = sendRequest.getBody();
                    DeliveryOptions options = sendRequest.getDeliveryOptions();

                    logger.debug("Received send request for address: {}", address);

                    // Forward to local EventBus handler
                    vertx.eventBus().send(address, body, options);

                } catch (Exception e) {
                    logger.error("Failed to process send request", e);
                }
            });

            logger.debug("Subscribed to inbox messages for key: {}", inboxKey);
        } catch (ZError e) {
            logger.error("Failed to subscribe to inbox messages", e);
        }
    }

    /**
     * Get the Vert.x instance used by this subscriber.
     */
    public io.vertx.core.Vertx getVertx() {
        return vertx;
    }
}
