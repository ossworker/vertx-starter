package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.zenoh.Session;
import io.zenoh.bytes.ZBytes;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.pubsub.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles EventBus send operations via Zenoh Pub/Sub for point-to-point messaging.
 */
public class ZenohEventBusSender {

    private static final Logger logger = LoggerFactory.getLogger(ZenohEventBusSender.class);

    private final Session session;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMessageCodec codec;
    private final String localNodeId;
    private final Map<String, List<io.vertx.core.spi.cluster.RegistrationInfo>> registrations;

    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public ZenohEventBusSender(Session session, KeyPrefixStrategy keyPrefix, ZenohMessageCodec codec,
                               String localNodeId,
                               Map<String, List<io.vertx.core.spi.cluster.RegistrationInfo>> registrations) {
        this.session = session;
        this.keyPrefix = keyPrefix;
        this.codec = codec;
        this.localNodeId = localNodeId;
        this.registrations = registrations;
    }

    /**
     * Send a message to a handler on a remote node.
     */
    public void send(String address, Message<?> message, Handler<AsyncResult<Message<?>>> replyHandler) {
        List<io.vertx.core.spi.cluster.RegistrationInfo> targetRegistrations = registrations.get(address);
        if (targetRegistrations == null || targetRegistrations.isEmpty()) {
            replyHandler.handle(Future.failedFuture("No handlers for address: " + address));
            return;
        }

        List<io.vertx.core.spi.cluster.RegistrationInfo> remoteRegistrations = targetRegistrations.stream()
                .filter(reg -> !localNodeId.equals(reg.nodeId()))
                .toList();

        if (remoteRegistrations.isEmpty()) {
            replyHandler.handle(Future.failedFuture("No remote handlers for address: " + address));
            return;
        }

        String targetNode = selectNode(remoteRegistrations);
        logger.debug("Sending message to node: {} for address: {}", targetNode, address);

        try {
            byte[] sendPayload = codec.buildSendPayload(message, "dummy-reply-id");

            String inboxKey = keyPrefix.eventBusInboxKey(targetNode);
            KeyExpr keyExpr = KeyExpr.tryFrom(inboxKey);
            Publisher inboxPublisher = session.declarePublisher(keyExpr);
            inboxPublisher.put(ZBytes.from(sendPayload));
            inboxPublisher.close();
            keyExpr.close();

            SimpleMessage responseMessage = new SimpleMessage(address, null, "OK");
            replyHandler.handle(Future.succeededFuture(responseMessage));

        } catch (Exception e) {
            logger.error("Failed to send message to address: {}", address, e);
            replyHandler.handle(Future.failedFuture(e));
        }
    }

    public void cleanup() {
        logger.info("ZenohEventBusSender cleaned up");
    }

    private String selectNode(List<io.vertx.core.spi.cluster.RegistrationInfo> registrations) {
        int index = Math.abs(roundRobinCounter.getAndIncrement()) % registrations.size();
        return registrations.get(index).nodeId();
    }

    /**
     * Simple Message implementation for cross-node responses.
     */
    static class SimpleMessage implements Message<Object> {
        private final String address;
        private final String replyAddress;
        private final Object body;

        SimpleMessage(String address, String replyAddress, Object body) {
            this.address = address;
            this.replyAddress = replyAddress;
            this.body = body;
        }

        @Override
        public String address() { return address; }

        @Override
        public MultiMap headers() { return MultiMap.caseInsensitiveMultiMap(); }

        @Override
        public Object body() { return body; }

        @Override
        public String replyAddress() { return replyAddress; }

        @Override
        public boolean isSend() { return true; }

        @Override
        public void reply(Object message, DeliveryOptions options) { }

        @Override
        public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
            return Future.failedFuture("Not supported");
        }
    }
}
