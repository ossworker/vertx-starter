package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * Encodes and decodes Vert.x EventBus messages to/from JSON for Zenoh transport.
 *
 * JSON structure:
 * {
 *   "messageId": "uuid",
 *   "address": "my.address",
 *   "replyAddress": "optional-reply-address",
 *   "headers": {"key": "value"},
 *   "body": {...},
 *   "bodyType": "java.lang.String"
 * }
 */
public class ZenohMessageCodec {

    private static final Logger logger = LoggerFactory.getLogger(ZenohMessageCodec.class);

    public ZenohMessageCodec() {
    }

    /**
     * Encode a simple message with address, body, and optional headers.
     * Used by ZenohEventBusBridge for publishing without creating a full Message object.
     */
    public byte[] encodeSimple(String address, Object body, MultiMap headers) {
        try {
            JsonObject json = new JsonObject()
                    .put("messageId", UUID.randomUUID().toString())
                    .put("address", address);

            // Encode headers
            if (headers != null && !headers.isEmpty()) {
                JsonObject headerJson = new JsonObject();
                headers.forEach(entry -> headerJson.put(entry.getKey(), entry.getValue()));
                json.put("headers", headerJson);
            }

            // Encode body
            if (body != null) {
                json.put("body", body);
                json.put("bodyType", body.getClass().getName());
            }

            return json.toBuffer().getBytes();
        } catch (Exception e) {
            logger.error("Failed to encode simple message", e);
            throw new RuntimeException("Failed to encode simple message", e);
        }
    }

    /**
     * Encode a Vert.x Message to JSON bytes.
     */
    public byte[] encode(Message<?> message) {
        try {
            JsonObject json = new JsonObject()
                    .put("messageId", UUID.randomUUID().toString())
                    .put("address", message.address());

            if (message.replyAddress() != null) {
                json.put("replyAddress", message.replyAddress());
            }

            // Encode headers
            MultiMap headers = message.headers();
            if (headers != null && !headers.isEmpty()) {
                JsonObject headerJson = new JsonObject();
                headers.forEach(entry -> headerJson.put(entry.getKey(), entry.getValue()));
                json.put("headers", headerJson);
            }

            // Encode body
            Object body = message.body();
            if (body != null) {
                json.put("body", body);
                json.put("bodyType", body.getClass().getName());
            }

            return json.toBuffer().getBytes();
        } catch (Exception e) {
            logger.error("Failed to encode message", e);
            throw new RuntimeException("Failed to encode message", e);
        }
    }

    /**
     * Decode JSON bytes to a DecodedMessage wrapper.
     * Note: This does not create a full Vert.x Message, just extracts the data.
     */
    public DecodedMessage decode(byte[] data) {
        try {
            JsonObject json = new JsonObject(io.vertx.core.buffer.Buffer.buffer(data));

            String address = json.getString("address");
            String replyAddress = json.getString("replyAddress");
            Object body = json.getValue("body");

            DeliveryOptions deliveryOptions = new DeliveryOptions();
            JsonObject headers = json.getJsonObject("headers");
            if (headers != null) {
                headers.forEach(entry -> deliveryOptions.addHeader(entry.getKey(), entry.getValue().toString()));
            }

            return new DecodedMessage(address, replyAddress, body, deliveryOptions);
        } catch (Exception e) {
            logger.error("Failed to decode message", e);
            throw new RuntimeException("Failed to decode message", e);
        }
    }

    /**
     * Build query payload for point-to-point send operations.
     */
    public byte[] buildSendPayload(Message<?> message, String replyId) {
        try {
            JsonObject json = new JsonObject()
                    .put("messageId", UUID.randomUUID().toString())
                    .put("address", message.address())
                    .put("replyId", replyId);

            if (message.replyAddress() != null) {
                json.put("originalReplyAddress", message.replyAddress());
            }

            // Encode headers
            MultiMap headers = message.headers();
            if (headers != null && !headers.isEmpty()) {
                JsonObject headerJson = new JsonObject();
                headers.forEach(entry -> headerJson.put(entry.getKey(), entry.getValue()));
                json.put("headers", headerJson);
            }

            // Encode body
            Object body = message.body();
            if (body != null) {
                json.put("body", body);
                json.put("bodyType", body.getClass().getName());
            }

            return json.toBuffer().getBytes();
        } catch (Exception e) {
            logger.error("Failed to build send payload", e);
            throw new RuntimeException("Failed to build send payload", e);
        }
    }

    /**
     * Decode send payload from a GET query.
     */
    public SendRequest decodeSendPayload(byte[] data) {
        try {
            JsonObject json = new JsonObject(io.vertx.core.buffer.Buffer.buffer(data));

            String address = json.getString("address");
            String replyId = json.getString("replyId");
            String originalReplyAddress = json.getString("originalReplyAddress");
            Object body = json.getValue("body");

            DeliveryOptions deliveryOptions = new DeliveryOptions();
            JsonObject headers = json.getJsonObject("headers");
            if (headers != null) {
                headers.forEach(entry -> deliveryOptions.addHeader(entry.getKey(), entry.getValue().toString()));
            }

            return new SendRequest(address, replyId, originalReplyAddress, body, deliveryOptions);
        } catch (Exception e) {
            logger.error("Failed to decode send payload", e);
            throw new RuntimeException("Failed to decode send payload", e);
        }
    }

    /**
     * Build reply payload for request/reply responses.
     */
    public byte[] buildReplyPayload(String replyId, Message<?> replyMessage) {
        try {
            JsonObject json = new JsonObject()
                    .put("replyId", replyId)
                    .put("address", replyMessage.address());

            Object body = replyMessage.body();
            if (body != null) {
                json.put("body", body);
                json.put("bodyType", body.getClass().getName());
            }

            return json.toBuffer().getBytes();
        } catch (Exception e) {
            logger.error("Failed to build reply payload", e);
            throw new RuntimeException("Failed to build reply payload", e);
        }
    }

    /**
     * Decode reply payload from a GET response.
     */
    public ReplyResponse decodeReplyPayload(byte[] data) {
        try {
            JsonObject json = new JsonObject(io.vertx.core.buffer.Buffer.buffer(data));

            String replyId = json.getString("replyId");
            String address = json.getString("address");
            Object body = json.getValue("body");

            return new ReplyResponse(replyId, address, body);
        } catch (Exception e) {
            logger.error("Failed to decode reply payload", e);
            throw new RuntimeException("Failed to decode reply payload", e);
        }
    }

    /**
     * Represents a decoded message with address, body, and options.
     */
    public static class DecodedMessage {
        private final String address;
        private final String replyAddress;
        private final Object body;
        private final DeliveryOptions deliveryOptions;

        public DecodedMessage(String address, String replyAddress, Object body, DeliveryOptions deliveryOptions) {
            this.address = address;
            this.replyAddress = replyAddress;
            this.body = body;
            this.deliveryOptions = deliveryOptions;
        }

        public String getAddress() { return address; }
        public String getReplyAddress() { return replyAddress; }
        public Object getBody() { return body; }
        public DeliveryOptions getDeliveryOptions() { return deliveryOptions; }
    }

    /**
     * Represents a decoded send request from a remote node.
     */
    public static class SendRequest {
        private final String address;
        private final String replyId;
        private final String originalReplyAddress;
        private final Object body;
        private final DeliveryOptions deliveryOptions;

        public SendRequest(String address, String replyId, String originalReplyAddress,
                          Object body, DeliveryOptions deliveryOptions) {
            this.address = address;
            this.replyId = replyId;
            this.originalReplyAddress = originalReplyAddress;
            this.body = body;
            this.deliveryOptions = deliveryOptions;
        }

        public String getAddress() { return address; }
        public String getReplyId() { return replyId; }
        public String getOriginalReplyAddress() { return originalReplyAddress; }
        public Object getBody() { return body; }
        public DeliveryOptions getDeliveryOptions() { return deliveryOptions; }
    }

    /**
     * Represents a decoded reply response from a remote node.
     */
    public static class ReplyResponse {
        private final String replyId;
        private final String address;
        private final Object body;

        public ReplyResponse(String replyId, String address, Object body) {
            this.replyId = replyId;
            this.address = address;
            this.body = body;
        }

        public String getReplyId() { return replyId; }
        public String getAddress() { return address; }
        public Object getBody() { return body; }
    }
}
