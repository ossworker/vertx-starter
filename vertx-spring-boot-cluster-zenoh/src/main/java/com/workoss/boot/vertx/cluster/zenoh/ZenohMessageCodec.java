package com.workoss.boot.vertx.cluster.zenoh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.HashMap;
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

    //private final ObjectMapper objectMapper = new ObjectMapper();

    private final ObjectMapper objectMapper;

    public ZenohMessageCodec(ObjectMapper objectMapper) {
        Assert.notNull(objectMapper,"jackson objectMapper 不能为空");
        this.objectMapper = objectMapper;
    }

    /**
     * Encode a simple message with address, body, and optional headers.
     * Used by ZenohEventBusBridge for publishing without creating a full Message object.
     */
    public byte[] encodeSimple(String address, Object body, MultiMap headers) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("messageId", UUID.randomUUID().toString());
            jsonMap.put("address", address);

            // Encode headers
            if (headers != null && !headers.isEmpty()) {
                Map<String, String> headerMap = new HashMap<>();
                headers.forEach(entry -> headerMap.put(entry.getKey(), entry.getValue()));
                jsonMap.put("headers", headerMap);
            }

            // Encode body
            if (body != null) {
                jsonMap.put("body", body);
                jsonMap.put("bodyType", body.getClass().getName());
            }

            return objectMapper.writeValueAsBytes(jsonMap);
        } catch (JsonProcessingException e) {
            logger.error("Failed to encode simple message", e);
            throw new RuntimeException("Failed to encode simple message", e);
        }
    }

    /**
     * Encode a Vert.x Message to JSON bytes.
     */
    public byte[] encode(Message<?> message) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("messageId", UUID.randomUUID().toString());
            jsonMap.put("address", message.address());

            if (message.replyAddress() != null) {
                jsonMap.put("replyAddress", message.replyAddress());
            }

            // Encode headers
            MultiMap headers = message.headers();
            if (headers != null && !headers.isEmpty()) {
                Map<String, String> headerMap = new HashMap<>();
                headers.forEach(entry -> headerMap.put(entry.getKey(), entry.getValue()));
                jsonMap.put("headers", headerMap);
            }

            // Encode body
            Object body = message.body();
            if (body != null) {
                jsonMap.put("body", body);
                jsonMap.put("bodyType", body.getClass().getName());
            }

            return objectMapper.writeValueAsBytes(jsonMap);
        } catch (JsonProcessingException e) {
            logger.error("Failed to encode message", e);
            throw new RuntimeException("Failed to encode message", e);
        }
    }

    /**
     * Decode JSON bytes to a DecodedMessage wrapper.
     * Note: This does not create a full Vert.x Message, just extracts the data.
     */
    @SuppressWarnings("unchecked")
    public DecodedMessage decode(byte[] data) {
        try {
            Map<String, Object> jsonMap = objectMapper.readValue(data, Map.class);

            String address = (String) jsonMap.get("address");
            String replyAddress = (String) jsonMap.get("replyAddress");
            Object body = jsonMap.get("body");

            DeliveryOptions deliveryOptions = new DeliveryOptions();
            if (jsonMap.containsKey("headers")) {
                Map<String, Object> headers = (Map<String, Object>) jsonMap.get("headers");
                if (headers != null) {
                    headers.forEach((k, v) -> deliveryOptions.addHeader(k, v.toString()));
                }
            }

            return new DecodedMessage(address, replyAddress, body, deliveryOptions);
        } catch (IOException e) {
            logger.error("Failed to decode message", e);
            throw new RuntimeException("Failed to decode message", e);
        }
    }

    /**
     * Build query payload for point-to-point send operations.
     */
    public byte[] buildSendPayload(Message<?> message, String replyId) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("messageId", UUID.randomUUID().toString());
            jsonMap.put("address", message.address());
            jsonMap.put("replyId", replyId);

            if (message.replyAddress() != null) {
                jsonMap.put("originalReplyAddress", message.replyAddress());
            }

            // Encode headers
            MultiMap headers = message.headers();
            if (headers != null && !headers.isEmpty()) {
                Map<String, String> headerMap = new HashMap<>();
                headers.forEach(entry -> headerMap.put(entry.getKey(), entry.getValue()));
                jsonMap.put("headers", headerMap);
            }

            // Encode body
            Object body = message.body();
            if (body != null) {
                jsonMap.put("body", body);
                jsonMap.put("bodyType", body.getClass().getName());
            }

            return objectMapper.writeValueAsBytes(jsonMap);
        } catch (JsonProcessingException e) {
            logger.error("Failed to build send payload", e);
            throw new RuntimeException("Failed to build send payload", e);
        }
    }

    /**
     * Decode send payload from a GET query.
     */
    @SuppressWarnings("unchecked")
    public SendRequest decodeSendPayload(byte[] data) {
        try {
            Map<String, Object> jsonMap = objectMapper.readValue(data, Map.class);

            String address = (String) jsonMap.get("address");
            String replyId = (String) jsonMap.get("replyId");
            String originalReplyAddress = (String) jsonMap.get("originalReplyAddress");
            Object body = jsonMap.get("body");

            DeliveryOptions deliveryOptions = new DeliveryOptions();
            if (jsonMap.containsKey("headers")) {
                Map<String, Object> headers = (Map<String, Object>) jsonMap.get("headers");
                if (headers != null) {
                    headers.forEach((k, v) -> deliveryOptions.addHeader(k, v.toString()));
                }
            }

            return new SendRequest(address, replyId, originalReplyAddress, body, deliveryOptions);
        } catch (IOException e) {
            logger.error("Failed to decode send payload", e);
            throw new RuntimeException("Failed to decode send payload", e);
        }
    }

    /**
     * Build reply payload for request/reply responses.
     */
    public byte[] buildReplyPayload(String replyId, Message<?> replyMessage) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("replyId", replyId);
            jsonMap.put("address", replyMessage.address());

            Object body = replyMessage.body();
            if (body != null) {
                jsonMap.put("body", body);
                jsonMap.put("bodyType", body.getClass().getName());
            }

            return objectMapper.writeValueAsBytes(jsonMap);
        } catch (JsonProcessingException e) {
            logger.error("Failed to build reply payload", e);
            throw new RuntimeException("Failed to build reply payload", e);
        }
    }

    /**
     * Decode reply payload from a GET response.
     */
    @SuppressWarnings("unchecked")
    public ReplyResponse decodeReplyPayload(byte[] data) {
        try {
            Map<String, Object> jsonMap = objectMapper.readValue(data, Map.class);

            String replyId = (String) jsonMap.get("replyId");
            String address = (String) jsonMap.get("address");
            Object body = jsonMap.get("body");

            return new ReplyResponse(replyId, address, body);
        } catch (IOException e) {
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
