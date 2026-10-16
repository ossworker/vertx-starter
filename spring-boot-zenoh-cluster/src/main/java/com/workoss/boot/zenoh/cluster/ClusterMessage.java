package com.workoss.boot.zenoh.cluster;

/**
 * A message received from the cluster, either a broadcast or a point-to-point delivery.
 *
 * @param address       the broadcast address, or {@code null} for a point-to-point message
 * @param sourceNodeId  the node that sent the message
 * @param targetNodeId  the recipient node for point-to-point, or {@code null} for a broadcast
 * @param payload       raw message bytes (never {@code null}, may be empty)
 * @param timestamp     sender-side millis when the message was published
 */
public record ClusterMessage(String address, String sourceNodeId, String targetNodeId,
                             byte[] payload, long timestamp) {

    /**
     * @return the payload decoded as a UTF-8 string
     */
    public String payloadAsString() {
        return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * @return {@code true} for a broadcast message
     */
    public boolean isBroadcast() {
        return address != null;
    }
}
