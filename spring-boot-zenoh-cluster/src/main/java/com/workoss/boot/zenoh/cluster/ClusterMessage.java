package com.workoss.boot.zenoh.cluster;

import java.nio.charset.StandardCharsets;

/**
 * A message received from the cluster.
 *
 * @param address      broadcast address, or {@code null} for a direct message
 * @param sourceNodeId sending node
 * @param targetNodeId receiving node for a direct message, or {@code null} for a broadcast
 * @param payload      message bytes, never {@code null}
 * @param timestamp    sender wall-clock millis
 */
public record ClusterMessage(String address, String sourceNodeId, String targetNodeId,
                             byte[] payload, long timestamp) {

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    public boolean isBroadcast() {
        return address != null;
    }
}
