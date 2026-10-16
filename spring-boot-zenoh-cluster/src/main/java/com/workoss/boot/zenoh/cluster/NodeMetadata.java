package com.workoss.boot.zenoh.cluster;

import java.nio.charset.StandardCharsets;

/**
 * Immutable description of a cluster node, published under {@code nodeinfo/{nodeId}} and refreshed
 * on every heartbeat so late joiners can resolve peers without a storage backend.
 * <p>
 * Wire format: {@code nodeId\nstartedAtMillis\nhostName} in UTF-8. The decode side is tolerant of
 * missing trailing fields so a node running a newer version stays readable.
 */
public record NodeMetadata(String nodeId, long startedAt, String hostName) {

    /**
     * Encode as {@code nodeId\nstartedAt\nhostName} UTF-8 bytes.
     */
    public static byte[] encode(NodeMetadata metadata) {
        String text = metadata.nodeId() + "\n" + metadata.startedAt() + "\n"
                + (metadata.hostName() == null ? "" : metadata.hostName());
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decode from the wire format; returns {@code null} when the payload is unusable.
     */
    public static NodeMetadata decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        String[] parts = new String(payload, StandardCharsets.UTF_8).split("\n", -1);
        if (parts.length == 0 || parts[0].isEmpty()) {
            return null;
        }
        long startedAt = 0L;
        if (parts.length > 1 && !parts[1].isEmpty()) {
            try {
                startedAt = Long.parseLong(parts[1]);
            } catch (NumberFormatException ignored) {
                startedAt = 0L;
            }
        }
        String host = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
        return new NodeMetadata(parts[0], startedAt, host);
    }
}
