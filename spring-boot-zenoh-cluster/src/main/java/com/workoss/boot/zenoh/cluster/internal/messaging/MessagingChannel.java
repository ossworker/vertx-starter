package com.workoss.boot.zenoh.cluster.internal.messaging;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.internal.KeySpace;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import com.workoss.zenoh.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Moves messages between Zenoh and the {@link MessagingRegistry} for one cluster run. Zenoh
 * delivers a session's own puts to its own subscribers, which is what gives broadcasts their
 * self-delivery.
 */
public final class MessagingChannel implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MessagingChannel.class);
    private static final byte WIRE_VERSION = 1;
    private static final int HEADER_BYTES = 1 + Long.BYTES + Short.BYTES;

    private final ZenohTransport transport;
    private final KeySpace keys;
    private final String nodeId;
    private final MessagingRegistry registry;
    private final List<AutoCloseable> channels = new ArrayList<>();

    public MessagingChannel(ZenohTransport transport, KeySpace keys, String nodeId, MessagingRegistry registry) {
        this.transport = transport;
        this.keys = keys;
        this.nodeId = nodeId;
        this.registry = registry;
    }

    public void start() {
        channels.add(transport.subscribe(keys.broadcastPattern(), this::onBroadcast, "zenoh-cluster-broadcast"));
        channels.add(transport.subscribe(keys.inboxKey(nodeId), this::onDirect, "zenoh-cluster-inbox"));
    }

    public void broadcast(String address, byte[] payload) {
        transport.put(keys.broadcastKey(address), encode(nodeId, payload));
    }

    public void send(String targetNodeId, byte[] payload) {
        transport.put(keys.inboxKey(targetNodeId), encode(nodeId, payload));
    }

    @Override
    public void close() {
        for (AutoCloseable channel : channels) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.debug("Failed to close messaging channel: {}", e.getMessage());
            }
        }
        channels.clear();
    }

    private void onBroadcast(Sample sample) {
        String address = keys.broadcastAddress(sample.keyExpr());
        ClusterMessage message = address == null ? null : decode(address, null, sample.payload());
        if (message != null) {
            registry.dispatchBroadcast(message);
        }
    }

    private void onDirect(Sample sample) {
        ClusterMessage message = decode(null, nodeId, sample.payload());
        if (message != null) {
            registry.dispatchDirect(message);
        }
    }

    // Wire: [1B version][8B timestamp][2B sourceLen][source][payload]
    static byte[] encode(String sourceNodeId, byte[] payload) {
        byte[] source = sourceNodeId.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(HEADER_BYTES + source.length + payload.length)
                .put(WIRE_VERSION)
                .putLong(System.currentTimeMillis())
                .putShort((short) source.length)
                .put(source)
                .put(payload)
                .array();
    }

    static ClusterMessage decode(String address, String targetNodeId, byte[] wire) {
        if (wire == null || wire.length < HEADER_BYTES || wire[0] != WIRE_VERSION) {
            logger.debug("Dropping malformed message");
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(wire, 1, wire.length - 1);
            long timestamp = buffer.getLong();
            byte[] source = new byte[Short.toUnsignedInt(buffer.getShort())];
            buffer.get(source);
            byte[] payload = new byte[buffer.remaining()];
            buffer.get(payload);
            return new ClusterMessage(address, new String(source, StandardCharsets.UTF_8), targetNodeId,
                    payload, timestamp);
        } catch (BufferUnderflowException e) {
            logger.debug("Dropping truncated message");
            return null;
        }
    }
}
