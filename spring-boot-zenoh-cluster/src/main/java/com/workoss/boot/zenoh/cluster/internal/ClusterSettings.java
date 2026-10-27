package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscovery;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import com.workoss.zenoh.link.LinkProtocol;

import java.util.EnumSet;

/**
 * Resolved settings of one cluster node. All durations are in milliseconds.
 *
 * @param antiEntropyMillis interval of the periodic full resync; {@code <= 0} disables it
 */
public record ClusterSettings(String nodeId, String appName, String environment,
                              ZenohTransport.Settings transport, long heartbeatMillis,
                              long failureTimeoutMillis, long tombstoneRetentionMillis,
                              long antiEntropyMillis, DnsPeerDiscovery.Settings dns) {

    public ClusterSettings {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (heartbeatMillis <= 0) {
            throw new IllegalArgumentException("heartbeat interval must be positive");
        }
        if (failureTimeoutMillis <= heartbeatMillis) {
            throw new IllegalArgumentException("failure timeout must be longer than the heartbeat interval");
        }
        if (tombstoneRetentionMillis <= 0) {
            throw new IllegalArgumentException("tombstone retention must be positive");
        }
        if (dns.enabled()) {
            if (transport.mode() != ZenohTransport.Mode.PEER) {
                throw new IllegalArgumentException("DNS discovery requires zenoh.cluster.mode=peer");
            }
            if (transport.listen().isEmpty()) {
                throw new IllegalArgumentException("DNS discovery requires zenoh.cluster.listen");
            }
            if (!EnumSet.of(LinkProtocol.TCP, LinkProtocol.UDP, LinkProtocol.TLS, LinkProtocol.QUIC,
                    LinkProtocol.WS).contains(transport.protocol())) {
                throw new IllegalArgumentException("DNS discovery requires an IP link protocol");
            }
        }
    }
}
