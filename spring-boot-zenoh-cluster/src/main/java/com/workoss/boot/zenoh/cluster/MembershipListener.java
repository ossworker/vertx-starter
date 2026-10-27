package com.workoss.boot.zenoh.cluster;

/**
 * Cluster membership changes of peer nodes; this node never reports itself. A peer that restarts
 * is reported as {@code nodeLeft} for the old incarnation followed by {@code nodeJoined} for the
 * new one.
 */
public interface MembershipListener {

    void nodeJoined(NodeInfo node);

    /**
     * Fired on graceful leave and when the node misses heartbeats for the failure timeout.
     */
    void nodeLeft(NodeInfo node);
}
