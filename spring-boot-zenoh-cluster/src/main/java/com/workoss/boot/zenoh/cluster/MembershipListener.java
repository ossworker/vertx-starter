package com.workoss.boot.zenoh.cluster;

/**
 * Listener for cluster membership changes. Register with
 * {@link ZenohClusterManager#addMembershipListener(MembershipListener)}.
 * <p>
 * Callbacks fire on the cluster's internal poll threads; implementations must not block.
 */
public interface MembershipListener {

    /**
     * A peer node became visible, either discovered on start or seen joining afterwards.
     *
     * @param nodeId the joined node's id
     */
    void nodeJoined(String nodeId);

    /**
     * A peer node left the cluster (liveliness key deleted or expired).
     *
     * @param nodeId the departed node's id
     */
    void nodeLeft(String nodeId);
}
