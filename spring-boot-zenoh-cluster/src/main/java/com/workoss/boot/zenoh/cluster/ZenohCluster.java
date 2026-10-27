package com.workoss.boot.zenoh.cluster;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Entry point of the Zenoh cluster: membership, replicated data structures and messaging.
 * <p>
 * All replicated state is eventually consistent: writes are atomic on the local node and converge
 * across nodes by last-writer-wins on a hybrid logical clock. Data lives in memory only and does
 * not survive a full cluster restart.
 * <p>
 * Handles returned by this interface ({@link SharedMap}, {@link ClusterCounter},
 * {@link ClusterMessaging}, membership listeners) stay valid across stop/start cycles; operating
 * on them while the cluster is not running fails with {@link IllegalStateException}.
 */
public interface ZenohCluster {

    /**
     * Stable id of this node; unchanged across restarts of the same instance.
     */
    String nodeId();

    boolean isActive();

    /**
     * Every node currently considered alive, this one included, ordered by node id. Empty while
     * the cluster is not running.
     */
    List<NodeInfo> members();

    Optional<NodeInfo> member(String nodeId);

    /**
     * Listeners are invoked one event at a time, in order, off the Zenoh receive threads.
     */
    void addMembershipListener(MembershipListener listener);

    /**
     * Like {@link #addMembershipListener(MembershipListener)}, but {@code order} places this
     * listener among the others: lower values are invoked first, equal values in registration
     * order. Listeners are still invoked one event at a time, off the Zenoh receive threads.
     */
    void addMembershipListener(MembershipListener listener, int order);

    void removeMembershipListener(MembershipListener listener);

    /**
     * Shared map whose values use {@link ValueCodec#jdk()}.
     */
    <K, V> SharedMap<K, V> sharedMap(String name);

    /**
     * @throws IllegalArgumentException when {@code name} is already bound to a different codec
     */
    <K, V> SharedMap<K, V> sharedMap(String name, ValueCodec<V> valueCodec);

    ClusterCounter counter(String name);

    /**
     * Single acquisition attempt.
     */
    Optional<ClusterLock> tryLock(String name);

    /**
     * Retries until the lock is acquired or {@code timeout} elapses.
     */
    Optional<ClusterLock> tryLock(String name, Duration timeout);

    ClusterMessaging messaging();

    /**
     * Zenoh session id while running.
     */
    Optional<String> sessionId();
}
