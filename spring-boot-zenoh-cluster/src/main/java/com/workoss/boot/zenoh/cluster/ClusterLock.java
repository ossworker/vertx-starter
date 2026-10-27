package com.workoss.boot.zenoh.cluster;

/**
 * A held cluster lock, obtained from {@link ZenohCluster#tryLock}.
 * <p>
 * Every acquisition carries a unique token, so the lock excludes other threads of the same node as
 * well as other nodes, and it is not re-entrant. A lock whose holder node is detected as gone is
 * considered free by everyone else. Exclusion is best-effort: two nodes contending within the
 * replication delay can both succeed, after which one of them silently loses the lock — check
 * {@link #isHeld()} before acting on protected state, and use an external coordination service
 * when strict mutual exclusion is required.
 */
public interface ClusterLock extends AutoCloseable {

    String name();

    /**
     * @return {@code true} while this acquisition is still the recorded holder
     */
    boolean isHeld();

    /**
     * Releases the lock if this acquisition still holds it. Idempotent.
     */
    void release();

    @Override
    default void close() {
        release();
    }
}
