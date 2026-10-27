package com.workoss.boot.zenoh.cluster;

/**
 * Registration of a {@link MessageHandler}; cancelling stops delivery, including messages that
 * were queued but not yet handled.
 */
public interface Subscription extends AutoCloseable {

    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
