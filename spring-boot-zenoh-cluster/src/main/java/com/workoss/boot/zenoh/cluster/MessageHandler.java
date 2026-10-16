package com.workoss.boot.zenoh.cluster;

/**
 * Handler for cluster messages. Handlers run on the broker's dispatch executor, so they may block
 * briefly; long-running work should be handed off to the application's own executor.
 */
public interface MessageHandler {

    /**
     * @param message the received message; never {@code null}
     */
    void onMessage(ClusterMessage message);
}
