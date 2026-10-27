package com.workoss.boot.zenoh.cluster;

@FunctionalInterface
public interface MessageHandler {

    void onMessage(ClusterMessage message);
}
