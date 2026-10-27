package com.workoss.boot.zenoh.cluster.internal.messaging;

import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.MessageHandler;
import com.workoss.boot.zenoh.cluster.Subscription;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public final class DefaultClusterMessaging implements ClusterMessaging {

    private final MessagingRegistry registry;
    private final Supplier<MessagingChannel> channel;

    /**
     * @param channel the current run's channel, or {@code null} while the cluster is not running
     */
    public DefaultClusterMessaging(MessagingRegistry registry, Supplier<MessagingChannel> channel) {
        this.registry = registry;
        this.channel = channel;
    }

    @Override
    public boolean isActive() {
        return channel.get() != null;
    }

    @Override
    public void broadcast(String address, byte[] payload) {
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("address must not be empty");
        }
        requireChannel().broadcast(address, Objects.requireNonNull(payload, "payload"));
    }

    @Override
    public void send(String targetNodeId, byte[] payload) {
        if (targetNodeId == null || targetNodeId.isEmpty()) {
            throw new IllegalArgumentException("targetNodeId must not be empty");
        }
        requireChannel().send(targetNodeId, Objects.requireNonNull(payload, "payload"));
    }

    @Override
    public Subscription subscribe(String address, MessageHandler handler) {
        return registry.subscribe(address, handler);
    }

    @Override
    public Subscription subscribe(String address, MessageHandler handler, int order) {
        return registry.subscribe(address, handler, order);
    }

    @Override
    public Subscription onDirectMessage(MessageHandler handler) {
        return registry.onDirectMessage(handler);
    }

    @Override
    public Subscription onDirectMessage(MessageHandler handler, int order) {
        return registry.onDirectMessage(handler, order);
    }

    @Override
    public Set<String> subscribedAddresses() {
        return registry.addresses();
    }

    private MessagingChannel requireChannel() {
        MessagingChannel current = channel.get();
        if (current == null) {
            throw new IllegalStateException("Zenoh cluster is not running");
        }
        return current;
    }
}
