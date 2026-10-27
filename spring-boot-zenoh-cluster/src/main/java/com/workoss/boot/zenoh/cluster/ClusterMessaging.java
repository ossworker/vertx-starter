package com.workoss.boot.zenoh.cluster;

import java.util.Set;

/**
 * Broadcast and point-to-point messaging between cluster nodes.
 * <p>
 * Messages are transient: nodes that are not running when a message is sent never see it. Each
 * subscription receives its messages one at a time, in arrival order, on its own virtual thread;
 * a slow handler only delays itself.
 */
public interface ClusterMessaging {

    boolean isActive();

    /**
     * Delivers {@code payload} to every subscription for {@code address} on every node, this one
     * included.
     *
     * @throws IllegalStateException when the cluster is not running
     */
    void broadcast(String address, byte[] payload);

    /**
     * Delivers {@code payload} to the direct-message subscriptions of exactly one node. Messages
     * to a node that is not running are dropped.
     *
     * @throws IllegalStateException when the cluster is not running
     */
    void send(String targetNodeId, byte[] payload);

    /**
     * Subscribes to a broadcast address. The address may be a wildcard pattern using
     * {@code /}-separated segments: {@code *} matches one segment and {@code **} matches zero or
     * more, e.g. {@code news/*} matches {@code news/eu} while {@code news/**} also matches
     * {@code news/eu/west}. Subscriptions may be registered before the cluster starts and survive
     * restarts.
     */
    Subscription subscribe(String address, MessageHandler handler);

    /**
     * Like {@link #subscribe(String, MessageHandler)}, but {@code order} places this handler among
     * the handlers of the same address: lower values are invoked first, equal values in
     * registration order.
     * <p>
     * Declaring an order also changes <em>how</em> handlers run: the ordered handlers of one
     * address share a single serial executor and are invoked one at a time, in order, so a handler
     * with a higher order can never overtake one with a lower order. A handler registered without
     * an order keeps its own executor and never waits for another handler. Use an order only when
     * the sequence matters and a blocking handler is acceptable.
     */
    Subscription subscribe(String address, MessageHandler handler, int order);

    Subscription onDirectMessage(MessageHandler handler);

    /**
     * Like {@link #onDirectMessage(MessageHandler)}, with the ordering semantics of
     * {@link #subscribe(String, MessageHandler, int)} applied to the direct handlers as a group.
     */
    Subscription onDirectMessage(MessageHandler handler, int order);

    /**
     * @return subscription addresses and wildcard patterns with at least one local subscription
     */
    Set<String> subscribedAddresses();
}
