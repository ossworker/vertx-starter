package com.workoss.boot.zenoh.cluster;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.CompletableFuture;

/**
 * Declares a cluster message handler on a Spring bean method. The container registers it with
 * {@link ClusterMessaging} once all singletons are ready, so the handler receives messages as soon
 * as the cluster is running and keeps working across restarts.
 * <p>
 * The annotated method takes exactly one parameter, which may be:
 * <ul>
 *   <li>{@link ClusterMessage} — the full message (address, source, target, payload, timestamp)</li>
 *   <li>{@code byte[]} — the raw payload</li>
 *   <li>{@link String} — the payload decoded as UTF-8</li>
 * </ul>
 * It may return {@code void} or a value; exceptions are caught and logged, they do not propagate to
 * the sender.
 * <p>
 * Handlers of the same address are invoked in the order given by Spring's {@code @Order} on the
 * method (or its bean): lower values first, equal values in registration order. Ordering an handler
 * also serialises it with its ordered peers on the same address; a handler without {@code @Order}
 * keeps running in parallel. See {@link ClusterMessaging#subscribe(String, MessageHandler, int)}.
 * <p>
 * A non-{@code void} return value can be published as a reply; see {@link #replyTo()}.
 * <pre>{@code
 * @Component
 * class OrderEvents {
 *     @ZenohSubscribe("orders/*")            // matches orders/eu, orders/us, ...
 *     void onOrder(ClusterMessage message) { ... }
 *
 *     @ZenohSubscribe(direct = true)         // point-to-point messages to this node
 *     void onDirect(byte[] payload) { ... }
 *
 *     @ZenohSubscribe(value = "orders/eu", replyTo = "orders/replies")
 *     String onOrderEu(String payload) {     // the returned value is broadcast to orders/replies
 *         return "ack:" + payload;
 *     }
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ZenohSubscribe {

    /**
     * Broadcast address patterns to subscribe to; supports {@code *} and {@code **} segments.
     * Ignored when {@link #direct()} is {@code true}. Must be non-empty otherwise.
     */
    String[] value() default {};

    /**
     * Subscribe to point-to-point messages sent to this node instead of a broadcast address.
     */
    boolean direct() default false;

    /**
     * Broadcast address a non-null return value of this handler is published to. Empty (the
     * default) discards the return value.
     * <p>
     * A {@link #direct()} handler needs no address: its return value is sent back to the node that
     * sent the message, which turns direct messages into request/response.
     * <p>
     * Supported return types are {@code void}, {@link String} (encoded as UTF-8), {@code byte[]}
     * (used as-is) and {@link CompletableFuture} of either; any other type fails the context at
     * startup. A {@link CompletableFuture} is never waited on: its result is published when the
     * future completes and a failed future is logged and publishes nothing.
     * <p>
     * Beware that a reply is an ordinary direct message: if the node that sent the request runs the
     * same replying handler, the reply triggers another reply. Reply only from handlers that run
     * on one node, or branch on the payload.
     */
    String replyTo() default "";
}
