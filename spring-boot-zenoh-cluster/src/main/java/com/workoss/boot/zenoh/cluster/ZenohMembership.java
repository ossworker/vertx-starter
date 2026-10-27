package com.workoss.boot.zenoh.cluster;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a cluster membership handler on a Spring bean method. The container registers it with
 * {@link ZenohCluster#addMembershipListener(MembershipListener, int)} once all singletons are
 * ready, so the handler sees membership changes of the running cluster and keeps working across
 * restarts.
 * <p>
 * The annotated method takes exactly one parameter of type {@link NodeInfo} and returns
 * {@code void}; exceptions are caught and logged. This node never reports itself, so only peer
 * nodes are ever passed in. A peer that restarts is reported as {@code nodeLeft} for the old
 * incarnation followed by {@code nodeJoined} for the new one.
 * <p>
 * Handlers are invoked one event at a time, off the Zenoh receive threads. Spring's {@code @Order}
 * on the method (or its bean) fixes the order across handlers: lower values first, equal values in
 * registration order.
 * <pre>{@code
 * @Component
 * class NodeLifecycle {
 *     @ZenohMembership(Event.NODE_JOINED)
 *     void onJoin(NodeInfo node) { log.info("{} joined", node.nodeId()); }
 *
 *     @ZenohMembership(Event.NODE_LEFT)
 *     void onLeft(NodeInfo node) { log.info("{} left", node.nodeId()); }
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ZenohMembership {

    /**
     * Membership events this handler is interested in; both by default.
     */
    Event[] value() default { Event.NODE_JOINED, Event.NODE_LEFT };

    enum Event {

        /** A peer node appeared, or reappeared with a new incarnation. */
        NODE_JOINED,

        /** A peer node left gracefully, or missed heartbeats for the failure timeout. */
        NODE_LEFT
    }
}
