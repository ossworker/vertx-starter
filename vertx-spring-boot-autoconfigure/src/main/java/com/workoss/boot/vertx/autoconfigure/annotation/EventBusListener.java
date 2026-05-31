package com.workoss.boot.vertx.autoconfigure.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declaratively register a Vert.x EventBus consumer on a method.
 * <p>
 * The annotated method will be invoked when a message is received on the specified address.
 * Supported parameter types:
 * <ul>
 *   <li>{@link io.vertx.core.eventbus.Message Message&lt;T&gt;} - the full message object</li>
 *   <li>{@link String}, {@link Object}, etc. - the message body</li>
 * </ul>
 * If the method returns a non-void value, it will be automatically sent back as a reply.
 * <p>
 * Example:
 * <pre>{@code
 * @EventBusListener("order.created")
 * public void onOrderCreated(String body) {
 *     log.info("Received order: {}", body);
 * }
 *
 * @EventBusListener("order.query")
 * public String onOrderQuery(Message<String> message) {
 *     return "Order: " + message.body();
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EventBusListener {

    /**
     * The EventBus address to subscribe to.
     */
    String value();
}
