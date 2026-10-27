package com.workoss.boot.zenoh.cluster.internal.messaging;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.MessageHandler;
import com.workoss.boot.zenoh.cluster.Subscription;
import com.workoss.boot.zenoh.cluster.ZenohSubscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.Order;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * Finds {@link ZenohSubscribe} methods on Spring beans and registers them with
 * {@link ClusterMessaging} once every singleton is built. Registrations are cancelled on context
 * close.
 */
public final class ZenohSubscribeProcessor
        implements BeanPostProcessor, SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(ZenohSubscribeProcessor.class);

    private final List<Candidate> candidates = new CopyOnWriteArrayList<>();
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> {
            ZenohSubscribe annotation = AnnotatedElementUtils.findMergedAnnotation(method, ZenohSubscribe.class);
            if (annotation != null) {
                candidates.add(new Candidate(beanName, bean, method, annotation));
            }
        }, ZenohSubscribeProcessor::isUserMethod);
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (candidates.isEmpty()) {
            return;
        }
        if (context.getBeanNamesForType(ClusterMessaging.class).length == 0) {
            logger.debug("@ZenohSubscribe handlers found but no ClusterMessaging bean; cluster is disabled");
            return;
        }
        ClusterMessaging messaging = context.getBean(ClusterMessaging.class);
        for (Candidate candidate : candidates) {
            register(messaging, candidate);
        }
        logger.info("Registered {} @ZenohSubscribe handler(s)", subscriptions.size());
    }

    @Override
    public void destroy() {
        subscriptions.forEach(Subscription::cancel);
        subscriptions.clear();
    }

    private void register(ClusterMessaging messaging, Candidate candidate) {
        Method method = candidate.method();
        ReflectionUtils.makeAccessible(method);
        Function<ClusterMessage, Object> adapter = adapterFor(method.getParameterTypes()[0]);
        MessageHandler handler = message -> invoke(messaging, candidate, message, adapter.apply(message));
        if (candidate.annotation().direct()) {
            subscriptions.add(subscribeDirect(messaging, candidate, handler));
            logger.debug("Bound {}.{} to direct messages", candidate.beanName(), method.getName());
            return;
        }
        String[] addresses = candidate.annotation().value();
        for (String address : addresses) {
            Integer order = orderOf(candidate);
            Subscription subscription = order == null
                    ? messaging.subscribe(address, handler)
                    : messaging.subscribe(address, handler, order);
            subscriptions.add(subscription);
            logger.debug("Bound {}.{} to broadcast '{}' (order={})", candidate.beanName(), method.getName(),
                    address, order);
        }
    }

    private static Subscription subscribeDirect(ClusterMessaging messaging, Candidate candidate,
                                                MessageHandler handler) {
        Integer order = orderOf(candidate);
        return order == null ? messaging.onDirectMessage(handler) : messaging.onDirectMessage(handler, order);
    }

    private void invoke(ClusterMessaging messaging, Candidate candidate, ClusterMessage message, Object argument) {
        Object result;
        try {
            result = candidate.method().invoke(candidate.bean(), argument);
        } catch (InvocationTargetException e) {
            logger.warn("@ZenohSubscribe handler {}.{} failed", candidate.beanName(),
                    candidate.method().getName(), e.getCause());
            return;
        } catch (IllegalAccessException e) {
            logger.warn("Cannot invoke @ZenohSubscribe handler {}.{}", candidate.beanName(),
                    candidate.method().getName(), e);
            return;
        }
        String replyTo = candidate.annotation().replyTo();
        boolean replies = candidate.annotation().direct() || !replyTo.isEmpty();
        if (!replies || result == null) {
            return;
        }
        if (result instanceof CompletableFuture<?> future) {
            // Never block the handler thread on the result: publish when the future completes.
            future.whenComplete((value, error) -> {
                if (error != null) {
                    logger.warn("@ZenohSubscribe handler {}.{} async result failed", candidate.beanName(),
                            candidate.method().getName(), error);
                } else if (value != null) {
                    publishReply(messaging, candidate, message, replyTo, value);
                }
            });
            return;
        }
        publishReply(messaging, candidate, message, replyTo, result);
    }

    private static void publishReply(ClusterMessaging messaging, Candidate candidate, ClusterMessage message,
                                     String replyTo, Object value) {
        byte[] payload;
        try {
            payload = encodeReply(value);
        } catch (IllegalArgumentException e) {
            logger.warn("Cannot publish reply of {}.{} to '{}': {}", candidate.beanName(),
                    candidate.method().getName(), replyTo, e.getMessage());
            return;
        }
        try {
            if (candidate.annotation().direct()) {
                messaging.send(message.sourceNodeId(), payload);
            } else {
                messaging.broadcast(replyTo, payload);
            }
        } catch (Exception e) {
            logger.warn("Cannot publish reply of {}.{} to '{}'", candidate.beanName(),
                    candidate.method().getName(), replyTo, e);
        }
    }

    private static byte[] encodeReply(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("unsupported reply type " + value.getClass().getName()
                + "; expected String or byte[]");
    }

    /**
     * @return the {@code @Order} of the method, or of its bean class, or {@code null} when neither
     *         declares one — which keeps the handler unordered and therefore parallel
     */
    private static Integer orderOf(Candidate candidate) {
        Order order = AnnotatedElementUtils.findMergedAnnotation(candidate.method(), Order.class);
        if (order == null) {
            order = AnnotatedElementUtils.findMergedAnnotation(
                    ClassUtils.getUserClass(candidate.bean()), Order.class);
        }
        return order == null ? null : order.value();
    }

    private static Function<ClusterMessage, Object> adapterFor(Class<?> parameterType) {
        if (ClusterMessage.class.equals(parameterType)) {
            return message -> message;
        }
        if (byte[].class.equals(parameterType)) {
            return ClusterMessage::payload;
        }
        if (String.class.equals(parameterType)) {
            return ClusterMessage::payloadAsString;
        }
        throw new IllegalStateException("Unsupported @ZenohSubscribe parameter type " + parameterType.getName()
                + "; expected ClusterMessage, byte[] or String");
    }

    private static boolean isUserMethod(Method method) {
        return !method.isSynthetic() && !method.isBridge() && !Modifier.isStatic(method.getModifiers());
    }

    private record Candidate(String beanName, Object bean, Method method, ZenohSubscribe annotation) {

        private Candidate {
            if (Modifier.isStatic(method.getModifiers())) {
                throw new IllegalStateException("@ZenohSubscribe method must not be static: " + method);
            }
            if (method.getParameterCount() != 1) {
                throw new IllegalStateException("@ZenohSubscribe method must take exactly one parameter "
                        + "(ClusterMessage, byte[] or String): " + method);
            }
            Class<?> parameterType = method.getParameterTypes()[0];
            if (!ClusterMessage.class.equals(parameterType) && !byte[].class.equals(parameterType)
                    && !String.class.equals(parameterType)) {
                throw new IllegalStateException("@ZenohSubscribe parameter must be ClusterMessage, byte[] "
                        + "or String: " + method);
            }
            if (!annotation.direct() && annotation.value().length == 0) {
                throw new IllegalStateException("@ZenohSubscribe needs at least one address, or direct=true: "
                        + method);
            }
            if (!annotation.replyTo().isEmpty() || annotation.direct()) {
                Class<?> returnType = method.getReturnType();
                if (!void.class.equals(returnType) && !String.class.equals(returnType)
                        && !byte[].class.equals(returnType) && !CompletableFuture.class.equals(returnType)) {
                    throw new IllegalStateException("@ZenohSubscribe with replyTo or direct=true requires a "
                            + "return type of void, String, byte[] or CompletableFuture: " + method);
                }
            }
        }
    }
}
