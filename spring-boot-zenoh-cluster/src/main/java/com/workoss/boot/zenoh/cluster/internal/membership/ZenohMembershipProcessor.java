package com.workoss.boot.zenoh.cluster.internal.membership;

import com.workoss.boot.zenoh.cluster.MembershipListener;
import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.ZenohMembership;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Finds {@link ZenohMembership} methods on Spring beans and registers them with
 * {@link ZenohCluster#addMembershipListener} once every singleton is built. Registrations are
 * removed on context close.
 */
public final class ZenohMembershipProcessor
        implements BeanPostProcessor, SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(ZenohMembershipProcessor.class);

    private final List<Candidate> candidates = new CopyOnWriteArrayList<>();
    private final List<MembershipListener> listeners = new CopyOnWriteArrayList<>();
    private ApplicationContext context;
    private volatile ZenohCluster cluster;

    @Override
    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> {
            ZenohMembership annotation = AnnotatedElementUtils.findMergedAnnotation(method, ZenohMembership.class);
            if (annotation != null) {
                candidates.add(new Candidate(beanName, bean, method, annotation));
            }
        }, ZenohMembershipProcessor::isUserMethod);
        return bean;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (candidates.isEmpty()) {
            return;
        }
        if (context.getBeanNamesForType(ZenohCluster.class).length == 0) {
            logger.debug("@ZenohMembership handlers found but no ZenohCluster bean; cluster is disabled");
            return;
        }
        ZenohCluster cluster = context.getBean(ZenohCluster.class);
        this.cluster = cluster;
        for (Candidate candidate : candidates) {
            ReflectionUtils.makeAccessible(candidate.method());
            MembershipListener listener = listenerFor(candidate);
            listeners.add(listener);
            Integer order = orderOf(candidate);
            if (order == null) {
                cluster.addMembershipListener(listener);
            } else {
                cluster.addMembershipListener(listener, order);
            }
            logger.debug("Bound {}.{} to membership events {} (order={})", candidate.beanName(),
                    candidate.method().getName(), candidate.annotation().value(), order);
        }
        logger.info("Registered {} @ZenohMembership handler(s)", listeners.size());
    }

    @Override
    public void destroy() {
        ZenohCluster current = cluster;
        if (current != null) {
            listeners.forEach(current::removeMembershipListener);
        }
        listeners.clear();
    }

    private static MembershipListener listenerFor(Candidate candidate) {
        Set<ZenohMembership.Event> events = EnumSet.copyOf(List.of(candidate.annotation().value()));
        return new MembershipListener() {
            @Override
            public void nodeJoined(NodeInfo node) {
                if (events.contains(ZenohMembership.Event.NODE_JOINED)) {
                    invoke(candidate, node);
                }
            }

            @Override
            public void nodeLeft(NodeInfo node) {
                if (events.contains(ZenohMembership.Event.NODE_LEFT)) {
                    invoke(candidate, node);
                }
            }
        };
    }

    private static void invoke(Candidate candidate, NodeInfo node) {
        try {
            candidate.method().invoke(candidate.bean(), node);
        } catch (InvocationTargetException e) {
            logger.warn("@ZenohMembership handler {}.{} failed", candidate.beanName(),
                    candidate.method().getName(), e.getCause());
        } catch (IllegalAccessException e) {
            logger.warn("Cannot invoke @ZenohMembership handler {}.{}", candidate.beanName(),
                    candidate.method().getName(), e);
        }
    }

    /**
     * @return the {@code @Order} of the method, or of its bean class, or {@code null} when neither
     *         declares one
     */
    private static Integer orderOf(Candidate candidate) {
        Order order = AnnotatedElementUtils.findMergedAnnotation(candidate.method(), Order.class);
        if (order == null) {
            order = AnnotatedElementUtils.findMergedAnnotation(
                    ClassUtils.getUserClass(candidate.bean()), Order.class);
        }
        return order == null ? null : order.value();
    }

    private static boolean isUserMethod(Method method) {
        return !method.isSynthetic() && !method.isBridge() && !Modifier.isStatic(method.getModifiers());
    }

    private record Candidate(String beanName, Object bean, Method method, ZenohMembership annotation) {

        private Candidate {
            if (Modifier.isStatic(method.getModifiers())) {
                throw new IllegalStateException("@ZenohMembership method must not be static: " + method);
            }
            if (method.getParameterCount() != 1 || !NodeInfo.class.equals(method.getParameterTypes()[0])) {
                throw new IllegalStateException("@ZenohMembership method must take exactly one NodeInfo "
                        + "parameter: " + method);
            }
            if (!void.class.equals(method.getReturnType())) {
                throw new IllegalStateException("@ZenohMembership method must return void: " + method);
            }
            if (annotation.value().length == 0) {
                throw new IllegalStateException("@ZenohMembership needs at least one event: " + method);
            }
        }
    }
}
