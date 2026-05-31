package com.workoss.boot.vertx.autoconfigure;

import com.workoss.boot.vertx.autoconfigure.annotation.EventBusListener;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

/**
 * Scans all Spring beans for {@link EventBusListener} annotated methods
 * and registers them as Vert.x EventBus consumers.
 * <p>
 * This runs after all singleton beans are initialized, ensuring that
 * the target beans and the Vert.x instance are both available.
 */
public class EventBusListenerRegistry implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(EventBusListenerRegistry.class);

    private final Vertx vertx;
    private ApplicationContext applicationContext;
    private final List<MessageConsumer<?>> consumers = new ArrayList<>();

    public EventBusListenerRegistry(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (applicationContext == null) {
            logger.warn("ApplicationContext not available, skipping EventBus listener registration");
            return;
        }

        String[] beanNames = applicationContext.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            try {
                Object bean = applicationContext.getBean(beanName);
                registerListenersForBean(bean);
            } catch (Exception e) {
                logger.debug("Skipping bean '{}' during EventBus listener scan: {}", beanName, e.getMessage());
            }
        }

        if (!consumers.isEmpty()) {
            logger.info("Registered {} EventBus listener(s)", consumers.size());
        }
    }

    private void registerListenersForBean(Object bean) {
        Class<?> beanClass = org.springframework.util.ClassUtils.getUserClass(bean);
        ReflectionUtils.doWithMethods(beanClass, method -> {
            EventBusListener annotation = AnnotatedElementUtils.findMergedAnnotation(method, EventBusListener.class);
            if (annotation != null) {
                String address = annotation.value();
                registerConsumer(address, bean, method);
            }
        }, ReflectionUtils.USER_DECLARED_METHODS);
    }

    private void registerConsumer(String address, Object bean, Method method) {
        Handler<Message<Object>> handler = msg -> invokeListenerMethod(bean, method, msg);

        MessageConsumer<Object> consumer = vertx.eventBus().consumer(address, handler);
        consumers.add(consumer);
        logger.info("Registered EventBus listener on address='{}' for method {}.{}()",
                address, bean.getClass().getSimpleName(), method.getName());
    }

    @SuppressWarnings("unchecked")
    private void invokeListenerMethod(Object bean, Method method, Message<Object> message) {
        try {
            Object result = doInvokeMethod(bean, method, message);
            // If method returns a value, send it back as a reply
            if (result != null && method.getReturnType() != void.class) {
                message.reply(result);
            }
        } catch (Exception e) {
            logger.error("Error invoking EventBus listener method {}.{}() for address='{}'",
                    bean.getClass().getSimpleName(), method.getName(), message.address(), e);
        }
    }

    private Object doInvokeMethod(Object bean, Method method, Message<Object> message)
            throws InvocationTargetException, IllegalAccessException {

        Parameter[] params = method.getParameters();
        if (params.length == 0) {
            return method.invoke(bean);
        }

        if (params.length == 1) {
            Class<?> paramType = params[0].getType();
            // Check if parameter is Message
            if (io.vertx.core.eventbus.Message.class.isAssignableFrom(paramType)) {
                return method.invoke(bean, message);
            }
            // Otherwise pass the body
            Object body = message.body();
            if (body != null && !paramType.isInstance(body)) {
                // Try to cast or convert
                logger.warn("Message body type {} does not match parameter type {}, using body as-is",
                        body.getClass().getName(), paramType.getName());
            }
            return method.invoke(bean, body);
        }

        // Multiple parameters not supported
        throw new IllegalArgumentException(
                "EventBusListener method must have at most 1 parameter, found " + params.length);
    }
}
