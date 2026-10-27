package com.workoss.boot.zenoh.cluster.internal.messaging;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.MessageHandler;
import com.workoss.boot.zenoh.cluster.Subscription;
import com.workoss.boot.zenoh.cluster.internal.SerialExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Local handler registrations. Outlives cluster runs so subscriptions survive restarts.
 * <p>
 * Every registration is dispatched on its own {@link SerialExecutor}, which keeps each handler's
 * messages in arrival order while isolating handlers from each other. A registration that declares
 * an explicit {@link Ordered order} is different: the ordered handlers of one address share a
 * single serial executor and are therefore invoked one at a time, in ascending order, so a lower
 * order can never be overtaken by a higher one. Handlers without an order keep their own executor
 * and never wait for another handler.
 * <p>
 * A subscription address may be a wildcard pattern ({@code news/*}, {@code news/**}); see
 * {@link AddressMatcher}. Literal addresses are indexed in a map for O(1) dispatch, while patterns
 * are kept in a list and tested against each incoming address. Both lists are kept sorted by order,
 * with equal orders in registration order.
 */
public final class MessagingRegistry {

    private static final Logger logger = LoggerFactory.getLogger(MessagingRegistry.class);

    /** Group key of the direct-message handlers, which have no address. */
    private static final String DIRECT_GROUP = "direct";

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Registration>> byAddress = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Registration> patterns = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Registration> direct = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, SerialExecutor> orderedExecutors = new ConcurrentHashMap<>();

    public Subscription subscribe(String address, MessageHandler handler) {
        return subscribe(address, handler, null);
    }

    /**
     * @param order position of this handler among the handlers of {@code address}; {@code null}
     *              keeps the handler on its own executor, any value moves it onto the shared
     *              executor of the address where handlers run one at a time in order
     */
    public Subscription subscribe(String address, MessageHandler handler, Integer order) {
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("address must not be empty");
        }
        Objects.requireNonNull(handler, "handler");
        Registration registration = new Registration(address, handler, order, executorFor(address, order));
        if (AddressMatcher.isPattern(address)) {
            insertOrdered(patterns, registration);
            registration.owner = () -> patterns.remove(registration);
        } else {
            byAddress.compute(address, (key, list) -> {
                CopyOnWriteArrayList<Registration> target = list == null ? new CopyOnWriteArrayList<>() : list;
                insertOrdered(target, registration);
                return target;
            });
            registration.owner = () -> byAddress.computeIfPresent(address, (key, list) -> {
                list.remove(registration);
                return list.isEmpty() ? null : list;
            });
        }
        return registration;
    }

    public Subscription onDirectMessage(MessageHandler handler) {
        return onDirectMessage(handler, null);
    }

    /**
     * @param order as in {@link #subscribe(String, MessageHandler, Integer)}, for the direct
     *              handlers as a group
     */
    public Subscription onDirectMessage(MessageHandler handler, Integer order) {
        Objects.requireNonNull(handler, "handler");
        Registration registration = new Registration(null, handler, order, executorFor(DIRECT_GROUP, order));
        insertOrdered(direct, registration);
        registration.owner = () -> direct.remove(registration);
        return registration;
    }

    public void dispatchBroadcast(ClusterMessage message) {
        String address = message.address();
        List<Registration> exact = byAddress.get(address);
        if (exact != null) {
            exact.forEach(registration -> registration.deliver(message));
        }
        for (Registration registration : patterns) {
            if (AddressMatcher.matches(registration.address, address)) {
                registration.deliver(message);
            }
        }
    }

    public void dispatchDirect(ClusterMessage message) {
        direct.forEach(registration -> registration.deliver(message));
    }

    public Set<String> addresses() {
        Set<String> result = new HashSet<>(byAddress.keySet());
        patterns.forEach(registration -> result.add(registration.address));
        return Set.copyOf(result);
    }

    private SerialExecutor executorFor(String groupKey, Integer order) {
        if (order == null) {
            return new SerialExecutor("zenoh-cluster-handler-" + groupKey);
        }
        return orderedExecutors.computeIfAbsent(groupKey,
                key -> new SerialExecutor("zenoh-cluster-ordered-" + key));
    }

    private static void insertOrdered(CopyOnWriteArrayList<Registration> list, Registration registration) {
        int key = orderKey(registration);
        int index = list.size();
        while (index > 0 && orderKey(list.get(index - 1)) > key) {
            index--;
        }
        list.add(index, registration);
    }

    private static int orderKey(Registration registration) {
        Integer order = registration.order;
        return order == null ? Ordered.LOWEST_PRECEDENCE : order;
    }

    private static final class Registration implements Subscription {

        private final String address;
        private final MessageHandler handler;
        private final Integer order;
        private final SerialExecutor executor;
        private volatile Runnable owner;
        private volatile boolean cancelled;

        private Registration(String address, MessageHandler handler, Integer order, SerialExecutor executor) {
            this.address = address;
            this.handler = handler;
            this.order = order;
            this.executor = executor;
        }

        private void deliver(ClusterMessage message) {
            executor.execute(() -> {
                if (cancelled) {
                    return;
                }
                try {
                    handler.onMessage(message);
                } catch (Exception e) {
                    logger.warn("Message handler failed", e);
                }
            });
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;
            Runnable remove = owner;
            if (remove != null) {
                remove.run();
            }
        }
    }
}
