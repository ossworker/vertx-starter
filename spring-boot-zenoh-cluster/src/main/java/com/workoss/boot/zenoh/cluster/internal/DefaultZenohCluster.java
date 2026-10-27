package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.ClusterCounter;
import com.workoss.boot.zenoh.cluster.ClusterLock;
import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.MembershipListener;
import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.internal.membership.MembershipService;
import com.workoss.boot.zenoh.cluster.internal.messaging.DefaultClusterMessaging;
import com.workoss.boot.zenoh.cluster.internal.messaging.MessagingRegistry;
import com.workoss.boot.zenoh.cluster.internal.store.DefaultClusterCounter;
import com.workoss.boot.zenoh.cluster.internal.store.DefaultSharedMap;
import com.workoss.boot.zenoh.cluster.internal.store.ReplicatedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * {@link ZenohCluster} bound to the Spring lifecycle. Restartable: every start builds a fresh
 * {@link ClusterRuntime} while handles, listeners and subscriptions live here and carry over.
 */
public final class DefaultZenohCluster implements ZenohCluster, SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(DefaultZenohCluster.class);

    private final ClusterSettings settings;
    private final MessagingRegistry registry = new MessagingRegistry();
    private final ClusterMessaging messaging;
    private final List<OrderedListener> listeners = new CopyOnWriteArrayList<>();
    private final SerialExecutor listenerExecutor = new SerialExecutor("zenoh-cluster-membership-events");
    private final ConcurrentHashMap<String, DefaultSharedMap<?, ?>> maps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ClusterCounter> counters = new ConcurrentHashMap<>();
    private volatile ClusterRuntime runtime;

    public DefaultZenohCluster(ClusterSettings settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.messaging = new DefaultClusterMessaging(registry, () -> {
            ClusterRuntime current = runtime;
            return current == null ? null : current.messaging();
        });
    }

    // --- Lifecycle ---

    @Override
    public synchronized void start() {
        if (runtime != null) {
            return;
        }
        runtime = ClusterRuntime.start(settings, registry, new MembershipService.Events() {
            @Override
            public void joined(NodeInfo node, boolean rejoined) {
                notifyListeners(listener -> listener.nodeJoined(node));
            }

            @Override
            public void left(NodeInfo node) {
                notifyListeners(listener -> listener.nodeLeft(node));
            }
        });
    }

    @Override
    public synchronized void stop() {
        ClusterRuntime current = runtime;
        if (current == null) {
            return;
        }
        runtime = null;
        current.close();
    }

    @Override
    public boolean isRunning() {
        return runtime != null;
    }

    /**
     * Starts early and stops late so that beans depending on the cluster find it running.
     */
    @Override
    public int getPhase() {
        return DEFAULT_PHASE - 2048;
    }

    // --- ZenohCluster ---

    @Override
    public String nodeId() {
        return settings.nodeId();
    }

    @Override
    public boolean isActive() {
        return runtime != null;
    }

    @Override
    public List<NodeInfo> members() {
        ClusterRuntime current = runtime;
        return current == null ? List.of() : current.membership().members();
    }

    @Override
    public Optional<NodeInfo> member(String nodeId) {
        ClusterRuntime current = runtime;
        return current == null ? Optional.empty() : current.membership().member(nodeId);
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {
        listeners.add(new OrderedListener(Ordered.LOWEST_PRECEDENCE,
                Objects.requireNonNull(listener, "listener")));
    }

    @Override
    public void addMembershipListener(MembershipListener listener, int order) {
        listeners.add(new OrderedListener(order, Objects.requireNonNull(listener, "listener")));
    }

    @Override
    public void removeMembershipListener(MembershipListener listener) {
        listeners.removeIf(ordered -> ordered.listener() == listener);
    }

    @Override
    public <K, V> SharedMap<K, V> sharedMap(String name) {
        return sharedMap(name, ValueCodec.jdk());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> SharedMap<K, V> sharedMap(String name, ValueCodec<V> valueCodec) {
        Objects.requireNonNull(valueCodec, "valueCodec");
        DefaultSharedMap<?, ?> map = maps.computeIfAbsent(requireName(name),
                n -> new DefaultSharedMap<>(n, this::requireStore, valueCodec));
        if (map.valueCodec() != valueCodec) {
            throw new IllegalArgumentException("Shared map '" + name + "' is already bound to another codec");
        }
        return (SharedMap<K, V>) map;
    }

    @Override
    public ClusterCounter counter(String name) {
        return counters.computeIfAbsent(requireName(name),
                n -> new DefaultClusterCounter(n, settings.nodeId(), this::requireStore));
    }

    @Override
    public Optional<ClusterLock> tryLock(String name) {
        return tryLock(name, Duration.ZERO);
    }

    @Override
    public Optional<ClusterLock> tryLock(String name, Duration timeout) {
        return requireRuntime().locks().tryAcquire(requireName(name), timeout);
    }

    @Override
    public ClusterMessaging messaging() {
        return messaging;
    }

    @Override
    public Optional<String> sessionId() {
        ClusterRuntime current = runtime;
        return current == null ? Optional.empty() : Optional.of(current.sessionId());
    }

    // --- Internals ---

    ReplicatedStore requireStore() {
        return requireRuntime().store();
    }

    private ClusterRuntime requireRuntime() {
        ClusterRuntime current = runtime;
        if (current == null) {
            throw new IllegalStateException("Zenoh cluster is not running");
        }
        return current;
    }

    private void notifyListeners(Consumer<MembershipListener> event) {
        listenerExecutor.execute(() -> {
            // Sorted per event so equal orders keep registration order and a late removal cannot
            // leave a gap; membership events are rare enough that the copy is irrelevant.
            List<MembershipListener> ordered = listeners.stream()
                    .sorted(Comparator.comparingInt(OrderedListener::order))
                    .map(OrderedListener::listener)
                    .toList();
            for (MembershipListener listener : ordered) {
                try {
                    event.accept(listener);
                } catch (Exception e) {
                    logger.warn("Membership listener failed", e);
                }
            }
        });
    }

    private record OrderedListener(int order, MembershipListener listener) {
    }

    private static String requireName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
        return name;
    }
}
