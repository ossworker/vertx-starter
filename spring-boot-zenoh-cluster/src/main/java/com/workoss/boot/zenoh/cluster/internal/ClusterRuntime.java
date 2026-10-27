package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.internal.membership.MembershipService;
import com.workoss.boot.zenoh.cluster.internal.messaging.MessagingChannel;
import com.workoss.boot.zenoh.cluster.internal.messaging.MessagingRegistry;
import com.workoss.boot.zenoh.cluster.internal.store.HybridClock;
import com.workoss.boot.zenoh.cluster.internal.store.LockService;
import com.workoss.boot.zenoh.cluster.internal.store.ReplicatedStore;
import com.workoss.boot.zenoh.cluster.internal.store.StoreReplicator;
import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscovery;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One run of a cluster node, from start to stop: a fresh incarnation with its own Zenoh session,
 * replica, membership view and threads. Discarded on stop; a restart builds a new one, so nothing
 * from a previous run (threads, locks, stale replica) leaks into the next.
 */
final class ClusterRuntime implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRuntime.class);
    private static final long SWEEP_INTERVAL_MILLIS = 5_000L;

    private final ClusterSettings settings;
    private final ZenohTransport transport;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService syncExecutor;
    private final ReplicatedStore store;
    private final StoreReplicator replicator;
    private final MessagingChannel messaging;
    private final MembershipService membership;
    private final LockService locks;
    private final DnsPeerDiscovery discovery;
    private final AtomicBoolean resyncPending = new AtomicBoolean(false);
    private volatile boolean started;
    private volatile boolean closed;

    private ClusterRuntime(ClusterSettings settings, MessagingRegistry registry, MembershipService.Events listener) {
        this.settings = settings;
        String nodeId = settings.nodeId();
        KeySpace keys = new KeySpace(settings.appName(), settings.environment());
        NodeInfo self = new NodeInfo(nodeId, UUID.randomUUID().toString(), System.currentTimeMillis(), hostName());
        this.transport = ZenohTransport.open(settings.transport(), nodeId);
        try {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(
                    r -> daemon(r, "zenoh-cluster-scheduler-" + nodeId));
            this.syncExecutor = Executors.newSingleThreadExecutor(r -> daemon(r, "zenoh-cluster-sync-" + nodeId));
            this.store = new ReplicatedStore(keys.storePrefix(), nodeId, new HybridClock(),
                    settings.tombstoneRetentionMillis());
            this.messaging = new MessagingChannel(transport, keys, nodeId, registry);
            this.membership = new MembershipService(transport, keys, self, settings.heartbeatMillis(),
                    settings.failureTimeoutMillis(), scheduler, new MembershipService.Events() {
                        @Override
                        public void joined(NodeInfo node, boolean rejoined) {
                            // Writes made while the peer was unreachable (startup race or a healed
                            // partition) never reached us through pub/sub.
                            requestResync();
                            listener.joined(node, rejoined);
                        }

                        @Override
                        public void left(NodeInfo node) {
                            listener.left(node);
                        }
                    });
            this.replicator = new StoreReplicator(transport, keys, store, membership::isPullSource);
            this.locks = new LockService(nodeId, self.incarnation(), membership::isLikelyAlive, store,
                    () -> !closed);
            this.discovery = new DnsPeerDiscovery(settings.dns(), settings.transport().connect(), endpoints -> {
                transport.updateConnectEndpoints(endpoints);
                requestResync();
            }, nodeId);
        } catch (RuntimeException e) {
            transport.close();
            throw e;
        }
    }

    static ClusterRuntime start(ClusterSettings settings, MessagingRegistry registry,
                                MembershipService.Events listener) {
        ClusterRuntime runtime = new ClusterRuntime(settings, registry, listener);
        try {
            runtime.doStart();
            return runtime;
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
    }

    private void doStart() {
        // Serve and listen before announcing ourselves, so peers that react to our join find us ready.
        replicator.start();
        messaging.start();
        membership.start();
        int merged = replicator.resync();
        scheduler.scheduleWithFixedDelay(this::sweep, SWEEP_INTERVAL_MILLIS, SWEEP_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
        if (settings.antiEntropyMillis() > 0) {
            scheduler.scheduleWithFixedDelay(this::requestResync, settings.antiEntropyMillis(),
                    settings.antiEntropyMillis(), TimeUnit.MILLISECONDS);
        }
        started = true;
        logger.info("Zenoh cluster node {} started (app={}, env={}, session={}, {} entries bootstrapped)",
                settings.nodeId(), settings.appName(), settings.environment(), transport.sessionId(), merged);
        discovery.start();
    }

    ReplicatedStore store() {
        return store;
    }

    MembershipService membership() {
        return membership;
    }

    MessagingChannel messaging() {
        return messaging;
    }

    LockService locks() {
        return locks;
    }

    String sessionId() {
        return transport.sessionId();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        started = false;
        discovery.close();
        scheduler.shutdownNow();
        syncExecutor.shutdownNow();
        try {
            // An in-flight heartbeat landing after our leave would briefly resurrect us on peers.
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closeQuietly(membership);
        closeQuietly(messaging);
        closeQuietly(replicator);
        closeQuietly(transport);
        logger.info("Zenoh cluster node {} stopped", settings.nodeId());
    }

    private void requestResync() {
        if (!started || !resyncPending.compareAndSet(false, true)) {
            return;
        }
        try {
            syncExecutor.execute(() -> {
                resyncPending.set(false);
                int merged = replicator.resync();
                if (merged > 0) {
                    logger.info("Resync merged {} entries from peers", merged);
                }
            });
        } catch (RejectedExecutionException e) {
            resyncPending.set(false);
        }
    }

    private void sweep() {
        try {
            store.sweep();
        } catch (Exception e) {
            logger.warn("Store sweep failed", e);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("Failed to close {}", closeable.getClass().getSimpleName(), e);
        }
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return null;
        }
    }

    private static Thread daemon(Runnable runnable, String name) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }
}
