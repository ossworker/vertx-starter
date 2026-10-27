package com.workoss.boot.zenoh.cluster.internal.store;

import com.workoss.boot.zenoh.cluster.ClusterLock;
import com.workoss.boot.zenoh.cluster.internal.KeySpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;

/**
 * Each lock is the single {@link KeySpace#LOCK_ENTRY} entry of its own {@code locks/{name}}
 * namespace; the entry value identifies the holder as
 * {@code nodeId \n incarnation \n token}. A lock counts as free when it is absent or when its
 * holder's node incarnation is no longer alive, which is how locks of crashed nodes are reclaimed
 * without a lease.
 * <p>
 * Bound to one run of the node: once that run ends ({@code active} turns false) its locks report
 * not held and release becomes a no-op, since the new incarnation no longer owns them.
 */
public final class LockService {

    /**
     * Liveness oracle {@code (nodeId, incarnation) -> alive}.
     */
    private final BiPredicate<String, String> holderAlive;
    private final String nodeId;
    private final String incarnation;
    private final ReplicatedStore store;
    private final BooleanSupplier active;

    public LockService(String nodeId, String incarnation, BiPredicate<String, String> holderAlive,
                       ReplicatedStore store, BooleanSupplier active) {
        this.nodeId = nodeId;
        this.incarnation = incarnation;
        this.holderAlive = holderAlive;
        this.store = store;
        this.active = active;
    }

    public Optional<ClusterLock> tryAcquire(String name, Duration timeout) {
        String namespace = KeySpace.lockNamespace(name);
        byte[] holder = (nodeId + "\n" + incarnation + "\n" + UUID.randomUUID())
                .getBytes(StandardCharsets.UTF_8);
        long timeoutMillis = timeout == null ? 0L : Math.max(0L, timeout.toMillis());
        long deadline = System.nanoTime() + Duration.ofMillis(timeoutMillis).toNanos();
        long backoff = Math.clamp(timeoutMillis / 10, 10L, 100L);
        while (active.getAsBoolean()) {
            boolean acquired = store.update(namespace, KeySpace.LOCK_ENTRY,
                    current -> isFree(current) ? Change.put(holder) : Change.KEEP).changed();
            if (acquired) {
                return Optional.of(new HeldLock(name, namespace, holder));
            }
            if (System.nanoTime() - deadline >= 0) {
                return Optional.empty();
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private boolean isFree(byte[] current) {
        if (current == null) {
            return true;
        }
        String[] parts = new String(current, StandardCharsets.UTF_8).split("\n", -1);
        return parts.length != 3 || !holderAlive.test(parts[0], parts[1]);
    }

    private final class HeldLock implements ClusterLock {

        private final String name;
        private final String namespace;
        private final byte[] holder;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private HeldLock(String name, String namespace, byte[] holder) {
            this.name = name;
            this.namespace = namespace;
            this.holder = holder;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isHeld() {
            return !released.get() && active.getAsBoolean()
                    && Arrays.equals(store.get(namespace, KeySpace.LOCK_ENTRY), holder);
        }

        @Override
        public void release() {
            if (!released.compareAndSet(false, true) || !active.getAsBoolean()) {
                return;
            }
            // Conditional: a lock already taken over by someone else must not be released by us.
            store.update(namespace, KeySpace.LOCK_ENTRY,
                    value -> Arrays.equals(value, holder) ? Change.DELETE : Change.KEEP);
        }
    }
}
