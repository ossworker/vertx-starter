package com.workoss.boot.vertx.contactcenter.uctp;

import com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator.Window;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Leases a Snowflake node number (0..1023) through the Vert.x cluster-wide {@link AsyncMap}, so the
 * backing cluster manager (Zenoh, Hazelcast, ...) is interchangeable and also isolates projects and
 * environments. Records are {@code owner,from,until}; a slot is reused only after its lease plus a
 * grace period expired, and the new range starts after the previous one, so a lapsed owner's IDs can
 * never be reissued. Because some cluster maps are only eventually consistent, every conditional write
 * is re-read after a settle delay before IDs are issued, and the lease is verified continuously: if the
 * record is overwritten the generator is revoked and a new node is leased.
 */
public final class ClusterSnowflakeService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterSnowflakeService.class);
    static final String MAP_NAME = "uctp-snowflake-nodes";
    static final int NODES = 1024;

    record Timing(long leaseMillis, long renewMillis, long verifyMillis, long settleMillis, long graceMillis,
                  long acquireTimeoutMillis, long backoffMinMillis, long backoffMaxMillis) {
        static final Timing DEFAULT = new Timing(60_000, 20_000, 1_000, 3_000, 5_000, 60_000, 200, 1_000);

        Timing {
            if (leaseMillis <= 0 || renewMillis <= 0 || verifyMillis <= 0 || settleMillis < 0 || graceMillis < 0
                    || acquireTimeoutMillis <= 0 || backoffMinMillis <= 0 || backoffMaxMillis < backoffMinMillis) {
                throw new IllegalArgumentException("Snowflake lease timings must be positive and ordered");
            }
            if (renewMillis + settleMillis >= leaseMillis || verifyMillis >= leaseMillis) {
                throw new IllegalArgumentException("Snowflake lease must outlive settle plus one renewal interval");
            }
        }
    }

    private record Lease(Window window, String value) {
        String key() {
            return String.valueOf(window.nodeId());
        }
    }

    private final Vertx vertx;
    private final int minMembers;
    private final Timing timing;
    private final LongSupplier clock;
    private final String owner = UUID.randomUUID().toString();
    private volatile AsyncMap<String, String> map;
    private volatile String lastObstacle = "none";
    private Future<SnowflakeGenerator> starting;
    private SnowflakeGenerator generator;
    private Lease lease;
    private long verifyTimer = -1;
    private long renewTimer = -1;
    private boolean closed;

    public ClusterSnowflakeService(Vertx vertx, int minMembers) {
        this(vertx, minMembers, Timing.DEFAULT, System::currentTimeMillis);
    }

    ClusterSnowflakeService(Vertx vertx, int minMembers, Timing timing, LongSupplier clock) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        if (minMembers < 1) {
            throw new IllegalArgumentException("minMembers must be at least 1");
        }
        this.minMembers = minMembers;
        this.timing = Objects.requireNonNull(timing, "timing");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public synchronized Future<SnowflakeGenerator> start() {
        if (closed) {
            return Future.failedFuture(new IllegalStateException("Snowflake service is closed"));
        }
        if (starting != null) {
            return starting;
        }
        starting = vertx.sharedData().<String, String>getAsyncMap(MAP_NAME)
                .compose(loaded -> {
                    map = loaded;
                    return acquire(deadline());
                })
                .compose(this::activate)
                .recover(failure -> stop().transform(ignored -> Future.<SnowflakeGenerator>failedFuture(failure)));
        return starting;
    }

    private Future<Lease> acquire(long deadline) {
        synchronized (this) {
            if (closed) {
                return Future.failedFuture(new IllegalStateException("Snowflake service closed during allocation"));
            }
        }
        if (clock.getAsLong() > deadline) {
            return Future.failedFuture(new IllegalStateException("No Snowflake node leased within "
                    + timing.acquireTimeoutMillis() + "ms: " + lastObstacle));
        }
        int members = members();
        if (members < minMembers) {
            lastObstacle = "cluster members " + members + " < required " + minMembers;
            return retryLater(deadline);
        }
        return map.entries()
                .compose(this::claim)
                .compose(this::settle)
                .recover(failure -> {
                    lastObstacle = "cluster map failure: " + failure;
                    return Future.succeededFuture(null);
                })
                .compose(confirmed -> confirmed != null ? Future.succeededFuture(confirmed) : retryLater(deadline));
    }

    private Future<Lease> retryLater(long deadline) {
        Promise<Lease> retried = Promise.promise();
        vertx.setTimer(backoff(), ignored -> acquire(deadline).onComplete(retried));
        return retried.future();
    }

    private Future<Lease> claim(Map<String, String> entries) {
        long now = clock.getAsLong();
        List<Integer> free = new ArrayList<>();
        for (int node = 0; node < NODES; node++) {
            String raw = entries.get(String.valueOf(node));
            if (raw == null) {
                free.add(node);
                continue;
            }
            Window prior = decode(node, raw);
            // Malformed records are never overwritten: their owner and range are unknown.
            if (prior != null && prior.until() + timing.graceMillis() < now) {
                free.add(node);
            }
        }
        if (free.isEmpty()) {
            lastObstacle = "free=0 of " + NODES + " nodes";
            return Future.succeededFuture(null);
        }
        int node = free.get(ThreadLocalRandom.current().nextInt(free.size()));
        String previous = entries.get(String.valueOf(node));
        Window prior = previous == null ? null : decode(node, previous);
        long from = prior == null ? now : Math.max(now, prior.until() + 1);
        Window window = new Window(node, owner, from, from + timing.leaseMillis());
        Lease candidate = new Lease(window, encode(window));
        Future<Boolean> written = previous == null
                ? map.putIfAbsent(candidate.key(), candidate.value()).map(Objects::isNull)
                : map.replaceIfPresent(candidate.key(), previous, candidate.value());
        return written.map(won -> {
            if (won) {
                return candidate;
            }
            lastObstacle = "lost conditional write on node " + node;
            return null;
        });
    }

    private Future<Lease> settle(Lease candidate) {
        if (candidate == null) {
            return Future.succeededFuture(null);
        }
        Promise<Lease> settled = Promise.promise();
        vertx.setTimer(Math.max(1, timing.settleMillis()), ignored -> map.get(candidate.key())
                .map(observed -> {
                    if (candidate.value().equals(observed)) {
                        return candidate;
                    }
                    lastObstacle = "node " + candidate.window().nodeId() + " taken by a concurrent writer";
                    return (Lease) null;
                })
                .onComplete(settled));
        return settled.future();
    }

    private synchronized Future<SnowflakeGenerator> activate(Lease confirmed) {
        if (closed) {
            release(confirmed);
            return Future.failedFuture(new IllegalStateException("Snowflake service closed during allocation"));
        }
        lease = confirmed;
        if (generator == null) {
            generator = new SnowflakeGenerator(confirmed.window(), clock);
        } else {
            generator.reassign(confirmed.window());
        }
        verifyTimer = vertx.setPeriodic(timing.verifyMillis(), ignored -> verify());
        renewTimer = vertx.setPeriodic(timing.renewMillis(), ignored -> renew());
        Window window = confirmed.window();
        log.info("Snowflake node {} leased (datacenter {}, worker {}) by {} until {}",
                window.nodeId(), window.datacenterId(), window.workerId(), owner, window.until());
        return Future.succeededFuture(generator);
    }

    private void verify() {
        Lease current;
        synchronized (this) {
            if (closed || lease == null) {
                return;
            }
            current = lease;
        }
        map.get(current.key())
                .onSuccess(observed -> {
                    if (!current.value().equals(observed) && !renewedFrom(current, observed)) {
                        lost(current, "record overwritten: " + observed);
                    }
                })
                .onFailure(failure -> log.warn("Snowflake lease verification read failed", failure));
    }

    private boolean renewedFrom(Lease current, String observed) {
        Window window = decode(current.window().nodeId(), observed);
        return window != null && window.owner().equals(owner) && window.from() == current.window().from()
                && window.until() >= current.window().until();
    }

    private void renew() {
        Lease current;
        synchronized (this) {
            if (closed || lease == null) {
                return;
            }
            current = lease;
        }
        Window window = current.window();
        long now = clock.getAsLong();
        if (now > window.until()) {
            lost(current, "lease lapsed before renewal");
            return;
        }
        Window extended = new Window(window.nodeId(), owner, window.from(),
                Math.max(window.until(), now + timing.leaseMillis()));
        Lease next = new Lease(extended, encode(extended));
        map.replaceIfPresent(current.key(), current.value(), next.value())
                .onSuccess(replaced -> {
                    if (replaced) {
                        applyRenewal(current, next);
                    } else {
                        lost(current, "renewal rejected");
                    }
                })
                .onFailure(failure -> log.warn("Snowflake lease renewal failed; retaining the authorized window",
                        failure));
    }

    private synchronized void applyRenewal(Lease current, Lease next) {
        if (closed) {
            release(next);
            return;
        }
        if (lease != current) {
            return;
        }
        lease = next;
        generator.extend(next.window());
    }

    private void lost(Lease current, String reason) {
        synchronized (this) {
            if (closed || lease != current) {
                return;
            }
            generator.revoke();
            lease = null;
            cancelTimers();
        }
        log.warn("Snowflake node {} lease lost ({}); IDs suspended until a node is leased again",
                current.window().nodeId(), reason);
        reacquire();
    }

    private void reacquire() {
        acquire(deadline()).compose(this::activate).onFailure(failure -> {
            synchronized (this) {
                if (closed) {
                    return;
                }
            }
            log.error("Snowflake node re-lease failed; retrying", failure);
            vertx.setTimer(backoff(), ignored -> reacquire());
        });
    }

    @Override
    public void close() {
        stop();
    }

    private synchronized Future<Void> stop() {
        closed = true;
        cancelTimers();
        if (generator != null) {
            generator.revoke();
        }
        Lease held = lease;
        lease = null;
        return held != null ? release(held) : Future.succeededFuture();
    }

    private Future<Void> release(Lease held) {
        Window window = held.window();
        long last = generator != null ? generator.lastTimestamp() : SnowflakeGenerator.EPOCH - 1;
        long until = Math.min(window.until(), Math.max(window.from(), Math.max(clock.getAsLong(), last)));
        Window released = new Window(window.nodeId(), owner, window.from(), until);
        return map.replaceIfPresent(held.key(), held.value(), encode(released))
                .onFailure(failure -> log.warn("Snowflake node {} release failed; it expires with its lease",
                        window.nodeId(), failure))
                .mapEmpty();
    }

    private void cancelTimers() {
        if (verifyTimer != -1) {
            vertx.cancelTimer(verifyTimer);
            verifyTimer = -1;
        }
        if (renewTimer != -1) {
            vertx.cancelTimer(renewTimer);
            renewTimer = -1;
        }
    }

    private int members() {
        ClusterManager manager = vertx instanceof VertxInternal internal ? internal.clusterManager() : null;
        return manager == null ? 1 : manager.getNodes().size();
    }

    private long deadline() {
        return clock.getAsLong() + timing.acquireTimeoutMillis();
    }

    private long backoff() {
        return ThreadLocalRandom.current().nextLong(timing.backoffMinMillis(), timing.backoffMaxMillis() + 1);
    }

    String owner() {
        return owner;
    }

    static String encode(Window window) {
        return window.owner() + "," + window.from() + "," + window.until();
    }

    static Window decode(int node, String raw) {
        if (raw == null) {
            return null;
        }
        String[] parts = raw.split(",", -1);
        if (parts.length != 3) {
            return null;
        }
        try {
            return new Window(node, parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]));
        } catch (IllegalArgumentException malformed) {
            return null;
        }
    }
}
