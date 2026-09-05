package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Query;
import com.workoss.zenoh.Queryable;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Sample;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * In-process replica of the whole {@code vertx/{app}/{env}/cluster/**} keyspace.
 * <p>
 * Replaces the Zenoh storage-manager plugin (filesystem / rocksdb backend). State lives in a
 * {@link ConcurrentHashMap}; Zenoh is used only to move writes between nodes and to let a freshly
 * joined node catch up:
 * <ul>
 *   <li>a {@link Subscriber} on {@code cluster/**} applies remote writes to the local replica</li>
 *   <li>a {@link Queryable} on {@code cluster/**} answers remote {@code session.get()} calls, which
 *       is how a joining node bootstraps its replica</li>
 * </ul>
 * Reads never touch JNI, so they are plain memory lookups on the calling thread.
 * <p>
 * Consistency model: mutations are atomic on the local replica (they run inside
 * {@code ConcurrentHashMap} compute blocks) and are then replicated asynchronously, so the store is
 * eventually consistent across nodes. Concurrent mutations of the same key from different nodes
 * converge on whichever write arrives last — the same guarantee the plugin-backed store provided,
 * where the router serialized writes by arrival order.
 * <p>
 * Trade-off versus the plugin: nothing survives a full cluster restart.
 */
public class ZenohMapStore {

    private static final Logger logger = LoggerFactory.getLogger(ZenohMapStore.class);

    /** Sample kind value for a Zenoh delete. */
    private static final byte KIND_DELETE = 1;

    private static final long SWEEP_INTERVAL_MILLIS = 5_000L;

    private final Vertx vertx;
    private final KeyPrefixStrategy keyPrefix;
    private final String nodeId;
    private final String clusterPattern;

    private final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Session session;
    private Subscriber replicaSubscriber;
    private Queryable queryable;
    private Thread replicaPollThread;
    private Thread queryPollThread;
    private Long sweeperTimerId;

    public ZenohMapStore(Vertx vertx, KeyPrefixStrategy keyPrefix, String nodeId) {
        this.vertx = vertx;
        this.keyPrefix = keyPrefix;
        this.nodeId = nodeId;
        this.clusterPattern = keyPrefix.getPrefix() + "/**";
    }

    /**
     * A stored value plus its absolute expiry, both of which travel over the wire so every replica
     * expires the entry at the same instant without needing a coordinating node.
     */
    private record Entry(byte[] value, long expiresAt) {
        boolean expired(long now) {
            return expiresAt > 0 && now >= expiresAt;
        }
    }

    // --- Lifecycle ---

    /**
     * Wires the local replica up to the Zenoh session. Until this is called the store is a plain
     * in-process map.
     */
    public void start(Session session) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.session = session;
        if (session != null) {
            // Declare the subscriber before bootstrapping so writes issued by peers in between are
            // not lost; bootstrap merges with putIfAbsent so those newer writes win.
            this.replicaSubscriber = new Subscriber(session, clusterPattern);
            this.queryable = new Queryable(session, clusterPattern);
            this.replicaPollThread = startPollThread(this::pollReplica, "zenoh-mapstore-replica");
            this.queryPollThread = startPollThread(this::pollQueries, "zenoh-mapstore-query");
            bootstrap();
        }
        if (vertx != null) {
            this.sweeperTimerId = vertx.setPeriodic(SWEEP_INTERVAL_MILLIS, id -> sweep());
        }
        logger.info("Cluster map store started for node {} (session={})", nodeId, session != null);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (vertx != null && sweeperTimerId != null) {
            vertx.cancelTimer(sweeperTimerId);
            sweeperTimerId = null;
        }
        // Undeclare first: that drops the sender side of each channel and unblocks the poll threads.
        Queryable q = this.queryable;
        this.queryable = null;
        if (q != null) {
            try {
                q.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare map store queryable: {}", e.getMessage());
            }
        }
        Subscriber s = this.replicaSubscriber;
        this.replicaSubscriber = null;
        if (s != null) {
            try {
                s.undeclare();
            } catch (Exception e) {
                logger.debug("Failed to undeclare map store subscriber: {}", e.getMessage());
            }
        }
        joinPollThread(replicaPollThread);
        replicaPollThread = null;
        joinPollThread(queryPollThread);
        queryPollThread = null;

        this.session = null;
        data.clear();
        logger.info("Cluster map store stopped for node {}", nodeId);
    }

    // --- Reads (local memory, never blocking) ---

    /**
     * @return the stored value, or {@code null} when absent or expired
     */
    public byte[] getRaw(String fullKey) {
        Entry entry = data.get(fullKey);
        if (entry == null) {
            return null;
        }
        if (entry.expired(System.currentTimeMillis())) {
            data.remove(fullKey, entry);
            return null;
        }
        return entry.value();
    }

    /**
     * Snapshot of every live entry whose full key starts with {@code rangePrefix}.
     */
    public Map<String, byte[]> scanRange(String rangePrefix) {
        long now = System.currentTimeMillis();
        Map<String, byte[]> result = new HashMap<>();
        for (Map.Entry<String, Entry> e : data.entrySet()) {
            if (e.getKey().startsWith(rangePrefix) && !e.getValue().expired(now)) {
                result.put(e.getKey(), e.getValue().value());
            }
        }
        return result;
    }

    // --- Mutations (atomic locally, replicated asynchronously) ---

    public void putRaw(String fullKey, byte[] value, long ttlMillis) {
        Entry entry = new Entry(value, expiryOf(ttlMillis));
        data.put(fullKey, entry);
        broadcastPut(fullKey, entry);
    }

    /**
     * @return the value already present, or {@code null} if this call stored {@code value}
     */
    public byte[] putRawIfAbsent(String fullKey, byte[] value, long ttlMillis) {
        Entry candidate = new Entry(value, expiryOf(ttlMillis));
        byte[][] existing = new byte[1][];
        boolean[] stored = new boolean[1];
        data.compute(fullKey, (k, cur) -> {
            if (cur != null && !cur.expired(System.currentTimeMillis())) {
                existing[0] = cur.value();
                return cur;
            }
            stored[0] = true;
            return candidate;
        });
        if (stored[0]) {
            broadcastPut(fullKey, candidate);
        }
        return existing[0];
    }

    /**
     * @return the removed value, or {@code null} if the key was absent or expired
     */
    public byte[] removeRaw(String fullKey) {
        Entry removed = data.remove(fullKey);
        if (removed == null || removed.expired(System.currentTimeMillis())) {
            return null;
        }
        broadcastDelete(fullKey);
        return removed.value();
    }

    public boolean removeRawIf(String fullKey, Predicate<byte[]> matches) {
        AtomicBoolean removed = new AtomicBoolean(false);
        data.computeIfPresent(fullKey, (k, cur) -> {
            if (cur.expired(System.currentTimeMillis())) {
                return null;
            }
            if (matches.test(cur.value())) {
                removed.set(true);
                return null;
            }
            return cur;
        });
        if (removed.get()) {
            broadcastDelete(fullKey);
        }
        return removed.get();
    }

    /**
     * Replaces the value of an existing entry, preserving its expiry.
     *
     * @return the previous value, or {@code null} if the key was absent or expired
     */
    public byte[] replaceRaw(String fullKey, byte[] value) {
        byte[][] previous = new byte[1][];
        Entry[] replacement = new Entry[1];
        data.computeIfPresent(fullKey, (k, cur) -> {
            if (cur.expired(System.currentTimeMillis())) {
                return null;
            }
            previous[0] = cur.value();
            replacement[0] = new Entry(value, cur.expiresAt());
            return replacement[0];
        });
        if (replacement[0] != null) {
            broadcastPut(fullKey, replacement[0]);
        }
        return previous[0];
    }

    public boolean replaceRawIf(String fullKey, Predicate<byte[]> matches, byte[] newValue) {
        Entry[] replacement = new Entry[1];
        data.computeIfPresent(fullKey, (k, cur) -> {
            if (cur.expired(System.currentTimeMillis())) {
                return null;
            }
            if (matches.test(cur.value())) {
                replacement[0] = new Entry(newValue, cur.expiresAt());
                return replacement[0];
            }
            return cur;
        });
        if (replacement[0] != null) {
            broadcastPut(fullKey, replacement[0]);
            return true;
        }
        return false;
    }

    /**
     * Atomically derives a new value from the current one. Returning {@code null} from
     * {@code fn} removes the entry. The result carries no expiry — callers that need a TTL should
     * use {@link #putRaw}.
     *
     * @return the current (non-expired) value passed to {@code fn}, or {@code null} if absent
     */
    public byte[] computeRaw(String fullKey, Function<byte[], byte[]> fn) {
        byte[][] previous = new byte[1][];
        Entry[] replacement = new Entry[1];
        data.compute(fullKey, (k, cur) -> {
            if (cur != null && cur.expired(System.currentTimeMillis())) {
                cur = null;
            }
            previous[0] = cur == null ? null : cur.value();
            byte[] computed = fn.apply(previous[0]);
            // Returning the same instance is how callers signal "unchanged" — keep the entry as it
            // was so no replication is triggered.
            replacement[0] = computed == null ? null
                    : computed == previous[0] ? cur
                    : new Entry(computed, 0L);
            return replacement[0];
        });
        if (replacement[0] != null && replacement[0].value() != previous[0]) {
            broadcastPut(fullKey, replacement[0]);
        } else if (replacement[0] == null && previous[0] != null) {
            broadcastDelete(fullKey);
        }
        return previous[0];
    }

    public void clearRange(String rangePrefix) {
        long now = System.currentTimeMillis();
        for (String key : data.keySet()) {
            if (!key.startsWith(rangePrefix)) {
                continue;
            }
            Entry removed = data.remove(key);
            if (removed != null && !removed.expired(now)) {
                broadcastDelete(key);
            }
        }
    }

    // --- Queryable support ---

    /**
     * Answers a peer's {@code session.get()} from the local replica. Every query must be closed by
     * the caller even when nothing matches, otherwise the peer waits out the full Zenoh get timeout.
     */
    public void answer(Query query) {
        String selector = query.getKeyExpr();
        if (selector == null || selector.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (selector.endsWith("/**")) {
            String prefix = selector.substring(0, selector.length() - "/**".length()) + "/";
            for (Map.Entry<String, Entry> e : data.entrySet()) {
                if (e.getKey().startsWith(prefix) && !e.getValue().expired(now)) {
                    query.reply(e.getKey(), encode(e.getValue()));
                }
            }
        } else {
            Entry entry = data.get(selector);
            if (entry != null && !entry.expired(now)) {
                query.reply(selector, encode(entry));
            }
        }
    }

    // --- Replication ---

    /**
     * Pulls the keyspace from peers that were already running when this node joined.
     */
    private void bootstrap() {
        try {
            List<Reply> replies = session.get(clusterPattern);
            long now = System.currentTimeMillis();
            int loaded = 0;
            for (Reply reply : replies) {
                if (!reply.success() || reply.sampleKeyExpr().isEmpty() || reply.samplePayload().isEmpty()) {
                    continue;
                }
                Entry entry = decode(reply.samplePayload().get());
                if (entry == null || entry.expired(now)) {
                    continue;
                }
                // putIfAbsent: anything the replica subscriber already delivered is newer than this
                // snapshot, so it must win.
                if (data.putIfAbsent(reply.sampleKeyExpr().get(), entry) == null) {
                    loaded++;
                }
            }
            if (loaded > 0) {
                logger.info("Bootstrapped {} cluster entries from peers", loaded);
            }
        } catch (Exception e) {
            logger.warn("Failed to bootstrap cluster state from peers: {}", e.getMessage());
        }
    }

    private void pollReplica() {
        while (running.get() && replicaSubscriber != null && !replicaSubscriber.isUndeclared()) {
            try {
                Optional<Sample> sampleOpt = replicaSubscriber.recv();
                if (sampleOpt.isEmpty()) {
                    continue;
                }
                Sample sample = sampleOpt.get();
                String key = sample.keyExpr();
                if (sample.kind() == KIND_DELETE) {
                    data.remove(key);
                } else {
                    Entry entry = decode(sample.payload());
                    if (entry != null) {
                        data.put(key, entry);
                    }
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                logger.debug("Map store replica poll error: {}", e.getMessage());
            }
        }
    }

    private void pollQueries() {
        while (running.get() && queryable != null && !queryable.isUndeclared()) {
            Queryable current = queryable;
            if (current == null) {
                break;
            }
            try (Query query = new Query(current)) {
                answer(query);
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                if (queryable == null || queryable.isUndeclared()) {
                    break;
                }
                logger.debug("Map store query poll error: {}", e.getMessage());
            }
        }
    }

    private void sweep() {
        long now = System.currentTimeMillis();
        data.entrySet().removeIf(e -> e.getValue().expired(now));
    }

    private void broadcastPut(String fullKey, Entry entry) {
        if (entry == null || session == null || vertx == null) {
            return;
        }
        byte[] payload = encode(entry);
        vertx.<Void>executeBlocking(() -> {
                    session.put(fullKey, payload);
                    return null;
                })
                .onFailure(t -> logger.warn("Failed to replicate put of '{}': {}", fullKey, t.getMessage()));
    }

    private void broadcastDelete(String fullKey) {
        if (session == null || vertx == null) {
            return;
        }
        vertx.<Void>executeBlocking(() -> {
                    session.delete(fullKey);
                    return null;
                })
                .onFailure(t -> logger.warn("Failed to replicate delete of '{}': {}", fullKey, t.getMessage()));
    }

    // --- Wire format ---

    private static long expiryOf(long ttlMillis) {
        return ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : 0L;
    }

    private static byte[] encode(Entry entry) {
        byte[] value = entry.value();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + value.length);
        buffer.putLong(entry.expiresAt());
        buffer.put(value);
        return buffer.array();
    }

    private static Entry decode(byte[] payload) {
        if (payload == null || payload.length < Long.BYTES) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        long expiresAt = buffer.getLong();
        byte[] value = new byte[payload.length - Long.BYTES];
        buffer.get(value);
        return new Entry(value, expiresAt);
    }

    // --- Threads ---

    private Thread startPollThread(Runnable loop, String namePrefix) {
        String suffix = nodeId == null || nodeId.length() < 8 ? String.valueOf(nodeId) : nodeId.substring(0, 8);
        Thread thread = new Thread(loop, namePrefix + "-" + suffix);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void joinPollThread(Thread thread) {
        if (thread == null) {
            return;
        }
        thread.interrupt();
        try {
            thread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
