package com.workoss.boot.zenoh.cluster.internal.store;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * In-memory replica of the cluster store with last-writer-wins convergence.
 * <p>
 * Every entry carries a {@link HybridClock} version and the id of the node that wrote it; an
 * incoming write replaces the local entry only when its {@code (version, origin)} is greater, so
 * every replica ends up on the same winner whatever order writes arrive in, and a node's own
 * echoed writes are no-ops. Deletes are kept as tombstones for the retention period so a delayed
 * older write cannot resurrect a deleted entry. Expiry instants are absolute and travel with the
 * entry, so all replicas expire it together.
 * <p>
 * Keys are {@code (namespace, key)} where {@code key} is a single escaped chunk; on the wire they
 * become {@code {storePrefix}/{namespace}/{key}}.
 */
public final class ReplicatedStore {

    @FunctionalInterface
    public interface Sink {
        void publish(String fullKey, byte[] payload);
    }

    public static final Sink NO_REPLICATION = (key, payload) -> {
    };

    /**
     * @param previous live value before the update, or {@code null}
     * @param changed  whether the update wrote anything
     */
    public record Result(byte[] previous, boolean changed) {
    }

    private static final byte WIRE_VERSION = 1;
    private static final byte FLAG_TOMBSTONE = 1;
    private static final int HEADER_BYTES = 1 + 1 + Long.BYTES + Long.BYTES + Short.BYTES;

    private record Entry(byte[] value, long expiresAt, long version, String origin) {

        boolean tombstone() {
            return value == null;
        }

        boolean liveAt(long now) {
            return value != null && (expiresAt <= 0 || now < expiresAt);
        }

        boolean supersedes(Entry other) {
            return version != other.version ? version > other.version : origin.compareTo(other.origin) > 0;
        }
    }

    private final String keyPrefix;
    private final String nodeId;
    private final HybridClock clock;
    private final long tombstoneRetentionMillis;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Entry>> namespaces = new ConcurrentHashMap<>();
    private volatile Sink sink = NO_REPLICATION;

    public ReplicatedStore(String storePrefix, String nodeId, HybridClock clock, long tombstoneRetentionMillis) {
        this.keyPrefix = storePrefix + "/";
        this.nodeId = nodeId;
        this.clock = clock;
        this.tombstoneRetentionMillis = tombstoneRetentionMillis;
    }

    public void setSink(Sink sink) {
        this.sink = sink == null ? NO_REPLICATION : sink;
    }

    // --- Reads ---

    /**
     * @return the live value, or {@code null}
     */
    public byte[] get(String namespace, String key) {
        ConcurrentHashMap<String, Entry> entries = namespaces.get(namespace);
        Entry entry = entries == null ? null : entries.get(key);
        return entry != null && entry.liveAt(System.currentTimeMillis()) ? entry.value() : null;
    }

    /**
     * Snapshot of the live entries of a namespace.
     */
    public Map<String, byte[]> entries(String namespace) {
        ConcurrentHashMap<String, Entry> entries = namespaces.get(namespace);
        if (entries == null) {
            return Map.of();
        }
        long now = System.currentTimeMillis();
        Map<String, byte[]> result = new HashMap<>();
        entries.forEach((key, entry) -> {
            if (entry.liveAt(now)) {
                result.put(key, entry.value());
            }
        });
        return result;
    }

    public int size(String namespace) {
        ConcurrentHashMap<String, Entry> entries = namespaces.get(namespace);
        if (entries == null) {
            return 0;
        }
        long now = System.currentTimeMillis();
        int count = 0;
        for (Entry entry : entries.values()) {
            if (entry.liveAt(now)) {
                count++;
            }
        }
        return count;
    }

    // --- Writes ---

    /**
     * Atomically derives a change from the current live value ({@code null} when absent) and, if
     * it writes, replicates the result. {@code fn} runs inside a {@code ConcurrentHashMap} compute
     * block: it must be fast and must not touch this store.
     */
    public Result update(String namespace, String key, Function<byte[], Change> fn) {
        Entry[] written = new Entry[1];
        byte[][] previous = new byte[1][];
        namespace(namespace).compute(key, (ignored, current) -> {
            long now = System.currentTimeMillis();
            Entry live = current != null && current.liveAt(now) ? current : null;
            previous[0] = live == null ? null : live.value();
            Entry next = switch (fn.apply(previous[0])) {
                case Change.Keep keep -> null;
                case Change.Delete delete -> live == null ? null : new Entry(null, 0L, clock.next(), nodeId);
                case Change.Put put -> new Entry(put.value(),
                        put.keepExpiry() ? (live == null ? 0L : live.expiresAt()) : expiryOf(put.ttlMillis(), now),
                        clock.next(), nodeId);
            };
            written[0] = next;
            return next == null ? current : next;
        });
        if (written[0] != null) {
            sink.publish(keyPrefix + namespace + "/" + key, encode(written[0]));
        }
        return new Result(previous[0], written[0] != null);
    }

    public void clear(String namespace) {
        ConcurrentHashMap<String, Entry> entries = namespaces.get(namespace);
        if (entries == null) {
            return;
        }
        for (String key : entries.keySet()) {
            update(namespace, key, current -> current == null ? Change.KEEP : Change.DELETE);
        }
    }

    // --- Replication ---

    /**
     * Merges a write received from a peer (or from a bootstrap reply).
     *
     * @return whether the local replica changed
     */
    public boolean applyRemote(String fullKey, byte[] payload) {
        String[] location = locate(fullKey);
        Entry incoming = location == null ? null : decode(payload);
        if (incoming == null) {
            return false;
        }
        clock.observe(incoming.version());
        boolean[] applied = new boolean[1];
        namespace(location[0]).compute(location[1], (ignored, current) -> {
            if (current == null || incoming.supersedes(current)) {
                applied[0] = true;
                return incoming;
            }
            return current;
        });
        return applied[0];
    }

    /**
     * Emits every entry matching {@code selector} — tombstones included, so a peer bootstrapping
     * from us also learns about deletes. Supports exact keys and {@code prefix/**}.
     */
    public void forEachMatching(String selector, BiConsumer<String, byte[]> consumer) {
        if (selector.endsWith("/**")) {
            String prefix = selector.substring(0, selector.length() - 2);
            namespaces.forEach((namespace, entries) -> entries.forEach((key, entry) -> {
                String fullKey = keyPrefix + namespace + "/" + key;
                if (fullKey.startsWith(prefix)) {
                    consumer.accept(fullKey, encode(entry));
                }
            }));
            return;
        }
        String[] location = locate(selector);
        if (location == null) {
            return;
        }
        ConcurrentHashMap<String, Entry> entries = namespaces.get(location[0]);
        Entry entry = entries == null ? null : entries.get(location[1]);
        if (entry != null) {
            consumer.accept(selector, encode(entry));
        }
    }

    /**
     * Turns expired entries into tombstones (locally only; every replica does the same) and drops
     * tombstones past the retention period.
     */
    public void sweep() {
        long now = System.currentTimeMillis();
        namespaces.forEach((namespace, entries) -> entries.forEach((key, entry) -> {
            if (!entry.tombstone() && !entry.liveAt(now)) {
                entries.computeIfPresent(key, (k, current) -> current == entry
                        ? new Entry(null, entry.expiresAt(), entry.version(), entry.origin()) : current);
            } else if (entry.tombstone() && purgeDeadline(entry) <= now) {
                entries.computeIfPresent(key, (k, current) -> current == entry ? null : current);
            }
        }));
    }

    // --- Internals ---

    private ConcurrentHashMap<String, Entry> namespace(String namespace) {
        return namespaces.computeIfAbsent(namespace, n -> new ConcurrentHashMap<>());
    }

    /**
     * @return {@code [namespace, key]}, or {@code null} for a key outside the store
     */
    private String[] locate(String fullKey) {
        if (fullKey == null || !fullKey.startsWith(keyPrefix)) {
            return null;
        }
        String relative = fullKey.substring(keyPrefix.length());
        int slash = relative.lastIndexOf('/');
        if (slash <= 0 || slash == relative.length() - 1) {
            return null;
        }
        return new String[]{relative.substring(0, slash), relative.substring(slash + 1)};
    }

    private long purgeDeadline(Entry tombstone) {
        return Math.max(HybridClock.wallMillis(tombstone.version()), tombstone.expiresAt()) + tombstoneRetentionMillis;
    }

    private static long expiryOf(long ttlMillis, long now) {
        return ttlMillis > 0 ? now + ttlMillis : 0L;
    }

    // Wire: [1B wire version][1B flags][8B version][8B expiresAt][2B originLen][origin][value]
    private static byte[] encode(Entry entry) {
        byte[] origin = entry.origin().getBytes(StandardCharsets.UTF_8);
        byte[] value = entry.tombstone() ? new byte[0] : entry.value();
        return ByteBuffer.allocate(HEADER_BYTES + origin.length + value.length)
                .put(WIRE_VERSION)
                .put(entry.tombstone() ? FLAG_TOMBSTONE : 0)
                .putLong(entry.version())
                .putLong(entry.expiresAt())
                .putShort((short) origin.length)
                .put(origin)
                .put(value)
                .array();
    }

    private static Entry decode(byte[] payload) {
        if (payload == null || payload.length < HEADER_BYTES || payload[0] != WIRE_VERSION) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(payload, 1, payload.length - 1);
            boolean tombstone = (buffer.get() & FLAG_TOMBSTONE) != 0;
            long version = buffer.getLong();
            long expiresAt = buffer.getLong();
            byte[] origin = new byte[Short.toUnsignedInt(buffer.getShort())];
            buffer.get(origin);
            byte[] value = new byte[buffer.remaining()];
            buffer.get(value);
            return new Entry(tombstone ? null : value, expiresAt, version, new String(origin, StandardCharsets.UTF_8));
        } catch (BufferUnderflowException e) {
            return null;
        }
    }
}
