package com.workoss.boot.zenoh.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Multi-node shared map backed by the replicated {@link ZenohMapStore}.
 * <p>
 * Reads are plain local memory lookups and return on the calling thread — no JNI, no executor hop.
 * Writes mutate the local replica atomically and hand the change to the store for asynchronous
 * replication to peers.
 * <p>
 * {@code putIfAbsent}, {@code removeIfPresent} and {@code replaceIfPresent} run inside a
 * {@code ConcurrentHashMap} compute block, so they are atomic with respect to other threads on this
 * node. Across nodes the map is eventually consistent: two nodes racing on the same key can both
 * observe success and converge on the write that arrives last.
 * <p>
 * Keys are encoded as raw UTF-8 when they are {@link String}s, and as Base64 of their JDK
 * serialization otherwise. Values go through the configured {@link ValueCodec}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ZenohSharedMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ZenohSharedMap.class);

    private final String mapName;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMapStore store;
    private final ValueCodec<V> valueCodec;
    private final String rangePrefix;

    public ZenohSharedMap(String mapName, KeyPrefixStrategy keyPrefix, ZenohMapStore store,
                          ValueCodec<V> valueCodec) {
        this.mapName = mapName;
        this.keyPrefix = keyPrefix;
        this.store = store;
        this.valueCodec = valueCodec;
        this.rangePrefix = keyPrefix.mapKey(mapName, "");
    }

    public String getMapName() {
        return mapName;
    }

    // --- Reads ---

    /**
     * @return the value, or {@code null} when absent
     */
    public V get(K key) {
        return valueCodec.decode(store.getRaw(entryKey(key)));
    }

    public boolean containsKey(K key) {
        return store.getRaw(entryKey(key)) != null;
    }

    public int size() {
        return store.scanRange(rangePrefix).size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    @SuppressWarnings("unchecked")
    public Set<K> keys() {
        Set<K> result = new HashSet<>();
        for (String fullKey : store.scanRange(rangePrefix).keySet()) {
            result.add((K) decodeKey(fullKey.substring(rangePrefix.length())));
        }
        return result;
    }

    public List<V> values() {
        List<V> result = new ArrayList<>();
        for (byte[] raw : store.scanRange(rangePrefix).values()) {
            V value = valueCodec.decode(raw);
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<K, V> entries() {
        Map<K, V> result = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.scanRange(rangePrefix).entrySet()) {
            V value = valueCodec.decode(e.getValue());
            if (value != null) {
                result.put((K) decodeKey(e.getKey().substring(rangePrefix.length())), value);
            }
        }
        return result;
    }

    // --- Writes ---

    public void put(K key, V value) {
        put(key, value, 0L);
    }

    /**
     * @param ttlMillis time-to-live; {@code <= 0} means no expiry
     */
    public void put(K key, V value, long ttlMillis) {
        store.putRaw(entryKey(key), valueCodec.encode(value), ttlMillis);
    }

    /**
     * @return the value already present, or {@code null} if this call stored {@code value}
     */
    public V putIfAbsent(K key, V value) {
        byte[] existing = store.putRawIfAbsent(entryKey(key), valueCodec.encode(value), 0L);
        return valueCodec.decode(existing);
    }

    /**
     * @return the removed value, or {@code null} when the key was absent
     */
    public V remove(K key) {
        return valueCodec.decode(store.removeRaw(entryKey(key)));
    }

    public boolean removeIfPresent(K key, V value) {
        return store.removeRawIf(entryKey(key), equalTo(value));
    }

    /**
     * @return the previous value, or {@code null} when the key was absent
     */
    public V replace(K key, V value) {
        return valueCodec.decode(store.replaceRaw(entryKey(key), valueCodec.encode(value)));
    }

    public boolean replaceIfPresent(K key, V oldValue, V newValue) {
        return store.replaceRawIf(entryKey(key), equalTo(oldValue), valueCodec.encode(newValue));
    }

    public void clear() {
        store.clearRange(rangePrefix);
    }

    // --- Internals ---

    private String entryKey(K key) {
        return keyPrefix.mapKey(mapName, encodeKey(key));
    }

    /**
     * Runs inside the store's atomic compute block, so the stored bytes are decoded on every
     * comparison. Comparing encoded bytes instead would be wrong: JDK serialization is not
     * canonical for values such as {@code HashMap}, whose field order depends on iteration order.
     */
    private Predicate<byte[]> equalTo(V expected) {
        return stored -> Objects.equals(valueCodec.decode(stored), expected);
    }

    private String encodeKey(K key) {
        if (key instanceof String s) {
            return s;
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(encodeKeyObject(key));
    }

    private byte[] encodeKeyObject(Object key) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(key);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Cannot use a non-serializable key in shared map '" + mapName + "'", e);
        }
    }

    @SuppressWarnings("unchecked")
    private K decodeKey(String encoded) {
        if (encoded.isEmpty()) {
            return null;
        }
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (K) ois.readObject();
            }
        } catch (Exception e) {
            // Not Base64/not serializable — it was stored as a raw String key.
            return (K) encoded;
        }
    }
}
