package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Future;
import io.vertx.core.shareddata.AsyncMap;
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
 * {@link AsyncMap} view over the in-process {@link ZenohMapStore} replica.
 * <p>
 * Reads are plain local memory lookups and complete on the calling thread — no JNI, no
 * {@code executeBlocking}. Writes mutate the local replica atomically and hand the change to the
 * store for asynchronous replication to peers.
 * <p>
 * {@code putIfAbsent}, {@code removeIfPresent} and {@code replaceIfPresent} run inside a
 * {@code ConcurrentHashMap} compute block, so they are atomic with respect to other threads on this
 * node. Across nodes the store is eventually consistent: two nodes racing on the same key can both
 * observe success and converge on the write that arrives last.
 */
public class ZenohAsyncMap<K, V> implements AsyncMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ZenohAsyncMap.class);

    private final String mapName;
    private final KeyPrefixStrategy keyPrefix;
    private final ZenohMapStore store;
    private final String rangePrefix;

    public ZenohAsyncMap(String mapName, KeyPrefixStrategy keyPrefix, ZenohMapStore store) {
        this.mapName = mapName;
        this.keyPrefix = keyPrefix;
        this.store = store;
        this.rangePrefix = keyPrefix.mapKey(mapName, "");
    }

    @Override
    public Future<V> get(K k) {
        return Future.succeededFuture(deserialize(store.getRaw(entryKey(k))));
    }

    @Override
    public Future<Void> put(K k, V v) {
        store.putRaw(entryKey(k), serialize(v), 0L);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> put(K k, V v, long ttl) {
        store.putRaw(entryKey(k), serialize(v), ttl);
        return Future.succeededFuture();
    }

    @Override
    public Future<V> putIfAbsent(K k, V v) {
        return putIfAbsent(k, v, 0L);
    }

    @Override
    public Future<V> putIfAbsent(K k, V v, long ttl) {
        byte[] existing = store.putRawIfAbsent(entryKey(k), serialize(v), ttl);
        return Future.succeededFuture(deserialize(existing));
    }

    @Override
    public Future<V> remove(K k) {
        return Future.succeededFuture(deserialize(store.removeRaw(entryKey(k))));
    }

    @Override
    public Future<Boolean> removeIfPresent(K k, V v) {
        return Future.succeededFuture(store.removeRawIf(entryKey(k), equalTo(v)));
    }

    @Override
    public Future<V> replace(K k, V v) {
        return Future.succeededFuture(deserialize(store.replaceRaw(entryKey(k), serialize(v))));
    }

    @Override
    public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
        return Future.succeededFuture(
                store.replaceRawIf(entryKey(k), equalTo(oldValue), serialize(newValue)));
    }

    @Override
    public Future<Void> clear() {
        store.clearRange(rangePrefix);
        return Future.succeededFuture();
    }

    @Override
    public Future<Integer> size() {
        return Future.succeededFuture(store.scanRange(rangePrefix).size());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Set<K>> keys() {
        Set<K> result = new HashSet<>();
        for (String fullKey : store.scanRange(rangePrefix).keySet()) {
            result.add((K) fullKey.substring(rangePrefix.length()));
        }
        return Future.succeededFuture(result);
    }

    @Override
    public Future<List<V>> values() {
        List<V> result = new ArrayList<>();
        for (byte[] raw : store.scanRange(rangePrefix).values()) {
            V value = deserialize(raw);
            if (value != null) {
                result.add(value);
            }
        }
        return Future.succeededFuture(result);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<K, V>> entries() {
        Map<K, V> result = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.scanRange(rangePrefix).entrySet()) {
            V value = deserialize(e.getValue());
            if (value != null) {
                result.put((K) e.getKey().substring(rangePrefix.length()), value);
            }
        }
        return Future.succeededFuture(result);
    }

    public String getMapName() {
        return mapName;
    }

    private String entryKey(K k) {
        return keyPrefix.mapKey(mapName, serializeKey(k));
    }

    /**
     * Runs inside the store's atomic compute block, so the stored bytes are deserialized on every
     * comparison. Comparing serialized bytes instead would be wrong: JDK serialization is not
     * canonical for values such as {@code HashMap}, whose field order depends on iteration order.
     */
    private Predicate<byte[]> equalTo(V expected) {
        return stored -> Objects.equals(deserialize(stored), expected);
    }

    // --- Serialization ---

    private String serializeKey(K key) {
        if (key instanceof String s) {
            return s;
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(serialize(key));
    }

    private byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Cannot store a non-serializable value in async map '" + mapName + "'", e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.debug("Deserialization failed in async map '{}': {}", mapName, e.getMessage());
            return null;
        }
    }
}
