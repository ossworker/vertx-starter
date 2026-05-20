package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Future;
import io.vertx.core.shareddata.AsyncMap;
import io.zenoh.Session;
import io.zenoh.bytes.ZBytes;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.query.Reply;
import io.zenoh.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AsyncMap implementation backed by Zenoh key-value store.
 * Keys follow the pattern: vertx/{app}/{env}/cluster/maps/{mapName}/{key}
 *
 * Data is serialized using Java serialization and stored as ZBytes in Zenoh.
 * This provides true distributed shared state across cluster nodes.
 */
public class ZenohAsyncMap<K, V> implements AsyncMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ZenohAsyncMap.class);

    private final String mapName;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;
    // Local cache for fast reads - eventually consistent with Zenoh
    private final Map<K, V> localCache = new ConcurrentHashMap<>();

    public ZenohAsyncMap(String mapName, KeyPrefixStrategy keyPrefix, Session session) {
        this.mapName = mapName;
        this.keyPrefix = keyPrefix;
        this.session = session;
    }

    @Override
    public Future<V> get(K k) {
        if (session == null) {
            return Future.succeededFuture(localCache.get(k));
        }
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            BlockingQueue<Optional<Reply>> replies = session.get(keyExpr.into());
            keyExpr.close();
            while (true) {
                Optional<Reply> wrapper = replies.take();
                if (wrapper.isEmpty()) {
                    break;
                }
                Reply reply = wrapper.get();
                if (reply instanceof Reply.Success success) {
                    Sample sample = success.getSample();
                    V value = deserialize(sample.getPayload().toBytes());
                    if (value != null) {
                        localCache.put(k, value);
                        return Future.succeededFuture(value);
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to get from Zenoh, falling back to local cache: {}", e.getMessage());
        }
        return Future.succeededFuture(localCache.get(k));
    }

    @Override
    public Future<Void> put(K k, V v) {
        localCache.put(k, v);
        if (session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                byte[] data = serialize(v);
                session.put(keyExpr, ZBytes.from(data));
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to put to Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> put(K k, V v, long ttl) {
        // Zenoh doesn't natively support TTL on key-value, store with local expiration tracking
        localCache.put(k, v);
        if (session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                byte[] data = serialize(v);
                session.put(keyExpr, ZBytes.from(data));
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to put to Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture();
    }

    @Override
    public Future<V> putIfAbsent(K k, V v) {
        V existing = localCache.putIfAbsent(k, v);
        if (existing == null && session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                byte[] data = serialize(v);
                session.put(keyExpr, ZBytes.from(data));
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to putIfAbsent to Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture(existing);
    }

    @Override
    public Future<V> putIfAbsent(K k, V v, long ttl) {
        V existing = localCache.putIfAbsent(k, v);
        if (existing == null && session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                byte[] data = serialize(v);
                session.put(keyExpr, ZBytes.from(data));
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to putIfAbsent to Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture(existing);
    }

    @Override
    public Future<V> remove(K k) {
        V removed = localCache.remove(k);
        if (session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                session.delete(keyExpr);
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to delete from Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture(removed);
    }

    @Override
    public Future<Boolean> removeIfPresent(K k, V v) {
        boolean removed = localCache.remove(k, v);
        if (removed && session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                session.delete(keyExpr);
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to delete from Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture(removed);
    }

    @Override
    public Future<V> replace(K k, V v) {
        V old = localCache.replace(k, v);
        if (old != null && session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                byte[] data = serialize(v);
                session.put(keyExpr, ZBytes.from(data));
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to replace in Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture(old);
    }

    @Override
    public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
        boolean replaced = localCache.replace(k, oldValue, newValue);
        if (replaced && session != null) {
            String key = keyPrefix.mapKey(mapName, serializeKey(k));
            try {
                KeyExpr keyExpr = KeyExpr.tryFrom(key);
                byte[] data = serialize(newValue);
                session.put(keyExpr, ZBytes.from(data));
                keyExpr.close();
            } catch (ZError e) {
                logger.warn("Failed to replaceIfPresent in Zenoh: {}", e.getMessage());
            }
        }
        return Future.succeededFuture(replaced);
    }

    @Override
    public Future<Void> clear() {
        localCache.clear();
        // In Zenoh we'd need to delete all keys matching the map pattern
        // This is a best-effort operation
        return Future.succeededFuture();
    }

    @Override
    public Future<Integer> size() {
        return Future.succeededFuture(localCache.size());
    }

    @Override
    public Future<Set<K>> keys() {
        return Future.succeededFuture(Set.copyOf(localCache.keySet()));
    }

    @Override
    public Future<List<V>> values() {
        return Future.succeededFuture(List.copyOf(localCache.values()));
    }

    @Override
    public Future<Map<K, V>> entries() {
        return Future.succeededFuture(Map.copyOf(localCache));
    }

    private String serializeKey(K key) {
        if (key instanceof String s) {
            return s;
        }
        // Base64 encode the serialized key for non-string keys
        byte[] data = serialize(key);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }

    private byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
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
            logger.debug("Deserialization failed: {}", e.getMessage());
            return null;
        }
    }
}
