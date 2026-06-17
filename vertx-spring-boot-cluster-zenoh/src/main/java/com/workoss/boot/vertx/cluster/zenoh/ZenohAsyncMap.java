package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Session;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * AsyncMap implementation backed entirely by Zenoh storage.
 * <p>
 * No local cache — all reads/writes go directly to the Zenoh backend
 * via {@code vertx.executeBlocking()}. Relies on Zenoh's storage plugin
 * (e.g. filesystem backend) for persistence and cross-node consistency.
 * <ul>
 *   <li>put → vertx.executeBlocking(session.put)</li>
 *   <li>get → vertx.executeBlocking(session.get)</li>
 *   <li>remove → vertx.executeBlocking(session.delete)</li>
 *   <li>ttl → vertx.setTimer(delay, ...)</li>
 * </ul>
 */
public class ZenohAsyncMap<K, V> implements AsyncMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ZenohAsyncMap.class);

    private final String mapName;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;
    private final Vertx vertx;

    public ZenohAsyncMap(String mapName, KeyPrefixStrategy keyPrefix, Session session, Vertx vertx) {
        this.mapName = mapName;
        this.keyPrefix = keyPrefix;
        this.session = session;
        this.vertx = vertx;
    }

    // --- AsyncMap operations: all via vertx.executeBlocking() → Zenoh ---

    @Override
    public Future<V> get(K k) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        if (session == null) return Future.succeededFuture(null);
        return vertx.executeBlocking(() -> querySingle(key));
    }

    @Override
    public Future<Void> put(K k, V v) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        byte[] data = serialize(v);
        if (session != null) {
            return vertx.<Void>executeBlocking(() -> { doPut(key, data); return null; });
        }
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> put(K k, V v, long ttl) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        byte[] data = serialize(v);
        if (ttl > 0) {
            vertx.setTimer(ttl, id -> remove(k));
        }
        if (session != null) {
            return vertx.<Void>executeBlocking(() -> { doPut(key, data); return null; });
        }
        return Future.succeededFuture();
    }

    @Override
    public Future<V> putIfAbsent(K k, V v) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        byte[] data = serialize(v);
        if (session == null) return Future.succeededFuture(null);
        return vertx.executeBlocking(() -> {
            // Query first — if exists, return existing value
            V existing = querySingle(key);
            if (existing != null) {
                return existing;
            }
            doPut(key, data);
            return null;
        });
    }

    @Override
    public Future<V> putIfAbsent(K k, V v, long ttl) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        byte[] data = serialize(v);
        if (session == null) return Future.succeededFuture(null);
        return vertx.executeBlocking(() -> {
            V existing = querySingle(key);
            if (existing != null) {
                return existing;
            }
            doPut(key, data);
            if (ttl > 0) {
                vertx.setTimer(ttl, id -> remove(k));
            }
            return null;
        });
    }

    @Override
    public Future<V> remove(K k) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        if (session != null) {
            return vertx.executeBlocking(() -> {
                V old = querySingle(key);
                doDelete(key);
                return old;
            });
        }
        return Future.succeededFuture(null);
    }

    @Override
    public Future<Boolean> removeIfPresent(K k, V v) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        if (session == null) return Future.succeededFuture(false);
        return vertx.executeBlocking(() -> {
            V current = querySingle(key);
            if (current != null && current.equals(v)) {
                doDelete(key);
                return true;
            }
            return false;
        });
    }

    @Override
    public Future<V> replace(K k, V v) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        byte[] data = serialize(v);
        if (session == null) return Future.succeededFuture(null);
        return vertx.executeBlocking(() -> {
            V current = querySingle(key);
            if (current != null) {
                doPut(key, data);
                return current;
            }
            return null;
        });
    }

    @Override
    public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        byte[] data = serialize(newValue);
        if (session == null) return Future.succeededFuture(false);
        return vertx.executeBlocking(() -> {
            V current = querySingle(key);
            if (current != null && current.equals(oldValue)) {
                doPut(key, data);
                return true;
            }
            return false;
        });
    }

    @Override
    public Future<Void> clear() {
        if (session == null) return Future.succeededFuture();
        return vertx.<Void>executeBlocking(() -> {
            try {
                List<Reply> replies = session.get(keyPrefix.mapPattern(mapName));
                for (Reply reply : replies) {
                    if (reply.success() && reply.sampleKeyExpr().isPresent()) {
                        doDelete(reply.sampleKeyExpr().get());
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to clear via zenoh: {}", e.getMessage());
            }
            return null;
        });
    }

    @Override
    public Future<Integer> size() {
        if (session == null) return Future.succeededFuture(0);
        return vertx.executeBlocking(() -> {
            try {
                List<Reply> replies = session.get(keyPrefix.mapPattern(mapName));
                int count = 0;
                for (Reply reply : replies) {
                    if (reply.success()) count++;
                }
                return count;
            } catch (Exception e) {
                logger.debug("Failed to get size from zenoh: {}", e.getMessage());
                return 0;
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Set<K>> keys() {
        if (session == null) return Future.succeededFuture(Set.of());
        String prefix = keyPrefix.mapKey(mapName, "");
        return vertx.executeBlocking(() -> {
            Set<K> result = new HashSet<>();
            try {
                List<Reply> replies = session.get(keyPrefix.mapPattern(mapName));
                for (Reply reply : replies) {
                    if (reply.success() && reply.sampleKeyExpr().isPresent()) {
                        String fullKey = reply.sampleKeyExpr().get();
                        if (fullKey.startsWith(prefix)) {
                            result.add((K) fullKey.substring(prefix.length()));
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to get keys from zenoh: {}", e.getMessage());
            }
            return result;
        });
    }

    @Override
    public Future<List<V>> values() {
        if (session == null) return Future.succeededFuture(List.of());
        return vertx.executeBlocking(() -> {
            List<V> result = new ArrayList<>();
            try {
                List<Reply> replies = session.get(keyPrefix.mapPattern(mapName));
                for (Reply reply : replies) {
                    if (reply.success() && reply.samplePayload().isPresent()) {
                        V v = deserialize(reply.samplePayload().get());
                        if (v != null) result.add(v);
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to get values from zenoh: {}", e.getMessage());
            }
            return result;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<K, V>> entries() {
        if (session == null) return Future.succeededFuture(Map.of());
        String prefix = keyPrefix.mapKey(mapName, "");
        return vertx.executeBlocking(() -> {
            Map<K, V> result = new HashMap<>();
            try {
                List<Reply> replies = session.get(keyPrefix.mapPattern(mapName));
                for (Reply reply : replies) {
                    if (reply.success() && reply.sampleKeyExpr().isPresent()
                            && reply.samplePayload().isPresent()) {
                        String fullKey = reply.sampleKeyExpr().get();
                        if (fullKey.startsWith(prefix)) {
                            K key = (K) fullKey.substring(prefix.length());
                            V value = deserialize(reply.samplePayload().get());
                            if (value != null) result.put(key, value);
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to get entries from zenoh: {}", e.getMessage());
            }
            return result;
        });
    }

    // --- Zenoh I/O helpers ---

    @SuppressWarnings("unchecked")
    private V querySingle(String key) {
        try {
            List<Reply> replies = session.get(key);
            for (Reply reply : replies) {
                if (reply.success() && reply.samplePayload().isPresent()) {
                    return (V) deserialize(reply.samplePayload().get());
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to query Zenoh for key '{}': {}", key, e.getMessage());
        }
        return null;
    }

    private void doPut(String key, byte[] data) {
        try {
            session.put(key, data);
        } catch (Exception e) {
            logger.warn("Failed to put to Zenoh '{}': {}", key, e.getMessage());
        }
    }

    private void doDelete(String key) {
        try {
            session.delete(key);
        } catch (Exception e) {
            logger.warn("Failed to delete from Zenoh '{}': {}", key, e.getMessage());
        }
    }

    // --- Serialization ---

    private String serializeKey(K key) {
        if (key instanceof String s) return s;
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
        if (data == null || data.length == 0) return null;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.debug("Deserialization failed: {}", e.getMessage());
            return null;
        }
    }
}
