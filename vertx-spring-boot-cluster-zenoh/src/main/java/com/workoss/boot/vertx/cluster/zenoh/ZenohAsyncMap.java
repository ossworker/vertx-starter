package com.workoss.boot.vertx.cluster.zenoh;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.zenoh.Session;
import io.zenoh.bytes.ZBytes;
import io.zenoh.exceptions.ZError;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.query.Reply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * AsyncMap implementation that directly queries Zenoh storage backend.
 * <p>
 * No local cache. All read/write operations go through Zenoh session:
 * <ul>
 *   <li>put → session.put() (write to storage backend)</li>
 *   <li>get → session.get() (query from storage backend)</li>
 *   <li>remove → session.delete() (delete from storage backend)</li>
 * </ul>
 * Requires a storage backend (e.g. filesystem) to be configured for data persistence.
 */
public class ZenohAsyncMap<K, V> implements AsyncMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ZenohAsyncMap.class);

    private static final long QUERY_TIMEOUT_SECONDS = 5;

    private final String mapName;
    private final KeyPrefixStrategy keyPrefix;
    private final Session session;


    public ZenohAsyncMap(String mapName, KeyPrefixStrategy keyPrefix, Session session) {
        this.mapName = mapName;
        this.keyPrefix = keyPrefix;
        this.session = session;
    }

    @Override
    public Future<V> get(K k) {
        if (session == null) {
            return Future.succeededFuture(null);
        }
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> querySingle(key))
        );
    }

    @Override
    public Future<Void> put(K k, V v) {
        if (session == null) return Future.succeededFuture();
        String key = keyPrefix.mapKey(mapName, serializeKey(k));
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    doPut(key, serialize(v));
                    return null;
                })
        );
    }

    @Override
    public Future<Void> put(K k, V v, long ttl) {
        return put(k, v);
    }

    @Override
    public Future<V> putIfAbsent(K k, V v) {
        if (session == null) return Future.succeededFuture(null);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    String entryKey = serializeKey(k);
                    String key = keyPrefix.mapKey(mapName, entryKey);
                    V existing = querySingle(key);
                    if (existing != null) return existing;
                    doPut(key, serialize(v));
                    return null;
                })
        );
    }

    @Override
    public Future<V> putIfAbsent(K k, V v, long ttl) {
        return putIfAbsent(k, v);
    }

    @Override
    public Future<V> remove(K k) {
        if (session == null) return Future.succeededFuture(null);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    String entryKey = serializeKey(k);
                    String key = keyPrefix.mapKey(mapName, entryKey);
                    V old = querySingle(key);
                    doDelete(key);
                    return old;
                })
        );
    }

    @Override
    public Future<Boolean> removeIfPresent(K k, V v) {
        if (session == null) return Future.succeededFuture(false);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    String entryKey = serializeKey(k);
                    String key = keyPrefix.mapKey(mapName, entryKey);
                    V current = querySingle(key);
                    if (current != null && current.equals(v)) {
                        doDelete(key);
                        return true;
                    }
                    return false;
                })
        );
    }

    @Override
    public Future<V> replace(K k, V v) {
        if (session == null) return Future.succeededFuture(null);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    String entryKey = serializeKey(k);
                    String key = keyPrefix.mapKey(mapName, entryKey);
                    V current = querySingle(key);
                    if (current != null) {
                        doPut(key, serialize(v));
                        return current;
                    }
                    return null;
                })
        );
    }

    @Override
    public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
        if (session == null) return Future.succeededFuture(false);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    String entryKey = serializeKey(k);
                    String key = keyPrefix.mapKey(mapName, entryKey);
                    V current = querySingle(key);
                    if (current != null && current.equals(oldValue)) {
                        doPut(key, serialize(newValue));
                        return true;
                    }
                    return false;
                })
        );
    }

    @Override
    public Future<Void> clear() {
        if (session == null) return Future.succeededFuture();
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    String pattern = keyPrefix.mapPattern(mapName);
                    List<String> keys = queryKeys(pattern);
                    for (String key : keys) {
                        doDelete(key);
                    }
                    return null;
                })
        );
    }

    @Override
    public Future<Integer> size() {
        if (session == null) return Future.succeededFuture(0);
        String pattern = keyPrefix.mapPattern(mapName);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> queryKeys(pattern).size())
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Set<K>> keys() {
        if (session == null) return Future.succeededFuture(Set.of());
        String pattern = keyPrefix.mapPattern(mapName);
        String mapsPrefix = keyPrefix.getPrefix() + "/maps/";
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    List<String> fullKeys = queryKeys(pattern);
                    Set<K> result = new HashSet<>();
                    for (String fullKey : fullKeys) {
                        String remainder = fullKey.substring(mapsPrefix.length());
                        int slashIdx = remainder.indexOf('/');
                        if (slashIdx > 0) {
                            result.add((K) remainder.substring(slashIdx + 1));
                        }
                    }
                    return result;
                })
        );
    }

    @Override
    public Future<List<V>> values() {
        if (session == null) return Future.succeededFuture(List.of());
        String pattern = keyPrefix.mapPattern(mapName);
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    List<byte[]> rawValues = queryAll(pattern);
                    List<V> result = new ArrayList<>();
                    for (byte[] data : rawValues) {
                        V v = deserialize(data);
                        if (v != null) result.add(v);
                    }
                    return result;
                })
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<K, V>> entries() {
        if (session == null) return Future.succeededFuture(Map.of());
        String pattern = keyPrefix.mapPattern(mapName);
        String mapsPrefix = keyPrefix.getPrefix() + "/maps/";
        return Future.fromCompletionStage(
                CompletableFuture.supplyAsync(() -> {
                    Map<K, V> result = new HashMap<>();
                    var replies = doQuery(pattern);
                    if (replies == null) return result;
                    try {
                        Optional<Reply> opt;
                        while ((opt = replies.poll(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) != null) {
                            if (opt.isEmpty()) break;
                            Reply reply = opt.get();
                            if (reply instanceof Reply.Success success) {
                                var sample = success.getSample();
                                String fullKey = sample.getKeyExpr().toString();
                                String remainder = fullKey.substring(mapsPrefix.length());
                                int slashIdx = remainder.indexOf('/');
                                if (slashIdx > 0) {
                                    K key = (K) remainder.substring(slashIdx + 1);
                                    V value = deserialize(sample.getPayload().toBytes());
                                    if (value != null) result.put(key, value);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        replies.clear();
                    }
                    return result;
                })
        );
    }

    // --- Zenoh I/O helpers ---

    /**
     * Query a single value from Zenoh storage.
     */
    @SuppressWarnings("unchecked")
    private V querySingle(String key) {
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            var replies = session.get(keyExpr);
            try {
                var opt = replies.poll(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (opt != null && opt.isPresent()) {
                    Reply reply = opt.get();
                    if (reply instanceof Reply.Success success) {
                        byte[] data = success.getSample().getPayload().toBytes();
                        return (V) deserialize(data);
                    }
                }
            } finally {
                replies.clear();
            }
            keyExpr.close();
        } catch (Exception e) {
            logger.debug("Failed to query Zenoh for key '{}': {}", key, e.getMessage());
        }
        return null;
    }

    /**
     * Query all matching key names.
     */
    private List<String> queryKeys(String pattern) {
        List<String> keys = new ArrayList<>();
        var replies = doQuery(pattern);
        if (replies == null) return keys;
        try {
            Optional<Reply> opt;
            while ((opt = replies.poll(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) != null) {
                if (opt.isEmpty()) break;
                Reply reply = opt.get();
                if (reply instanceof Reply.Success success) {
                    keys.add(success.getSample().getKeyExpr().toString());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            replies.clear();
        }
        return keys;
    }

    /**
     * Query all matching values (raw bytes).
     */
    private List<byte[]> queryAll(String pattern) {
        List<byte[]> values = new ArrayList<>();
        var replies = doQuery(pattern);
        if (replies == null) return values;
        try {
            Optional<Reply> opt;
            while ((opt = replies.poll(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) != null) {
                if (opt.isEmpty()) break;
                Reply reply = opt.get();
                if (reply instanceof Reply.Success success) {
                    values.add(success.getSample().getPayload().toBytes());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            replies.clear();
        }
        return values;
    }

    /**
     * Execute a Zenoh get query, returning the replies queue.
     */
    private java.util.concurrent.BlockingQueue<Optional<Reply>> doQuery(String pattern) {
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(pattern);
            var replies = session.get(keyExpr);
            keyExpr.close();
            return replies;
        } catch (Exception e) {
            logger.debug("Failed to query Zenoh pattern '{}': {}", pattern, e.getMessage());
            return null;
        }
    }

    private void doPut(String key, byte[] data) {
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            session.put(keyExpr, ZBytes.from(data));
            keyExpr.close();
        } catch (ZError e) {
            logger.warn("Failed to put to Zenoh '{}': {}", key, e.getMessage());
        }
    }

    private void doDelete(String key) {
        try {
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            session.delete(keyExpr);
            keyExpr.close();
        } catch (ZError e) {
            logger.warn("Failed to delete from Zenoh '{}': {}", key, e.getMessage());
        }
    }

    // --- Serialization ---

    private String serializeKey(K key) {
        if (key instanceof String s) {
            return s;
        }
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
