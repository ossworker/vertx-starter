package com.workoss.boot.zenoh.cluster.internal.store;

import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.internal.BuiltinCodecs;
import com.workoss.boot.zenoh.cluster.internal.KeyEscaper;
import com.workoss.boot.zenoh.cluster.internal.KeySpace;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Entry keys are typed so the encoding is reversible: {@code s} + escaped text for {@link String}
 * keys, {@code j} + Base64url of the JDK serialization for anything else.
 */
public final class DefaultSharedMap<K, V> implements SharedMap<K, V> {

    private final String name;
    private final String namespace;
    private final Supplier<ReplicatedStore> store;
    private final ValueCodec<V> valueCodec;

    public DefaultSharedMap(String name, Supplier<ReplicatedStore> store, ValueCodec<V> valueCodec) {
        this.name = name;
        this.namespace = KeySpace.mapNamespace(name);
        this.store = store;
        this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec");
    }

    public ValueCodec<V> valueCodec() {
        return valueCodec;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public V get(K key) {
        return decode(store.get().get(namespace, encodeKey(key)));
    }

    @Override
    public boolean containsKey(K key) {
        return store.get().get(namespace, encodeKey(key)) != null;
    }

    @Override
    public int size() {
        return store.get().size(namespace);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Set<K> keySet() {
        Set<K> keys = new HashSet<>();
        for (String encoded : store.get().entries(namespace).keySet()) {
            keys.add(decodeKey(encoded));
        }
        return keys;
    }

    @Override
    public List<V> values() {
        List<V> values = new ArrayList<>();
        for (byte[] raw : store.get().entries(namespace).values()) {
            V value = decode(raw);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public Map<K, V> entries() {
        Map<K, V> result = new HashMap<>();
        store.get().entries(namespace).forEach((encoded, raw) -> {
            V value = decode(raw);
            if (value != null) {
                result.put(decodeKey(encoded), value);
            }
        });
        return result;
    }

    @Override
    public void put(K key, V value) {
        put(key, value, null);
    }

    @Override
    public void put(K key, V value, Duration ttl) {
        byte[] encoded = encode(value);
        long ttlMillis = ttl == null ? 0L : ttl.toMillis();
        store.get().update(namespace, encodeKey(key), current -> Change.put(encoded, ttlMillis));
    }

    @Override
    public V putIfAbsent(K key, V value) {
        byte[] encoded = encode(value);
        ReplicatedStore.Result result = store.get().update(namespace, encodeKey(key),
                current -> current == null ? Change.put(encoded) : Change.KEEP);
        return decode(result.previous());
    }

    @Override
    public V remove(K key) {
        ReplicatedStore.Result result = store.get().update(namespace, encodeKey(key),
                current -> current == null ? Change.KEEP : Change.DELETE);
        return decode(result.previous());
    }

    @Override
    public boolean remove(K key, V value) {
        return store.get().update(namespace, encodeKey(key),
                current -> current != null && Objects.equals(decode(current), value) ? Change.DELETE : Change.KEEP)
                .changed();
    }

    @Override
    public V replace(K key, V value) {
        byte[] encoded = encode(value);
        ReplicatedStore.Result result = store.get().update(namespace, encodeKey(key),
                current -> current == null ? Change.KEEP : Change.replace(encoded));
        return decode(result.previous());
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        byte[] encoded = encode(newValue);
        // Decoded comparison: JDK serialization is not canonical (e.g. HashMap field order), so
        // equal values may have different bytes.
        return store.get().update(namespace, encodeKey(key),
                current -> current != null && Objects.equals(decode(current), oldValue)
                        ? Change.replace(encoded) : Change.KEEP)
                .changed();
    }

    @Override
    public void clear() {
        store.get().clear(namespace);
    }

    private byte[] encode(V value) {
        return valueCodec.encode(Objects.requireNonNull(value, "Shared map values must not be null"));
    }

    private V decode(byte[] raw) {
        return raw == null ? null : valueCodec.decode(raw);
    }

    private String encodeKey(K key) {
        Objects.requireNonNull(key, "Shared map keys must not be null");
        if (key instanceof String text) {
            return "s" + KeyEscaper.escapeAllowEmpty(text);
        }
        return "j" + Base64.getUrlEncoder().withoutPadding().encodeToString(BuiltinCodecs.serialize(key));
    }

    @SuppressWarnings("unchecked")
    private K decodeKey(String encoded) {
        String body = encoded.substring(1);
        if (encoded.charAt(0) == 's') {
            return (K) KeyEscaper.unescape(body);
        }
        try {
            return (K) BuiltinCodecs.deserialize(Base64.getUrlDecoder().decode(body));
        } catch (Exception e) {
            throw new IllegalStateException("Undecodable key in shared map '" + name + "': " + encoded, e);
        }
    }
}
