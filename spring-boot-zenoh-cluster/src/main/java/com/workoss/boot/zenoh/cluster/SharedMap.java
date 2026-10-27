package com.workoss.boot.zenoh.cluster;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cluster-wide map replicated to every node.
 * <p>
 * Reads are local memory lookups. Conditional writes ({@code putIfAbsent}, {@code remove(k, v)},
 * {@code replace}) are atomic against other threads on this node; across nodes, concurrent writes to
 * the same key converge on a single winner chosen by last-writer-wins.
 * <p>
 * {@link String} keys are stored as-is (escaped); other keys must be {@link java.io.Serializable}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SharedMap<K, V> {

    String name();

    /**
     * @return the value, or {@code null} when absent or expired
     */
    V get(K key);

    boolean containsKey(K key);

    int size();

    boolean isEmpty();

    Set<K> keySet();

    List<V> values();

    Map<K, V> entries();

    void put(K key, V value);

    /**
     * @param ttl time-to-live; {@code null}, zero or negative means no expiry
     */
    void put(K key, V value, Duration ttl);

    /**
     * @return the value already present, or {@code null} if this call stored {@code value}
     */
    V putIfAbsent(K key, V value);

    /**
     * @return the removed value, or {@code null} when absent
     */
    V remove(K key);

    /**
     * Removes the entry only while it maps to {@code value}.
     */
    boolean remove(K key, V value);

    /**
     * Replaces the value of an existing entry, keeping its expiry.
     *
     * @return the previous value, or {@code null} when absent (nothing is stored then)
     */
    V replace(K key, V value);

    /**
     * Replaces the value only while the entry maps to {@code oldValue}, keeping its expiry.
     */
    boolean replace(K key, V oldValue, V newValue);

    void clear();
}
