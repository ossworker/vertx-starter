package com.workoss.boot.vertx.web.server;

import io.vertx.core.MultiMap;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lazy {@link MultiValueMap} view backed by a Vert.x {@link MultiMap}.
 * <p>
 * Avoids the full copy of request headers performed eagerly by the previous
 * {@code buildHeaders()} implementation. Point lookups (get/getFirst/containsKey/size)
 * are delegated directly to the underlying Vert.x MultiMap with zero allocations.
 * Iteration (keySet/entrySet/values) materializes a cache on first access since
 * those paths typically inspect every header anyway.
 * <p>
 * Mutating operations trigger a one-time copy to a backing {@link LinkedMultiValueMap}
 * to keep the original Vert.x MultiMap immutable from the adapter's perspective.
 */
final class VertxHttpHeadersAdapter implements MultiValueMap<String, String> {

    private final MultiMap source;
    /** Lazy mutable copy for write operations (rare path). */
    private LinkedMultiValueMap<String, String> writableCopy;

    VertxHttpHeadersAdapter(MultiMap source) {
        this.source = source;
    }

    private MultiValueMap<String, String> writable() {
        if (writableCopy == null) {
            LinkedMultiValueMap<String, String> copy = new LinkedMultiValueMap<>(source.size());
            for (String name : source.names()) {
                copy.put(name, new ArrayList<>(source.getAll(name)));
            }
            writableCopy = copy;
        }
        return writableCopy;
    }

    private boolean isWritable() {
        return writableCopy != null;
    }

    // --- MultiValueMap specific ---

    @Override
    public String getFirst(String key) {
        if (isWritable()) return writableCopy.getFirst(key);
        return source.get(key);
    }

    @Override
    public void add(String key, String value) {
        writable().add(key, value);
    }

    @Override
    public void addAll(String key, List<? extends String> values) {
        writable().addAll(key, values);
    }

    @Override
    public void addAll(MultiValueMap<String, String> values) {
        writable().addAll(values);
    }

    @Override
    public void set(String key, String value) {
        writable().set(key, value);
    }

    @Override
    public void setAll(Map<String, String> values) {
        writable().setAll(values);
    }

    @Override
    public Map<String, String> toSingleValueMap() {
        if (isWritable()) return writableCopy.toSingleValueMap();
        Map<String, String> single = new LinkedHashMap<>(source.size());
        for (String name : source.names()) {
            single.put(name, source.get(name));
        }
        return single;
    }

    // --- Map<String, List<String>> ---

    @Override
    public int size() {
        return isWritable() ? writableCopy.size() : source.names().size();
    }

    @Override
    public boolean isEmpty() {
        return isWritable() ? writableCopy.isEmpty() : source.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (isWritable()) return writableCopy.containsKey(key);
        return key instanceof String s && source.contains(s);
    }

    @Override
    public boolean containsValue(Object value) {
        return writable().containsValue(value);
    }

    @Override
    public List<String> get(Object key) {
        if (isWritable()) return writableCopy.get(key);
        if (!(key instanceof String s) || !source.contains(s)) return null;
        return source.getAll(s);
    }

    @Override
    public List<String> put(String key, List<String> value) {
        return writable().put(key, value);
    }

    @Override
    public List<String> remove(Object key) {
        return writable().remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends List<String>> m) {
        writable().putAll(m);
    }

    @Override
    public void clear() {
        writable().clear();
    }

    @Override
    public Set<String> keySet() {
        if (isWritable()) return writableCopy.keySet();
        return new LinkedHashSet<>(source.names());
    }

    @Override
    public Collection<List<String>> values() {
        return writable().values();
    }

    @Override
    public Set<Map.Entry<String, List<String>>> entrySet() {
        if (isWritable()) return writableCopy.entrySet();
        // Snapshot view: rare path, materialize once
        Set<String> names = source.names();
        return new AbstractSet<>() {
            @Override
            public Iterator<Map.Entry<String, List<String>>> iterator() {
                Iterator<String> it = names.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Map.Entry<String, List<String>> next() {
                        String name = it.next();
                        return new AbstractMap.SimpleImmutableEntry<>(name, source.getAll(name));
                    }
                };
            }

            @Override
            public int size() {
                return names.size();
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Map<?, ?> other)) return false;
        return entrySet().equals(other.entrySet());
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public String toString() {
        return isWritable() ? writableCopy.toString() : Collections.unmodifiableSet(source.names()).toString();
    }
}
