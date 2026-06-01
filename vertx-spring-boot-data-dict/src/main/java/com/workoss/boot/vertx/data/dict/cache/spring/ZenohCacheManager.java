package com.workoss.boot.vertx.data.dict.cache.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zenoh.Session;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Spring {@link CacheManager} implementation backed by Zenoh.
 * <p>
 * Creates {@link ZenohCache} instances for each cache name requested.
 */
public class ZenohCacheManager implements CacheManager {

    private final Supplier<Session> sessionSupplier;
    private final String keyPrefix;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>();

    public ZenohCacheManager(Supplier<Session> sessionSupplier, String keyPrefix, ObjectMapper objectMapper) {
        this.sessionSupplier = sessionSupplier;
        this.keyPrefix = keyPrefix;
        this.objectMapper = objectMapper;
    }

    @Override
    public Cache getCache(String name) {
        return caches.computeIfAbsent(name, n -> new ZenohCache(n, sessionSupplier, keyPrefix, objectMapper));
    }

    @Override
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(caches.keySet());
    }
}
