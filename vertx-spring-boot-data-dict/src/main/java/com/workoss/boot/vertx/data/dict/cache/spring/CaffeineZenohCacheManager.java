package com.workoss.boot.vertx.data.dict.cache.spring;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Two-level {@link CacheManager} combining Caffeine (L1) and Zenoh (L2).
 * <p>
 * Read order: Caffeine -> Zenoh -> DB
 * Write order: DB -> Zenoh -> Caffeine
 * Evict order: Caffeine -> Zenoh
 */
public class CaffeineZenohCacheManager implements CacheManager {

    private final CaffeineCacheManager caffeineCacheManager;
    private final ZenohCacheManager zenohCacheManager;

    public CaffeineZenohCacheManager(CaffeineCacheManager caffeineCacheManager, ZenohCacheManager zenohCacheManager) {
        this.caffeineCacheManager = caffeineCacheManager;
        this.zenohCacheManager = zenohCacheManager;
    }

    @Override
    public Cache getCache(String name) {
        Cache l1 = caffeineCacheManager.getCache(name);
        Cache l2 = zenohCacheManager != null ? zenohCacheManager.getCache(name) : null;
        return new CaffeineZenohCache(name, l1, l2);
    }

    @Override
    public Collection<String> getCacheNames() {
        Set<String> names = new HashSet<>(caffeineCacheManager.getCacheNames());
        if (zenohCacheManager != null) {
            names.addAll(zenohCacheManager.getCacheNames());
        }
        return names;
    }
}
