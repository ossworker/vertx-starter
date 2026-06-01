package com.workoss.boot.vertx.data.dict.cache.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;

import java.util.concurrent.Callable;

/**
 * Two-level {@link Cache} combining Caffeine (L1) and Zenoh (L2).
 * <p>
 * Read order: Caffeine -> Zenoh -> DB (via valueLoader)
 * Write order: L1 -> L2
 * Evict order: L1 -> L2
 */
public class CaffeineZenohCache implements Cache {

    private static final Logger logger = LoggerFactory.getLogger(CaffeineZenohCache.class);

    private final String name;
    private final Cache l1Cache;
    private final Cache l2Cache;

    public CaffeineZenohCache(String name, Cache l1Cache, Cache l2Cache) {
        this.name = name;
        this.l1Cache = l1Cache;
        this.l2Cache = l2Cache;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return l1Cache != null ? l1Cache.getNativeCache() : null;
    }

    @Override
    public ValueWrapper get(Object key) {
        // L1 first
        if (l1Cache != null) {
            ValueWrapper wrapper = l1Cache.get(key);
            if (wrapper != null) {
                logger.debug("L1 cache hit for name={}, key={}", name, key);
                return wrapper;
            }
        }

        // L2 fallback
        if (l2Cache != null) {
            ValueWrapper wrapper = l2Cache.get(key);
            if (wrapper != null) {
                logger.debug("L2 cache hit for name={}, key={}", name, key);
                // Promote to L1
                if (l1Cache != null) {
                    l1Cache.put(key, wrapper.get());
                }
                return wrapper;
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Class<T> type) {
        ValueWrapper wrapper = get(key);
        if (wrapper != null) {
            Object value = wrapper.get();
            if (value != null && type.isInstance(value)) {
                return (T) value;
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        ValueWrapper wrapper = get(key);
        if (wrapper != null) {
            return (T) wrapper.get();
        }
        try {
            T value = valueLoader.call();
            put(key, value);
            return value;
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    @Override
    public void put(Object key, Object value) {
        // Write L2 first, then L1
        if (l2Cache != null) {
            try {
                l2Cache.put(key, value);
            } catch (Exception e) {
                logger.warn("Failed to write to L2 cache: {}", e.getMessage());
            }
        }
        if (l1Cache != null) {
            l1Cache.put(key, value);
        }
    }

    @Override
    public void evict(Object key) {
        if (l1Cache != null) {
            l1Cache.evict(key);
        }
        if (l2Cache != null) {
            l2Cache.evict(key);
        }
    }

    @Override
    public void clear() {
        if (l1Cache != null) {
            l1Cache.clear();
        }
        if (l2Cache != null) {
            l2Cache.clear();
        }
    }
}
