package com.workoss.boot.vertx.data.dict.cache;

import com.workoss.boot.vertx.data.dict.entity.DictItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Two-level cache manager for dictionary data with concurrency safety and statistics.
 * <p>
 * Lookup order: L1 (local) -> L2 (distributed).<br>
 * Write order:  L2 (distributed) -> L1 (local).<br>
 * Evict order:  L1 (local) -> L2 (distributed).
 */
public class DictCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(DictCacheManager.class);

    private final DictCache l1Cache;
    private final DictCache l2Cache;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Statistics
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);

    public DictCacheManager(DictCache l1Cache, DictCache l2Cache) {
        this.l1Cache = l1Cache;
        this.l2Cache = l2Cache;
    }

    /**
     * Get a single item (L1 -> L2).
     */
    public Optional<DictItem> get(String type, String code) {
        // L1
        Optional<DictItem> result = l1Cache.get(type, code);
        if (result.isPresent()) {
            hits.incrementAndGet();
            return result;
        }

        // L2 with promotion
        if (l2Cache != null) {
            result = l2Cache.get(type, code);
            if (result.isPresent()) {
                l1Cache.put(type, code, result.get());
                hits.incrementAndGet();
                return result;
            }
        }

        misses.incrementAndGet();
        return Optional.empty();
    }

    /**
     * Put a single item into both levels.
     */
    public void put(String type, String code, DictItem item) {
        lock.readLock().lock();
        try {
            if (l2Cache != null) {
                l2Cache.put(type, code, item);
            }
            l1Cache.put(type, code, item);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all items of a type (L1 -> L2).
     */
    public Optional<List<DictItem>> getList(String type) {
        Optional<List<DictItem>> result = l1Cache.getList(type);
        if (result.isPresent()) {
            hits.incrementAndGet();
            return result;
        }

        if (l2Cache != null) {
            result = l2Cache.getList(type);
            if (result.isPresent()) {
                l1Cache.putList(type, result.get());
                hits.incrementAndGet();
                return result;
            }
        }

        misses.incrementAndGet();
        return Optional.empty();
    }

    /**
     * Put a list of items into both levels.
     */
    public void putList(String type, List<DictItem> items) {
        lock.readLock().lock();
        try {
            if (l2Cache != null) {
                l2Cache.putList(type, items);
            }
            l1Cache.putList(type, items);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Evict a single item from both levels.
     */
    public void evict(String type, String code) {
        lock.readLock().lock();
        try {
            l1Cache.evict(type, code);
            if (l2Cache != null) {
                l2Cache.evict(type, code);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Evict all items of a type from both levels.
     */
    public void evictType(String type) {
        lock.readLock().lock();
        try {
            l1Cache.evictType(type);
            if (l2Cache != null) {
                l2Cache.evictType(type);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Evict all items from both levels.
     */
    public void evictAll() {
        lock.writeLock().lock();
        try {
            l1Cache.evictAll();
            if (l2Cache != null) {
                l2Cache.evictAll();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reset statistics.
     */
    public void resetStats() {
        hits.set(0);
        misses.set(0);
    }

    /**
     * Get current hit count.
     */
    public long getHits() {
        return hits.get();
    }

    /**
     * Get current miss count.
     */
    public long getMisses() {
        return misses.get();
    }

    /**
     * Calculate hit rate (0.0 ~ 1.0).
     */
    public double getHitRate() {
        long total = hits.get() + misses.get();
        return total == 0 ? 0.0 : (double) hits.get() / total;
    }
}
