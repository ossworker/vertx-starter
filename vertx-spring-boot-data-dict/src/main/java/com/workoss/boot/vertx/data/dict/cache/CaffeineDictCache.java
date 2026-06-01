package com.workoss.boot.vertx.data.dict.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.workoss.boot.vertx.data.dict.entity.DictItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * L1 Cache implementation using Caffeine (local in-memory cache).
 */
public class CaffeineDictCache implements DictCache {

    private static final Logger logger = LoggerFactory.getLogger(CaffeineDictCache.class);

    private final Cache<String, DictItem> itemCache;
    private final Cache<String, List<DictItem>> listCache;

    public CaffeineDictCache(int maximumSize, int expireSeconds) {
        this.itemCache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .recordStats()
                .build();
        this.listCache = Caffeine.newBuilder()
                .maximumSize(Math.max(1, maximumSize / 10))
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .recordStats()
                .build();
    }

    @Override
    public Optional<DictItem> get(String type, String code) {
        DictItem item = itemCache.getIfPresent(buildItemKey(type, code));
        if (item != null) {
            logger.debug("L1 cache hit for {}/{}", type, code);
        }
        return Optional.ofNullable(item);
    }

    @Override
    public void put(String type, String code, DictItem item) {
        itemCache.put(buildItemKey(type, code), item);
        logger.debug("L1 cache put {}/{}", type, code);
    }

    @Override
    public Optional<List<DictItem>> getList(String type) {
        return Optional.ofNullable(listCache.getIfPresent(type));
    }

    @Override
    public void putList(String type, List<DictItem> items) {
        listCache.put(type, List.copyOf(items));
        logger.debug("L1 cache put list for type={}", type);
    }

    @Override
    public void evict(String type, String code) {
        itemCache.invalidate(buildItemKey(type, code));
        logger.debug("L1 cache evict {}/{}", type, code);
    }

    @Override
    public void evictType(String type) {
        listCache.invalidate(type);
        logger.debug("L1 cache evict type={}", type);
    }

    @Override
    public void evictAll() {
        itemCache.invalidateAll();
        listCache.invalidateAll();
        logger.info("L1 cache evicted all");
    }

    private static String buildItemKey(String type, String code) {
        return type + ":" + code;
    }
}
