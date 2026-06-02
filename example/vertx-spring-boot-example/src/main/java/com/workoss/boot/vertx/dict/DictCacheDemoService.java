package com.workoss.boot.vertx.dict;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Demonstrates Spring Cache annotations with two-level cache (Caffeine + Zenoh).
 * <p>
 * When dict cache is enabled, these methods will use CaffeineZenohCacheManager
 * as the underlying CacheManager.
 */
@Slf4j
@Service
public class DictCacheDemoService {

    /**
     * Cacheable method: result is cached in L1 (Caffeine) and L2 (Zenoh).
     * On subsequent calls with the same key, the cached value is returned
     * without executing the method body.
     */
    @Cacheable(value = "demo", key = "'region:' + #regionCode")
    public String getRegionName(String regionCode) {
        log.info("Computing region name for code={} (cache miss)", regionCode);
        // Simulate expensive DB lookup
        simulateDbLatency();
        return switch (regionCode.toUpperCase()) {
            case "CN" -> "中国";
            case "US" -> "美国";
            case "BJ" -> "北京";
            case "SH" -> "上海";
            case "GD" -> "广东";
            case "SZ" -> "深圳";
            case "GZ" -> "广州";
            default -> "未知";
        };
    }

    /**
     * Cacheable with TTL-like behavior using condition.
     * Only caches results for specific region codes.
     */
    @Cacheable(value = "demo", key = "'vip:' + #regionCode", condition = "#regionCode.startsWith('V')")
    public String getVipRegionName(String regionCode) {
        log.info("Computing VIP region name for code={} (cache miss)", regionCode);
        simulateDbLatency();
        return "VIP-" + regionCode;
    }

    /**
     * CacheEvict: removes the cached entry.
     * Called after data mutations to keep cache consistent.
     */
    @CacheEvict(value = "demo", key = "'region:' + #regionCode")
    public void invalidateRegionCache(String regionCode) {
        log.info("Invalidating cache for region={}", regionCode);
    }

    /**
     * CacheEvict with allEntries=true: clears entire cache.
     * Useful for bulk updates or admin operations.
     */
    @CacheEvict(value = "demo", allEntries = true)
    public void clearAllDemoCache() {
        log.info("Clearing all demo cache entries");
    }

    private void simulateDbLatency() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(50, 200));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
