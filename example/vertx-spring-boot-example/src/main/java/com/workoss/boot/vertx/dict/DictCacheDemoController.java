package com.workoss.boot.vertx.dict;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST API demonstrating Spring Cache annotations with two-level cache.
 */
@Slf4j
@RestController
@RequestMapping("/api/cache/demo")
@RequiredArgsConstructor
public class DictCacheDemoController {

    private final DictCacheDemoService demoService;

    /**
     * Demonstrate @Cacheable - first call is a cache miss, subsequent calls are hits.
     */
    @GetMapping("/region/{code}")
    public Map<String, String> getRegion(@PathVariable String code) {
        long start = System.currentTimeMillis();
        String name = demoService.getRegionName(code);
        long elapsed = System.currentTimeMillis() - start;
        log.info("GET /api/cache/demo/region/{} -> {} ({}ms)", code, name, elapsed);

        Map<String, String> result = new HashMap<>();
        result.put("code", code);
        result.put("name", name);
        result.put("elapsedMs", String.valueOf(elapsed));
        return result;
    }

    /**
     * Demonstrate @CacheEvict - invalidate a single entry.
     */
    @DeleteMapping("/region/{code}")
    public Map<String, String> evictRegion(@PathVariable String code) {
        demoService.invalidateRegionCache(code);
        Map<String, String> result = new HashMap<>();
        result.put("action", "evicted");
        result.put("code", code);
        return result;
    }

    /**
     * Demonstrate @CacheEvict(allEntries=true) - clear entire cache.
     */
    @DeleteMapping("/clear")
    public Map<String, String> clearAll() {
        demoService.clearAllDemoCache();
        Map<String, String> result = new HashMap<>();
        result.put("action", "cleared");
        result.put("scope", "all");
        return result;
    }
}
