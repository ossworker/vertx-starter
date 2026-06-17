package com.workoss.boot.vertx.cluster;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.SharedData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.Resource;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * REST controller demonstrating cluster shared data (AsyncMap) operations.
 * Data is persisted through Zenoh storage backend (filesystem).
 * <p>
 * Returns CompletionStage to avoid blocking the Vert.x event loop thread.
 */
@RestController
@RequestMapping("/cluster")
@Slf4j
public class ClusterDataController {

    @Resource
    private Vertx vertx;

    /**
     * Put a key-value pair into the cluster shared map.
     * Data is persisted to the Zenoh storage backend.
     *
     * GET /cluster/set?map=myMap&key=k1&value=v1
     */
    @GetMapping("/set")
    public CompletionStage<Map<String, Object>> set(
            @RequestParam(defaultValue = "default") String map,
            @RequestParam String key,
            @RequestParam String value) {
        SharedData sharedData = vertx.sharedData();
        return sharedData.<String, String>getAsyncMap(map)
                .compose(asyncMap -> asyncMap.put(key, value))
                .<Map<String, Object>>map(v -> {
                    log.info("Cluster set: map={}, key={}, value={}", map, key, value);
                    return Map.of("status", "ok", "map", map, "key", key, "value", value);
                })
                .otherwise(e -> {
                    log.error("Cluster set failed", e);
                    return Map.of("status", "error", "message", e.getMessage() != null ? e.getMessage() : e.toString());
                })
                .toCompletionStage();
    }

    /**
     * Get a value from the cluster shared map.
     * Data is read from the Zenoh storage backend.
     *
     * GET /cluster/get?map=myMap&key=k1
     */
    @GetMapping("/get")
    public CompletionStage<Map<String, Object>> get(
            @RequestParam(defaultValue = "default") String map,
            @RequestParam String key) {
        SharedData sharedData = vertx.sharedData();
        return sharedData.<String, String>getAsyncMap(map)
                .compose(asyncMap -> asyncMap.get(key))
                .<Map<String, Object>>map(value -> {
                    log.info("Cluster get: map={}, key={}, value={}", map, key, value);
                    if (value != null) {
                        return Map.of("status", "ok", "map", map, "key", key, "value", value);
                    } else {
                        return Map.of("status", "ok", "map", map, "key", key, "value", "null (not found)");
                    }
                })
                .otherwise(e -> {
                    log.error("Cluster get failed", e);
                    return Map.of("status", "error", "message", e.getMessage() != null ? e.getMessage() : e.toString());
                })
                .toCompletionStage();
    }

    /**
     * Delete a key from the cluster shared map.
     *
     * GET /cluster/delete?map=myMap&key=k1
     */
    @GetMapping("/delete")
    public CompletionStage<Map<String, Object>> delete(
            @RequestParam(defaultValue = "default") String map,
            @RequestParam String key) {
        SharedData sharedData = vertx.sharedData();
        return sharedData.<String, String>getAsyncMap(map)
                .compose(asyncMap -> asyncMap.remove(key))
                .<Map<String, Object>>map(removed -> {
                    log.info("Cluster delete: map={}, key={}, removed={}", map, key, removed);
                    return Map.<String, Object>of("status", "ok", "map", map, "key", key,
                            "removed", removed != null ? removed : "null");
                })
                .otherwise(e -> {
                    log.error("Cluster delete failed", e);
                    return Map.of("status", "error", "message", e.getMessage() != null ? e.getMessage() : e.toString());
                })
                .toCompletionStage();
    }

    /**
     * Get all entries from the cluster shared map.
     *
     * GET /cluster/entries?map=myMap
     */
    @GetMapping("/entries")
    public CompletionStage<Map<String, Object>> entries(@RequestParam(defaultValue = "default") String map) {
        SharedData sharedData = vertx.sharedData();
        return sharedData.<String, String>getAsyncMap(map)
                .compose(asyncMap -> asyncMap.entries())
                .<Map<String, Object>>map(entries -> {
                    log.info("Cluster entries: map={}, count={}", map, entries.size());
                    return Map.<String, Object>of("status", "ok", "map", map, "count", String.valueOf(entries.size()),
                            "entries", entries);
                })
                .otherwise(e -> {
                    log.error("Cluster entries failed", e);
                    return Map.of("status", "error", "message", e.getMessage() != null ? e.getMessage() : e.toString());
                })
                .toCompletionStage();
    }
}
