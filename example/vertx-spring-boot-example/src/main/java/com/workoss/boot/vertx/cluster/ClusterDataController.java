package com.workoss.boot.vertx.cluster;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.shareddata.SharedData;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * REST controller demonstrating cluster shared data across Vert.x nodes.
 * <p>
 * Three kinds of shared state are exposed, all backed by the Zenoh cluster manager so they are
 * visible to every node in the cluster (node A, node B, and any late-joining node C):
 * <ul>
 *   <li>{@code AsyncMap} — {@code /set}, {@code /get}, {@code /delete}, {@code /entries};</li>
 *   <li>{@code Counter} (cluster-wide atomic counter) — {@code /count/inc}, {@code /count/get};</li>
 *   <li>{@code Lock} (cluster-wide distributed lock) — {@code /lock/with}.</li>
 * </ul>
 * Every response carries the {@code node} id of the node that served it, and {@code /nodes} reports
 * the live cluster membership, so you can prove the cross-node behaviour: start node A and node B,
 * {@code A /set} then {@code B /get} returns the value; start node C afterwards and {@code C /get}
 * still returns it (late joiner bootstraps the existing map from a peer queryable).
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
                    return Map.of("status", "ok", "map", map, "key", key, "value", value,
                            "node", currentNodeId());
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
                        return Map.of("status", "ok", "map", map, "key", key, "value", value,
                                "node", currentNodeId());
                    } else {
                        return Map.of("status", "ok", "map", map, "key", key, "value", "null (not found)",
                                "node", currentNodeId());
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
                            "removed", removed != null ? removed : "null",
                            "node", currentNodeId());
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
                            "entries", entries, "node", currentNodeId());
                })
                .otherwise(e -> {
                    log.error("Cluster entries failed", e);
                    return Map.of("status", "error", "message", e.getMessage() != null ? e.getMessage() : e.toString());
                })
                .toCompletionStage();
    }

    /**
     * Increment a cluster-wide shared counter (Counter backed by the Zenoh cluster manager).
     * The counter is shared across all nodes A/B/C in the cluster, so a {@code /count/inc} on node A
     * is observed by {@code /count/get} on node B or C.
     *
     * GET /cluster/count/inc?name=hits
     */
    @GetMapping("/count/inc")
    public CompletionStage<Map<String, Object>> countInc(
            @RequestParam(defaultValue = "default") String name) {
        return vertx.sharedData().getCounter(name)
                .compose(Counter::incrementAndGet)
                .<Map<String, Object>>map(value -> Map.of("status", "ok", "counter", name,
                        "value", String.valueOf(value), "node", currentNodeId()))
                .otherwise(this::errorMap)
                .toCompletionStage();
    }

    /**
     * Read a cluster-wide shared counter.
     *
     * GET /cluster/count/get?name=hits
     */
    @GetMapping("/count/get")
    public CompletionStage<Map<String, Object>> countGet(
            @RequestParam(defaultValue = "default") String name) {
        return vertx.sharedData().getCounter(name)
                .compose(Counter::get)
                .<Map<String, Object>>map(value -> Map.of("status", "ok", "counter", name,
                        "value", String.valueOf(value), "node", currentNodeId()))
                .otherwise(this::errorMap)
                .toCompletionStage();
    }

    /**
     * Acquire a cluster-wide distributed lock, hold it for {@code ms} milliseconds (simulating a
     * critical section), then release it. Fire this on node A and node B at the same time: because
     * the lock is shared across the cluster, only one node holds it at a time — the other blocks
     * inside {@code getLock} until the first releases. That serialisation proves the lock works
     * across nodes. For a contended variant use {@code getLockWithTimeout}.
     *
     * GET /cluster/lock/with?name=myLock&ms=500
     */
    @GetMapping("/lock/with")
    public CompletionStage<Map<String, Object>> lockWith(
            @RequestParam(defaultValue = "default") String name,
            @RequestParam(defaultValue = "500") long ms) {
        String nodeId = currentNodeId();
        return vertx.sharedData().getLock(name)
                .compose(lock -> {
                    long acquiredAt = System.currentTimeMillis();
                    log.info("Lock acquired: name={}, node={}", name, nodeId);
                    Promise<Void> released = Promise.promise();
                    vertx.setTimer(Math.min(ms, 5000L), id -> {
                        lock.release();
                        log.info("Lock released: name={}, node={}, heldMs={}", name, nodeId,
                                System.currentTimeMillis() - acquiredAt);
                        released.complete();
                    });
                    return released.future();
                })
                .<Map<String, Object>>map(v -> Map.of("status", "ok", "node", nodeId,
                        "lock", name, "heldMs", String.valueOf(ms)))
                .otherwise(this::errorMap)
                .toCompletionStage();
    }

    /**
     * Show cluster membership: this node's id plus the full set of node ids currently in the
     * cluster. Use it to confirm that node A and node B (and any late-joining node C) have all
     * discovered each other. A value of {@code clustered=false} means Vert.x was not started in
     * clustered mode (the shared state would then be local only).
     *
     * GET /cluster/nodes
     */
    @GetMapping("/nodes")
    public Map<String, Object> nodes() {
        ClusterManager cm = clusterManager();
        if (cm == null) {
            return Map.of("status", "ok", "clustered", false);
        }
        List<String> members = cm.getNodes();
        return Map.of("status", "ok", "clustered", true, "nodeId", cm.getNodeId(),
                "memberCount", members.size(), "nodes", members);
    }

    // --- helpers ---

    /**
     * Vert.x 5 removed {@code getClusterManager()} from the public {@link Vertx} interface; the
     * cluster manager is reachable through {@link VertxInternal}, which the runtime instance always
     * implements. Returns {@code null} when Vert.x is not running clustered.
     */
    private ClusterManager clusterManager() {
        return vertx instanceof VertxInternal internal ? internal.clusterManager() : null;
    }

    private String currentNodeId() {
        ClusterManager cm = clusterManager();
        return cm != null ? cm.getNodeId() : "non-clustered";
    }

    private Map<String, Object> errorMap(Throwable e) {
        String message = e.getMessage() != null ? e.getMessage() : e.toString();
        return Map.of("status", "error", "node", currentNodeId(), "message", message);
    }
}
