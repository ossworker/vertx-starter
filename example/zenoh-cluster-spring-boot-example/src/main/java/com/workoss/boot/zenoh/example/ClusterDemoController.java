package com.workoss.boot.zenoh.example;

import com.workoss.boot.zenoh.cluster.ClusterCounter;
import com.workoss.boot.zenoh.cluster.ClusterLock;
import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST surface over the four cluster capabilities of {@code spring-boot-zenoh-cluster}:
 * <ul>
 *   <li>cluster management — {@code /cluster/info}, {@code /cluster/nodes},
 *       {@code /cluster/membership} (the {@code @ZenohMembership} events this node observed);</li>
 *   <li>shared map — {@code /cluster/map/*};</li>
 *   <li>cluster counter / lock — {@code /cluster/counter/*}, {@code /cluster/lock/with};</li>
 *   <li>messaging — {@code /cluster/broadcast} (to every node), {@code /cluster/send} (to one node),
 *       {@code /cluster/messages} (what this node received);</li>
 *   <li>declarative listener demo — {@code /cluster/demo/*} publishes to the addresses of the
 *       annotated handlers in {@link MessageCollector}, including the {@code replyTo} echo demos
 *       and the {@code @Order} sequence.</li>
 * </ul>
 * Every response carries the id of the node that served it, so you can prove the cross-node
 * behaviour: start instances A and B, call {@code A /cluster/map/set}, then call
 * {@code B /cluster/map/get} and see the value; call {@code A /cluster/broadcast} and watch
 * {@code /cluster/messages} on both A and B.
 * <p>
 * Shared-map values use {@link ValueCodec#utf8()} throughout this demo to avoid JDK serialization.
 */
@RestController
public class ClusterDemoController {

    private static final Logger log = LoggerFactory.getLogger(ClusterDemoController.class);

    private final ZenohCluster cluster;
    private final MessageCollector collector;

    public ClusterDemoController(ZenohCluster cluster, MessageCollector collector) {
        this.cluster = cluster;
        this.collector = collector;
    }

    @GetMapping("/cluster/info")
    public Map<String, Object> info() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("nodeId", cluster.nodeId());
        body.put("active", cluster.isActive());
        cluster.sessionId().ifPresent(id -> body.put("sessionId", id));
        body.put("memberCount", cluster.members().size());
        return body;
    }

    @GetMapping("/cluster/nodes")
    public Map<String, Object> nodes() {
        List<Map<String, Object>> members = cluster.members().stream()
                .map(this::memberView)
                .collect(Collectors.toList());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("nodeId", cluster.nodeId());
        body.put("active", cluster.isActive());
        body.put("memberCount", members.size());
        body.put("members", members);
        return body;
    }

    // --- shared map ---

    @GetMapping("/cluster/map/set")
    public Map<String, Object> mapSet(
            @RequestParam(defaultValue = "default") String map,
            @RequestParam String key,
            @RequestParam String value,
            @RequestParam(required = false, defaultValue = "0") long ttlMs) {
        SharedMap<String, String> sharedMap = cluster.sharedMap(map, ValueCodec.utf8());
        if (ttlMs > 0) {
            sharedMap.put(key, value, Duration.ofMillis(ttlMs));
        } else {
            sharedMap.put(key, value);
        }
        log.info("map set: map={}, key={}, value={}, node={}", map, key, value, cluster.nodeId());
        return ok("map", map, Map.of("key", key, "value", value, "ttlMs", ttlMs));
    }

    @GetMapping("/cluster/map/get")
    public Map<String, Object> mapGet(
            @RequestParam(defaultValue = "default") String map,
            @RequestParam String key) {
        String value = cluster.sharedMap(map, ValueCodec.utf8()).get(key);
        log.info("map get: map={}, key={}, value={}, node={}", map, key, value, cluster.nodeId());
        Map<String, Object> body = new LinkedHashMap<>(ok("map", map, Map.of("key", key)));
        body.put("value", value == null ? "null (not found)" : value);
        return body;
    }

    @GetMapping("/cluster/map/delete")
    public Map<String, Object> mapDelete(
            @RequestParam(defaultValue = "default") String map,
            @RequestParam String key) {
        String removed = cluster.sharedMap(map, ValueCodec.utf8()).remove(key);
        log.info("map delete: map={}, key={}, removed={}, node={}", map, key, removed, cluster.nodeId());
        return ok("map", map, Map.of("key", key, "removed", removed == null ? "null" : removed));
    }

    @GetMapping("/cluster/map/entries")
    public Map<String, Object> mapEntries(@RequestParam(defaultValue = "default") String map) {
        SharedMap<String, String> sharedMap = cluster.sharedMap(map, ValueCodec.utf8());
        Map<String, String> entries = sharedMap.entries();
        log.info("map entries: map={}, count={}, node={}", map, entries.size(), cluster.nodeId());
        return ok("map", map, Map.of("count", entries.size(), "entries", entries));
    }

    // --- counter ---

    @GetMapping("/cluster/counter/inc")
    public Map<String, Object> counterInc(
            @RequestParam(defaultValue = "default") String name,
            @RequestParam(required = false, defaultValue = "1") long delta) {
        ClusterCounter counter = cluster.counter(name);
        long value = delta >= 0 ? counter.addAndGet(delta) : counter.getAndAdd(delta);
        return ok("counter", name, Map.of("value", value));
    }

    @GetMapping("/cluster/counter/get")
    public Map<String, Object> counterGet(@RequestParam(defaultValue = "default") String name) {
        return ok("counter", name, Map.of("value", cluster.counter(name).get()));
    }

    // --- lock ---

    @GetMapping("/cluster/lock/with")
    public Map<String, Object> lockWith(
            @RequestParam(defaultValue = "default") String name,
            @RequestParam(required = false, defaultValue = "500") long ms) {
        long heldMs = Math.min(ms, 5_000L);
        try (ClusterLock lock = cluster.tryLock(name, Duration.ofSeconds(5)).orElse(null)) {
            if (lock == null) {
                return Map.of("status", "locked-by-other", "node", cluster.nodeId(), "lock", name);
            }
            long acquiredAt = System.currentTimeMillis();
            log.info("lock acquired: name={}, node={}", name, cluster.nodeId());
            Thread.sleep(heldMs);
            long elapsed = System.currentTimeMillis() - acquiredAt;
            log.info("lock released: name={}, node={}, heldMs={}", name, cluster.nodeId(), elapsed);
            Map<String, Object> body = new LinkedHashMap<>(ok("lock", name, Map.of("heldMs", elapsed)));
            body.put("held", lock.isHeld());
            return body;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Map.of("status", "interrupted", "node", cluster.nodeId(), "lock", name);
        }
    }

    // --- messaging ---

    @GetMapping("/cluster/broadcast")
    public Map<String, Object> broadcast(
            @RequestParam String msg,
            @RequestParam(required = false, defaultValue = MessageCollector.DEMO_BROADCAST_ADDRESS) String address) {
        cluster.messaging().broadcast(address, msg.getBytes(StandardCharsets.UTF_8));
        log.info("broadcast: address={}, msg={}, node={}", address, msg, cluster.nodeId());
        return ok("broadcast", address, Map.of("msg", msg));
    }

    @GetMapping("/cluster/send")
    public Map<String, Object> send(
            @RequestParam String target,
            @RequestParam String msg) {
        cluster.messaging().send(target, msg.getBytes(StandardCharsets.UTF_8));
        log.info("send: target={}, msg={}, node={}", target, msg, cluster.nodeId());
        return ok("send", target, Map.of("msg", msg));
    }

    // --- convenience publishers for the @ZenohSubscribe demo addresses ---
    // Each maps to one annotated handler in MessageCollector, so a single call demonstrates that
    // the declarative subscription fires on this node (and, in a multi-node run, on every node).

    @GetMapping("/cluster/demo/alert")
    public Map<String, Object> demoAlert(@RequestParam String region, @RequestParam String msg) {
        return broadcast(msg, "alerts/" + region);
    }

    @GetMapping("/cluster/demo/raw")
    public Map<String, Object> demoRaw(@RequestParam String topic, @RequestParam String msg) {
        return broadcast(msg, "raw/" + topic);
    }

    @GetMapping("/cluster/demo/notice")
    public Map<String, Object> demoNotice(@RequestParam String topic, @RequestParam String msg) {
        return broadcast(msg, "notices/" + topic);
    }

    @GetMapping("/cluster/demo/echo")
    public Map<String, Object> demoEcho(@RequestParam String msg) {
        return broadcast(msg, MessageCollector.ECHO_ADDRESS);
    }

    @GetMapping("/cluster/demo/slow-echo")
    public Map<String, Object> demoSlowEcho(@RequestParam String msg) {
        return broadcast(msg, MessageCollector.SLOW_ECHO_ADDRESS);
    }

    @GetMapping("/cluster/demo/ordered")
    public Map<String, Object> demoOrdered(@RequestParam String msg) {
        return broadcast(msg, "demo/ordered/x");
    }

    @GetMapping("/cluster/demo/ordered-sequence")
    public Map<String, Object> demoOrderedSequence() {
        List<String> sequence = collector.recentOrderedSequence();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("nodeId", cluster.nodeId());
        body.put("count", sequence.size());
        body.put("sequence", sequence);
        return body;
    }

    @GetMapping("/cluster/messages")
    public Map<String, Object> messages() {
        List<MessageCollector.ReceivedView> recent = collector.recent();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("nodeId", cluster.nodeId());
        body.put("count", recent.size());
        body.put("messages", recent);
        return body;
    }

    @GetMapping("/cluster/membership")
    public Map<String, Object> membership() {
        List<MessageCollector.MembershipView> events = collector.recentMembership();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("nodeId", cluster.nodeId());
        body.put("count", events.size());
        body.put("events", events);
        return body;
    }

    // --- helpers ---

    private Map<String, Object> memberView(NodeInfo node) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("nodeId", node.nodeId());
        view.put("incarnation", node.incarnation());
        view.put("startedAt", node.startedAt());
        view.put("hostName", node.hostName());
        return view;
    }

    private Map<String, Object> ok(String kind, String name, Map<String, Object> extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("kind", kind);
        body.put("name", name);
        body.put("node", cluster.nodeId());
        body.putAll(extra);
        return body;
    }
}
