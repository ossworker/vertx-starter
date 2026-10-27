package com.workoss.boot.vertx.ws;

import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * 用户信息相关的 REST 后端接口，与 {@link ChatWebSocketHandler} 共用同一张集群在线表
 * （{@link ChatWebSocketHandler#USERS_MAP}），用于演示“用户信息”既能在 WebSocket 实时通道里
 * 流动，也能通过普通 HTTP 接口查询。
 * <p>
 * 所有跨节点状态都落到 Zenoh 集群共享 {@code AsyncMap}，因此 A 节点连接进来的用户在 B/C 节点的
 * {@code /ws/chat/users} 也能看到。
 */
@RestController
@RequestMapping("/ws/chat")
@Slf4j
public class UserController {

    @Resource
    private Vertx vertx;

    /**
     * 前端接入说明：WebSocket 端点地址、连接示例、当前节点 id。
     *
     * GET /ws/chat/config
     */
    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of("status", "ok", "endpoint", "/ws/chat",
                "connectExample", "ws://<host>:<port>/ws/chat?name=Alice&role=admin",
                "node", currentNodeId());
    }

    /**
     * 按入参构造并返回一个用户信息对象（演示后端如何生成/校验用户信息）。
     *
     * GET /ws/chat/me?name=Alice&role=admin
     */
    @GetMapping("/me")
    public Map<String, Object> me(@RequestParam(defaultValue = "guest") String name,
                                  @RequestParam(defaultValue = "user") String role,
                                  @RequestParam(defaultValue = "lobby") String room) {
        UserInfo me = new UserInfo(UUID.randomUUID().toString(), name, role,
                System.currentTimeMillis(), currentNodeId(), room);
        return Map.of("status", "ok", "user", me, "node", currentNodeId());
    }

    /**
     * 查询集群全局在线用户名单（来自聊天室维护的共享 AsyncMap）。
     * 可选 {@code room} 参数按房间过滤。
     *
     * GET /ws/chat/users[?room=team-a]
     */
    @GetMapping("/users")
    public CompletionStage<Map<String, Object>> users(@RequestParam(required = false) String room) {
        return vertx.sharedData().<String, UserInfo>getAsyncMap(ChatWebSocketHandler.USERS_MAP)
                .compose(map -> map.entries())
                .<Map<String, Object>>map(entries -> {
                    List<UserInfo> all = new ArrayList<>(entries.values());
                    List<UserInfo> filtered = room == null || room.isBlank()
                            ? all
                            : all.stream().filter(u -> room.equals(u.getRoom())).toList();
                    return Map.of("status", "ok", "count", String.valueOf(filtered.size()),
                            "users", filtered, "node", currentNodeId());
                })
                .otherwise(e -> Map.of("status", "error", "message",
                        e.getMessage() != null ? e.getMessage() : e.toString()))
                .toCompletionStage();
    }

    /**
     * 列出当前集群里所有活跃房间及其成员数（按房间对共享在线表分组）。
     *
     * GET /ws/chat/rooms
     */
    @GetMapping("/rooms")
    public CompletionStage<Map<String, Object>> rooms() {
        return vertx.sharedData().<String, UserInfo>getAsyncMap(ChatWebSocketHandler.USERS_MAP)
                .compose(map -> map.entries())
                .<Map<String, Object>>map(entries -> {
                    Map<String, List<UserInfo>> byRoom = new LinkedHashMap<>();
                    for (UserInfo u : entries.values()) {
                        String r = u.getRoom() == null || u.getRoom().isBlank() ? "lobby" : u.getRoom();
                        byRoom.computeIfAbsent(r, k -> new ArrayList<>()).add(u);
                    }
                    Map<String, Object> rooms = new LinkedHashMap<>();
                    for (Map.Entry<String, List<UserInfo>> e : byRoom.entrySet()) {
                        List<Map<String, String>> members = e.getValue().stream().map(u -> Map.of(
                                "id", u.getId(), "name", u.getName(),
                                "role", u.getRole(), "nodeId", u.getNodeId())).toList();
                        rooms.put(e.getKey(), Map.of("count", String.valueOf(e.getValue().size()), "members", members));
                    }
                    return Map.of("status", "ok", "rooms", rooms, "node", currentNodeId());
                })
                .otherwise(e -> Map.of("status", "error", "message",
                        e.getMessage() != null ? e.getMessage() : e.toString()))
                .toCompletionStage();
    }

    private String currentNodeId() {
        if (vertx instanceof VertxInternal internal && internal.clusterManager() != null) {
            return internal.clusterManager().getNodeId();
        }
        return "non-clustered";
    }
}
