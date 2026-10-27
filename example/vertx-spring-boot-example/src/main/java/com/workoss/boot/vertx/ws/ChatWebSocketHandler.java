package com.workoss.boot.vertx.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多房间聊天 WebSocket 处理器。
 * <p>
 * 每个连接即一个用户：连接时从查询参数 {@code ?name=}&amp;{@code role=}&amp;{@code room=} 解析出
 * {@link UserInfo}，注册到 {@link ClusterWebSocketBroadcaster} 的「房间分组」
 * （{@link #groupOf(String)}），并写入集群共享 {@code AsyncMap}（{@link #USERS_MAP}），
 * 以便任意节点都能拿到全局在线名单、并按房间过滤。
 * <p>
 * 房间是跨节点隔离的第一级单元：{@code #team-a} 里 A 节点发出的消息只会被注册在 {@code chat:team-a}
 * 分组的会话收到——无论这些会话实际挂在 A/B/C 哪台节点上；{@code #team-b} 完全收不到。这正是
 * 「多人 + 跨节点」的核心：多个房间各自多人，房间内通过 Zenoh 事件总线集群广播，房间间互不影响。
 * <p>
 * 消息流：
 * <ul>
 *   <li>连接成功 → 向本人推送 {@code welcome}（自身信息 + 房间 + 全局在线名单）；</li>
 *   <li>向本房间分组广播 {@code system: X joined #room} 与 {@code roster}（在线名单）；</li>
 *   <li>收到 {@code {"type":"chat","text":"..."}} → 附上发送者 {@link UserInfo} 后仅向本房间分组
 *       集群级广播；</li>
 *   <li>收到 {@code {"type":"switch","room":"new"}} → 原地切换房间（注销旧分组、注册新分组、
 *       更新共享表），无需断开重连；</li>
 *   <li>连接关闭 → 广播 {@code system: X left} 并从在线名单移除。</li>
 * </ul>
 * 出站报文统一为 JSON 字符串，符合 {@link ClusterWebSocketBroadcaster} 对“可序列化的可传输值”的要求。
 */
@Slf4j
public class ChatWebSocketHandler implements WebSocketHandler {

    /** 广播分组前缀；房间 room 对应的分组为 {@code chat:<room>}。 */
    public static final String GROUP_PREFIX = "chat:";
    /** 集群共享在线用户表名（键=用户 id，值=UserInfo），被本处理器与 UserController 共用。 */
    public static final String USERS_MAP = "ws-chat-users";
    /** 默认房间名。 */
    public static final String DEFAULT_ROOM = "lobby";

    private final ClusterWebSocketBroadcaster broadcaster;
    private final ObjectMapper objectMapper;
    private final Vertx vertx;
    /** 本节点持有的会话 id → 用户信息（含当前房间），用于发送消息时附上发送者身份。 */
    private final Map<String, UserInfo> localUsers = new ConcurrentHashMap<>();
    /** Vert.x sessionId → 稳定用户身份 uid（刷新后用同一 uid 重连即可 resume）。 */
    private final Map<String, String> sessionUid = new ConcurrentHashMap<>();
    /** 稳定身份 uid → 当前活跃会话的 Vert.x sessionId；刷新后指向新会话，旧会话 close 时据此跳过清理。 */
    private final Map<String, String> uidSession = new ConcurrentHashMap<>();
    /** 每房间最近消息 ring buffer，用于断线重连后补发遗漏消息（按节点内存保留，演示用）。 */
    private final Map<String, Deque<ChatMsg>> roomBuffers = new ConcurrentHashMap<>();
    /** 每房间自增序号，供客户端作为 resume 游标。 */
    private final Map<String, AtomicLong> roomSeq = new ConcurrentHashMap<>();
    /** uid → 宽限定时器 id：断线后延迟真正移除，期内同 uid 重连则取消（resume）。 */
    private final Map<String, Long> resumeTimers = new ConcurrentHashMap<>();
    private static final int REPLAY_CAP = 256;
    private static final long RESUME_GRACE_MS = 5000;

    /** 房间内一条已广播消息的快照，含序号与原始 JSON，用于断线重连补发。 */
    private record ChatMsg(long seq, String json) {}

    public ChatWebSocketHandler(ClusterWebSocketBroadcaster broadcaster, ObjectMapper objectMapper, Vertx vertx) {
        this.broadcaster = broadcaster;
        this.objectMapper = objectMapper;
        this.vertx = vertx;
    }

    /** 房间名 → 广播分组名。 */
    public static String groupOf(String room) {
        return GROUP_PREFIX + (room == null || room.isEmpty() ? DEFAULT_ROOM : room);
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        URI uri = session.getHandshakeInfo().getUri();
        Map<String, String> qp = queryParams(uri);
        String name = qp.getOrDefault("name", "guest-" + session.getId().substring(0, 6));
        String role = qp.getOrDefault("role", "user");
        String room = sanitizeRoom(qp.getOrDefault("room", DEFAULT_ROOM));
        String group = groupOf(room);

        // 稳定身份：优先用客户端 localStorage 里的 uid，刷新后重连可 resume；否则退回 sessionId。
        String uid = qp.get("uid");
        boolean hasUid = uid != null && !uid.isBlank();
        String id = hasUid ? uid : session.getId();
        long resumeSeq = parseLong(qp.get("resume"), 0L);

        UserInfo self = new UserInfo(id, name, role, System.currentTimeMillis(), currentNodeId(), room);
        String sessionId = session.getId();

        // 判定 resume：
        //  - 本身份此前已有活跃会话（刷新竞态下旧会话尚未 close，localUsers 已包含该 uid）→ 视为重连；
        //  - 或断线宽限期内已调度“移除定时器”→ 取消定时器，视为重连。
        // 两种都不满足才是全新加入，正常广播 joined。
        boolean hadPrior = localUsers.containsKey(id);
        Long pending = resumeTimers.remove(id);
        boolean resuming = pending != null || hadPrior;
        if (pending != null) {
            vertx.cancelTimer(pending);
        }
        // 同一身份若已有旧 session，先注销其 broadcaster 登记，避免刷新窗口内重复投递。
        String prevSession = uidSession.put(id, sessionId);
        if (prevSession != null && !prevSession.equals(sessionId)) {
            broadcaster.unregister(prevSession);
        }

        broadcaster.register(session, group);
        sessionUid.put(sessionId, id);
        localUsers.put(id, self);

        // 先写集群在线表，再取全局名单，最后向本人推送 welcome。
        return Mono.fromCompletionStage(vertx.sharedData().<String, UserInfo>getAsyncMap(USERS_MAP).toCompletionStage())
                .flatMap(map -> Mono.fromCompletionStage(map.put(id, self).toCompletionStage())
                        .then(Mono.fromCompletionStage(map.entries().toCompletionStage()))
                        .flatMap(entries -> {
                            List<UserInfo> online = new ArrayList<>(entries.values());
                            String welcome = json(Map.of("type", "welcome", "user", self, "room", room,
                                    "online", online, "resumed", resuming));
                            Mono<Void> intro = session.send(Flux.just(session.textMessage(welcome)));

                            if (!resuming) {
                                broadcaster.broadcast(json(Map.of("type", "system", "text",
                                        self.getName() + " joined #" + room, "room", room,
                                        "time", System.currentTimeMillis())), group);
                            }
                            // 断线重连：补发本房间内 seq 大于 resume 的遗漏消息（仅发给本人，不广播）。
                            if (resuming && resumeSeq > 0) {
                                replayMissed(sessionId, room, resumeSeq);
                            }
                            // 在线名单/房间目录全局可见（每个用户带 room 字段，前端按当前房间过滤）；
                            // 聊天消息仍按房间分组隔离，仅房间内成员收到。
                            broadcaster.broadcast(json(Map.of("type", "roster", "online", online)), null);
                            Mono<Void> pump = session.receive()
                                    .doOnNext(message -> onMessage(session, message))
                                    .doFinally(signal -> onClose(sessionId, self))
                                    .then();
                            return Mono.when(intro, pump);
                        }));
    }

    /** 补发本房间内序号大于 {@code resumeSeq} 的遗漏消息，仅定向发给 {@code sessionId} 本人。 */
    private void replayMissed(String sessionId, String room, long resumeSeq) {
        Deque<ChatMsg> buf = roomBuffers.get(room);
        if (buf == null) {
            return;
        }
        for (ChatMsg m : buf) {
            if (m.seq() > resumeSeq) {
                broadcaster.sendToLocal(sessionId, m.json());
            }
        }
    }

    private static long parseLong(String s, long dflt) {
        if (s == null) {
            return dflt;
        }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }


    private void onMessage(WebSocketSession session, WebSocketMessage message) {
        String raw = message.getPayloadAsText();
        JsonNode node;
        try {
            node = objectMapper.readTree(raw);
        } catch (Exception e) {
            return;
        }
        String type = node.has("type") ? node.get("type").asText() : "chat";
        if ("switch".equals(type)) {
            String newRoom = node.has("room") ? node.get("room").asText() : DEFAULT_ROOM;
            onSwitch(session, newRoom);
            return;
        }
        String content = node.has("text") ? node.get("text").asText() : raw;
        if (content == null || content.isBlank()) {
            return;
        }
        UserInfo selfUser = localUsers.getOrDefault(sessionUid.get(session.getId()),
                new UserInfo(session.getId(), "anonymous", "user", 0L, currentNodeId(), DEFAULT_ROOM));
        String group = groupOf(selfUser.getRoom());
        long seq = roomSeq.computeIfAbsent(selfUser.getRoom(), k -> new AtomicLong()).incrementAndGet();
        String chatJson = json(Map.of("type", "chat", "from", selfUser, "room", selfUser.getRoom(),
                "text", content, "time", System.currentTimeMillis(), "seq", seq));
        // 写入本房间 replay buffer（容量上限 REPLAY_CAP），供断线重连补发。
        Deque<ChatMsg> buf = roomBuffers.computeIfAbsent(selfUser.getRoom(), k -> new ArrayDeque<>());
        buf.addLast(new ChatMsg(seq, chatJson));
        while (buf.size() > REPLAY_CAP) {
            buf.removeFirst();
        }
        broadcaster.broadcast(chatJson, group);
    }

    /** 原地切换房间：注销旧分组、注册新分组、更新共享表，再向新房间广播系统消息与名单。 */
    private void onSwitch(WebSocketSession session, String requestedRoom) {
        String newRoom = sanitizeRoom(requestedRoom);
        UserInfo self = localUsers.get(sessionUid.get(session.getId()));
        if (self == null) {
            return;
        }
        String oldGroup = groupOf(self.getRoom());
        String newGroup = groupOf(newRoom);
        if (!oldGroup.equals(newGroup)) {
            broadcaster.unregister(session.getId());
            broadcaster.register(session, newGroup);
        }
        self.setRoom(newRoom);
        // 先通知旧房间“离开”，再更新共享表、广播新房间名单。
        if (!oldGroup.equals(newGroup)) {
            broadcaster.broadcast(json(Map.of("type", "system", "text",
                    self.getName() + " left #" + self.getRoom(), "room", self.getRoom(),
                    "time", System.currentTimeMillis())), oldGroup);
        }
        vertx.sharedData().<String, UserInfo>getAsyncMap(USERS_MAP)
                .compose(map -> map.put(self.getId(), self))
                .onComplete(ar -> {
                    broadcaster.broadcast(json(Map.of("type", "system", "text",
                            self.getName() + " moved to #" + newRoom, "room", newRoom,
                            "time", System.currentTimeMillis())), newGroup);
                    broadcastRoster();
                });
        // 直接给本人下发房间切换确认，更新 UI 房间标签。
        broadcaster.sendToLocal(session.getId(),
                json(Map.of("type", "roomChanged", "room", newRoom, "user", self)));
    }

    /** 从集群共享表取出在线名单，向所有节点/会话全局广播 {@code roster}（房间目录对全员可见）。 */
    private void broadcastRoster() {
        vertx.sharedData().<String, UserInfo>getAsyncMap(USERS_MAP)
                .compose(map -> map.entries())
                .onSuccess(entries -> broadcaster.broadcast(
                        json(Map.of("type", "roster", "online", new ArrayList<>(entries.values()))), null))
                .onFailure(e -> log.warn("Failed to build roster", e));
    }

    private void onClose(String sessionId, UserInfo self) {
        String id = self.getId();
        sessionUid.remove(sessionId);
        broadcaster.unregister(sessionId);
        // 若本会话已不是该身份当前活跃会话（刷新重连后新会话已接管 uidSession），跳过清理。
        if (!sessionId.equals(uidSession.get(id))) {
            localUsers.remove(id);
            return;
        }
        // 宽限期内若同 uid 重连，handle 会 unregister 旧 session 并取消本定时器，不会真正清理。
        long timer = vertx.setTimer(RESUME_GRACE_MS, tid -> {
            // 定时器触发时，若 uid 仍指向本会话（无人接管），才执行真实下线清理。
            if (sessionId.equals(uidSession.get(id))) {
                localUsers.remove(id);
                uidSession.remove(id);
                resumeTimers.remove(id);
                broadcaster.broadcast(json(Map.of("type", "system", "text",
                        self.getName() + " left #" + self.getRoom(), "room", self.getRoom(),
                        "time", System.currentTimeMillis())), groupOf(self.getRoom()));
                vertx.sharedData().<String, UserInfo>getAsyncMap(USERS_MAP)
                        .compose(map -> map.remove(id))
                        .onComplete(ar -> broadcastRoster());
            }
        });
        resumeTimers.put(id, timer);
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            log.warn("Failed to serialize websocket payload", e);
            return "{\"type\":\"error\"}";
        }
    }

    private static String sanitizeRoom(String room) {
        if (room == null || room.isBlank()) {
            return DEFAULT_ROOM;
        }
        // 房间名仅保留字母数字与常见符号，避免分组名注入。
        String cleaned = room.trim().replaceAll("[^\\w\\-.]", "-");
        return cleaned.isEmpty() ? DEFAULT_ROOM : cleaned;
    }

    private static Map<String, String> queryParams(URI uri) {
        Map<String, String> params = new LinkedHashMap<>();
        String q = uri.getQuery();
        if (q == null || q.isEmpty()) {
            return params;
        }
        for (String pair : q.split("&")) {
            int idx = pair.indexOf('=');
            if (idx < 0) {
                params.put(decode(pair), "");
            } else {
                params.put(decode(pair.substring(0, idx)), decode(pair.substring(idx + 1)));
            }
        }
        return params;
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private String currentNodeId() {
        if (vertx instanceof VertxInternal internal && internal.clusterManager() != null) {
            return internal.clusterManager().getNodeId();
        }
        return "non-clustered";
    }
}
