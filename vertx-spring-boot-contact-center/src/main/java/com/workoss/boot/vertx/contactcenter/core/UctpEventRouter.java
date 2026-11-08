package com.workoss.boot.vertx.contactcenter.core;

import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Outbound;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Route;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * UCTP 事件路由器：把出站信封投递到本节点 WS 会话，并通过 Vert.x 事件总线跨节点扇出。
 *
 * <p>每个节点维护本地索引（WS 会话 → 身份 → 会话组），集群消息携带目标描述
 * （会话组 / 身份 / 单个 WS 会话），各节点只写自己持有的连接——与
 * {@link ClusterWebSocketBroadcaster} 的「归属节点写」原则一致。信封本体走
 * {@link JsonObject}（Vert.x 内置编解码器），无需自定义 codec。
 */
public class UctpEventRouter {

    private static final Logger log = LoggerFactory.getLogger(UctpEventRouter.class);

    /** 集群出站地址。 */
    public static final String OUT_ADDRESS = "vertx.uctp.out";
    /** 集群在线状态同步地址。 */
    public static final String PRESENCE_ADDRESS = "vertx.uctp.presence";
    public static final String MEMBERSHIP_ADDRESS = "vertx.uctp.membership";

    /** 会话组名前缀：Conversation 组 = uctp:conv:{cid}。 */
    public static final String GROUP_PREFIX = "uctp:conv:";

    private final Vertx vertx;
    private final ClusterWebSocketBroadcaster broadcaster;
    /** Local connection-scoped observers; never published on the event bus. */
    private final Map<String, Consumer<JsonObject>> replyObservers = new ConcurrentHashMap<>();

    /** wsSessionId → 已认证身份（未认证前不在表内）。 */
    private final Map<String, String> identityByWsSession = new ConcurrentHashMap<>();
    /** identityId → 本节点持有的 wsSessionIds。 */
    private final Map<String, Set<String>> wsSessionsByIdentity = new ConcurrentHashMap<>();
    /** wsSessionId → 已加入的会话组。 */
    private final Map<String, Set<String>> groupsByWsSession = new ConcurrentHashMap<>();
    /** 组 → 成员 wsSessionIds。 */
    private final Map<String, Set<String>> wsSessionsByGroup = new ConcurrentHashMap<>();
    /** 全集群在线身份（各节点经 PRESENCE_ADDRESS 同步）。 */
    private final Set<String> onlineIdentities = ConcurrentHashMap.newKeySet();
    /** Only owner-published, durably committed membership may authorize a group subscription. */
    private final Map<String, Set<String>> membersByConversation = new ConcurrentHashMap<>();
    /** Workforce's synchronous API can notify before its asynchronous membership save finishes. */
    private final Map<String, List<Outbound>> pendingAssignments = new java.util.HashMap<>();
    private int pendingAssignmentCount;
    private final String membershipOrigin = java.util.UUID.randomUUID().toString();

    public UctpEventRouter(Vertx vertx, ClusterWebSocketBroadcaster broadcaster) {
        this.vertx = vertx;
        this.broadcaster = broadcaster;
    }

    /** 注册集群消费者：每个节点只投递到自己持有的 WS 会话。 */
    public void registerListener() {
        vertx.eventBus().<JsonObject>consumer(OUT_ADDRESS, message -> deliverLocally(message.body()));
        vertx.eventBus().<JsonObject>consumer(MEMBERSHIP_ADDRESS, message -> {
            JsonObject body = message.body();
            if (membershipOrigin.equals(body.getString("origin"))) return;
            Set<String> members = ConcurrentHashMap.newKeySet();
            body.getJsonArray("identities").forEach(value -> members.add((String) value));
            updateMembers(body.getString("cid"), members);
        });
        vertx.eventBus().<JsonObject>consumer(PRESENCE_ADDRESS, message -> {
            JsonObject body = message.body();
            if ("online".equals(body.getString("op"))) {
                onlineIdentities.add(body.getString("identity_id"));
            } else {
                onlineIdentities.remove(body.getString("identity_id"));
            }
        });
    }

    // --- 本地绑定管理 ---

    /** 把 WS 会话注册进广播器本地表（出站写链的落点），无分组。 */
    public void registerLocalSession(org.springframework.web.reactive.socket.WebSocketSession session) {
        broadcaster.register(session, null);
    }

    public void registerReplyObserver(String wsSessionId, Consumer<JsonObject> observer) {
        replyObservers.put(wsSessionId, observer);
    }

    public void unregisterLocalSession(String wsSessionId) {
        replyObservers.remove(wsSessionId);
        broadcaster.unregister(wsSessionId);
    }

    /** Cached replies and admission errors are local-only and must not complete a pending request. */
    public void replayToLocal(String wsSessionId, JsonObject envelope) {
        JsonObject publicEnvelope = envelope.copy();
        publicEnvelope.remove("_recipients");
        sendLocal(wsSessionId, UctpEnvelope.wire(publicEnvelope), false);
    }

    public void bindIdentity(String wsSessionId, String identityId) {
        String previous = identityByWsSession.get(wsSessionId);
        if (previous != null && !previous.equals(identityId)) unbind(wsSessionId);
        identityByWsSession.put(wsSessionId, identityId);
        wsSessionsByIdentity.computeIfAbsent(identityId, k -> new CopyOnWriteArraySet<>()).add(wsSessionId);
        if (onlineIdentities.add(identityId)) {
            vertx.eventBus().publish(PRESENCE_ADDRESS,
                    new JsonObject().put("op", "online").put("identity_id", identityId));
        }
    }

    public String identityOf(String wsSessionId) {
        return identityByWsSession.get(wsSessionId);
    }

    /** 该身份是否在全集群在线。 */
    public boolean identityOnline(String identityId) {
        return onlineIdentities.contains(identityId);
    }

    public void conversationMembers(String cid, Set<String> identities) {
        updateMembers(cid, Set.copyOf(identities));
        vertx.eventBus().publish(MEMBERSHIP_ADDRESS, new JsonObject().put("cid", cid).put("origin", membershipOrigin)
                .put("identities", new io.vertx.core.json.JsonArray(new java.util.ArrayList<>(identities))));
    }

    private void updateMembers(String cid, Set<String> identities) {
        if (identities.isEmpty()) membersByConversation.remove(cid);
        else membersByConversation.put(cid, identities);
        releaseAssignments(cid, identities.isEmpty());
        String group = GROUP_PREFIX + cid;
        Set<String> sessions = wsSessionsByGroup.get(group);
        if (sessions != null) {
            sessions.removeIf(ws -> {
                String identity = identityByWsSession.get(ws);
                if (identity != null && identities.contains(identity)) return false;
                Set<String> groups = groupsByWsSession.get(ws);
                if (groups != null) groups.remove(group);
                return true;
            });
            if (sessions.isEmpty()) wsSessionsByGroup.remove(group, sessions);
        }
    }

    public void joinGroup(String wsSessionId, String cid) {
        if (wsSessionId == null) return;
        String bound = identityByWsSession.get(wsSessionId);
        Set<String> members = membersByConversation.get(cid);
        if (bound == null || members == null || !members.contains(bound)) return;
        String group = GROUP_PREFIX + cid;
        groupsByWsSession.computeIfAbsent(wsSessionId, k -> ConcurrentHashMap.newKeySet()).add(group);
        wsSessionsByGroup.computeIfAbsent(group, k -> ConcurrentHashMap.newKeySet()).add(wsSessionId);
    }

    /** 连接关闭：清掉身份与全部组绑定。 */
    public void unbind(String wsSessionId) {
        replyObservers.remove(wsSessionId);
        String identityId = identityByWsSession.remove(wsSessionId);
        if (identityId != null) {
            Set<String> sessions = wsSessionsByIdentity.get(identityId);
            if (sessions != null) {
                sessions.remove(wsSessionId);
                if (sessions.isEmpty()) {
                    wsSessionsByIdentity.remove(identityId, sessions);
                }
            }
        }
        Set<String> groups = groupsByWsSession.remove(wsSessionId);
        if (identityId != null) {
            // 该身份在本地已无其他会话时广播离线
            Set<String> remaining = wsSessionsByIdentity.get(identityId);
            if (remaining == null || remaining.isEmpty()) {
                onlineIdentities.remove(identityId);
                vertx.eventBus().publish(PRESENCE_ADDRESS,
                        new JsonObject().put("op", "offline").put("identity_id", identityId));
            }
        }
        if (groups != null) {
            for (String group : groups) {
                Set<String> members = wsSessionsByGroup.get(group);
                if (members != null) {
                    members.remove(wsSessionId);
                    if (members.isEmpty()) {
                        wsSessionsByGroup.remove(group, members);
                    }
                }
            }
        }
    }

    // --- 出站投递 ---

    public void deliver(List<Outbound> outbounds) {
        for (Outbound outbound : outbounds) {
            deliver(outbound);
        }
    }

    private boolean assignmentReady(Outbound outbound) {
        if (!(outbound.route() instanceof Route.IdentityTarget target)) return true;
        Set<String> members = membersByConversation.get(outbound.envelope().getString("cid"));
        JsonObject payload = outbound.envelope().getJsonObject("payload", new JsonObject());
        String peer = payload.getString("agent_identity_id", payload.getString("customer_identity_id"));
        return members != null && members.contains(target.identityId()) && peer != null && members.contains(peer);
    }

    private boolean deferAssignment(Outbound outbound) {
        if (!"workforce.assigned".equals(outbound.envelope().getString("type"))) return false;
        String cid = outbound.envelope().getString("cid");
        if (cid == null) return false;
        synchronized (pendingAssignments) {
            if (assignmentReady(outbound)) return false;
            List<Outbound> pending = pendingAssignments.get(cid);
            if (pendingAssignmentCount >= 4096 || pending != null && pending.size() >= 64) {
                log.warn("assignment notification capacity reached cid={}", cid);
                return true; // No false assignment signal on saturation; consultation.list is authoritative.
            }
            pendingAssignments.computeIfAbsent(cid, ignored -> new java.util.ArrayList<>())
                    .add(new Outbound(outbound.envelope().copy(), outbound.route()));
            pendingAssignmentCount++;
            return true;
        }
    }

    private void releaseAssignments(String cid, boolean closed) {
        List<Outbound> ready = new java.util.ArrayList<>();
        synchronized (pendingAssignments) {
            List<Outbound> pending = pendingAssignments.get(cid);
            if (pending == null) return;
            pending.removeIf(outbound -> {
                if (!closed && !assignmentReady(outbound)) return false;
                pendingAssignmentCount--;
                if (!closed) ready.add(outbound);
                return true;
            });
            if (pending.isEmpty()) pendingAssignments.remove(cid);
        }
        ready.forEach(this::deliver);
    }

    public void deliver(Outbound outbound) {
        if (deferAssignment(outbound)) return;
        JsonObject publicEnvelope = outbound.envelope().copy();
        publicEnvelope.remove("_recipients");
        String wire = UctpEnvelope.wire(publicEnvelope);
        Route route = outbound.route();
        JsonObject clusterMessage = new JsonObject().put("wire", wire);
        switch (route) {
            case Route.ConversationGroup group ->
                    clusterMessage.put("target", new JsonObject()
                            .put("mode", "group").put("group", GROUP_PREFIX + group.cid()));
            case Route.IdentityTarget identity ->
                    clusterMessage.put("target", new JsonObject()
                            .put("mode", "identity").put("identity_id", identity.identityId()));
            case Route.WsSessionTarget wsSession ->
                    clusterMessage.put("target", new JsonObject()
                            .put("mode", "ws").put("ws_session_id", wsSession.wsSessionId()));
        }
        vertx.eventBus().publish(OUT_ADDRESS, clusterMessage);
    }

    private void deliverLocally(JsonObject clusterMessage) {
        JsonObject target = clusterMessage.getJsonObject("target");
        String wire = clusterMessage.getString("wire");
        String mode = target.getString("mode");
        switch (mode) {
            case "group" -> {
                Set<String> members = wsSessionsByGroup.get(target.getString("group"));
                if (members != null) {
                    String cid = target.getString("group").substring(GROUP_PREFIX.length());
                    Set<String> authorized = membersByConversation.get(cid);
                    if (authorized != null) members.stream()
                            .filter(id -> identityByWsSession.get(id) != null && authorized.contains(identityByWsSession.get(id)))
                            .forEach(id -> sendLocal(id, wire, true));
                }
            }
            case "identity" -> {
                Set<String> sessions = wsSessionsByIdentity.get(target.getString("identity_id"));
                if (sessions != null) {
                    sessions.forEach(id -> sendLocal(id, wire, true));
                }
            }
            case "ws" -> sendLocal(target.getString("ws_session_id"), wire, true);
            default -> log.warn("Unknown uctp route mode {}", mode);
        }
    }

    private void sendLocal(String wsSessionId, String wire, boolean observe) {
        Consumer<JsonObject> observer = observe ? replyObservers.get(wsSessionId) : null;
        if (observer != null) {
            JsonObject envelope = new JsonObject(wire);
            if (envelope.getValue("in_reply_to") instanceof String
                    && !envelope.getString("type", "").startsWith("auth.")) {
                try {
                    observer.accept(envelope.copy());
                } catch (RuntimeException e) {
                    log.warn("uctp reply observer failed ws={}", wsSessionId);
                }
            }
        }
        broadcaster.sendToLocal(wsSessionId, wire);
    }
}
