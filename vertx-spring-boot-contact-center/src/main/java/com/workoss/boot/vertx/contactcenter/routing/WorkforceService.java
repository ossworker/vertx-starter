package com.workoss.boot.vertx.contactcenter.routing;

import com.workoss.boot.vertx.contactcenter.core.ConversationService;
import com.workoss.boot.vertx.contactcenter.core.UctpEventRouter;
import com.workoss.boot.vertx.contactcenter.uctp.AuthPrincipal;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Outbound;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Route.identity;

/**
 * Workforce（VoIP-3 §4.3）：坐席池 + 排队路由（ACD）。
 *
 * <p>技能组 + 优先级队列：客户 {@code workforce.enqueue} 进入等待队列（priority 大者优先，
 * 同优先级先到先得）；坐席 {@code workforce.login} 按技能组上线，匹配规则为
 * 「请求技能 ⊆ 坐席技能」，优先选择会话负载最小者，同负载取最久未分配者。转接/AI→人工升级走
 * {@code workforce.handoff}，分配成功后老坐席角色降级为 observer（VoIP-3 handoff 语义）。
 *
 * <p>集群模型：全集群只有「路由节点」（节点 ID 字典序最小者）维护坐席与队列权威状态；
 * 其余节点把 workforce.* 信封广播到 {@value #ADDRESS}，路由节点认领执行。
 */
public class WorkforceService {

    private static final Logger log = LoggerFactory.getLogger(WorkforceService.class);

    public static final String ADDRESS = "vertx.uctp.workforce";

    private final Vertx vertx;
    private final UctpEventRouter router;
    private final ConversationService conversationService;
    private final UctpIds ids;
    private final UctpEnvelope envelopes;

    /** 坐席接待状态：只控制新 cid 分配，不影响已有会话。 */
    public enum AgentState {AVAILABLE, BUSY, BREAK}

    /** 坐席记录（路由节点权威状态，访问由 WorkforceService 锁串行化）。 */
    static final class AgentRecord {
        final String identityId;
        String displayName;
        final String kind;
        final Set<String> skills = new HashSet<>();
        final Set<String> assignedConversations = new HashSet<>();
        final Set<String> pendingConversations = new HashSet<>();
        AgentState state = AgentState.AVAILABLE;
        boolean loggedIn = true;
        Instant lastAssignedAt = Instant.EPOCH;
        int handled;

        AgentRecord(String identityId, String displayName, String kind) {
            this.identityId = identityId;
            this.displayName = displayName;
            this.kind = kind;
        }

        JsonObject toJson() {
            return new JsonObject()
                    .put("identity_id", identityId)
                    .put("display_name", displayName)
                    .put("kind", kind)
                    .put("skills", new JsonArray(skills.stream().sorted().toList()))
                    .put("state", state.name().toLowerCase())
                    .put("logged_in", loggedIn)
                    .put("assigned_cids", new JsonArray(assignedConversations.stream().sorted().toList()))
                    .put("active_conversations", assignedConversations.size())
                    .put("handled", handled);
        }
    }

    /** 排队条目。 */
    static final class WaitEntry {
        final String cid;
        final String customerIdentityId;
        final Set<String> skills = new HashSet<>();
        final int priority;
        final Instant enqueuedAt = Instant.now();
        final boolean transfer;
        final String fromIdentityId;
        final String sid;
        /** 外部渠道客户（短信/邮件）：无 WS 连接，在线判定恒真。 */
        final boolean external;

        WaitEntry(String cid, String customerIdentityId, Set<String> skills, int priority,
                  boolean transfer, String fromIdentityId, String sid) {
            this(cid, customerIdentityId, skills, priority, transfer, fromIdentityId, sid, false);
        }

        WaitEntry(String cid, String customerIdentityId, Set<String> skills, int priority,
                  boolean transfer, String fromIdentityId, String sid, boolean external) {
            this.cid = cid;
            this.customerIdentityId = customerIdentityId;
            this.skills.addAll(skills);
            this.priority = priority;
            this.transfer = transfer;
            this.fromIdentityId = fromIdentityId;
            this.sid = sid;
            this.external = external;
        }
    }

    private final Map<String, AgentRecord> agents = new HashMap<>();
    private final Deque<WaitEntry> queue = new ArrayDeque<>();
    private final Map<String, WaitEntry> inFlight = new HashMap<>();
    private final Map<String, String> externalCustomers = new HashMap<>();

    public WorkforceService(Vertx vertx, UctpEventRouter router, ConversationService conversationService,
                            UctpIds ids, UctpEnvelope envelopes) {
        this.vertx = vertx;
        this.router = router;
        this.conversationService = conversationService;
        this.ids = ids;
        this.envelopes = envelopes;
        conversationService.sessionEndedListener(this::onSessionEnded);
        conversationService.conversationClosedListener(this::onConversationClosed);
    }

    /** 注册集群消费者：仅路由节点认领执行。 */
    public void registerListener() {
        vertx.eventBus().<JsonObject>consumer(ADDRESS, message -> {
            if (!isRouterNode()) {
                return;
            }
            JsonObject body = message.body();
            execute(AuthPrincipal.fromJson(body.getJsonObject("principal")), body.getJsonObject("env"));
        });
    }

    /** 处理一条 workforce.* 信封：非路由节点转发，路由节点执行。 */
    public void handle(AuthPrincipal principal, JsonObject env) {
        if (isRouterNode()) {
            execute(principal, env);
        } else {
            vertx.eventBus().publish(ADDRESS, new JsonObject()
                    .put("principal", principal.toJson())
                    .put("env", env));
        }
    }

    private boolean isRouterNode() {
        if (vertx instanceof VertxInternal internal && internal.clusterManager() != null) {
            List<String> nodes = new ArrayList<>(internal.clusterManager().getNodes());
            nodes.sort(String::compareTo);
            return !nodes.isEmpty() && nodes.get(0).equals(internal.clusterManager().getNodeId());
        }
        return true;
    }

    // ------------------------------------------------------------------
    // 命令执行（路由节点）
    // ------------------------------------------------------------------

    private synchronized void execute(AuthPrincipal principal, JsonObject env) {
        try {
            switch (UctpEnvelope.type(env)) {
                case "workforce.login" -> onLogin(principal, env);
                case "workforce.logout" -> onLogout(principal, env);
                case "workforce.status" -> onStatus(principal, env);
                case "workforce.enqueue" -> onEnqueue(principal, env);
                case "workforce.handoff" -> onHandoff(principal, env);
                case "workforce.agents" -> onAgentsQuery(principal, env);
                default -> router.deliver(new Outbound(
                        envelopes.error(env, 501, "protocol", "not-implemented"),
                        identity(principal.identityId())));
            }
        } catch (Exception e) {
            log.warn("workforce execute failed type={}", UctpEnvelope.type(env), e);
            router.deliver(new Outbound(
                    envelopes.error(env, 500, "protocol", "internal"),
                    identity(principal.identityId())));
        }
    }

    private void onLogin(AuthPrincipal principal, JsonObject env) {
        JsonObject payload = env.getJsonObject("payload", new JsonObject());
        AgentRecord agent = agents.computeIfAbsent(principal.identityId(),
                id -> new AgentRecord(id, principal.displayName(), principal.kind()));
        agent.displayName = payload.getString("display_name", agent.displayName);
        agent.skills.clear();
        agent.skills.addAll(toStringSet(payload.getJsonArray("skills", new JsonArray())));
        if (!agent.loggedIn) {
            agent.state = AgentState.AVAILABLE;
        }
        agent.loggedIn = true;
        router.deliver(new Outbound(envelopes.ack(env), identity(principal.identityId())));
        broadcastRoster();
        tryAssign();
    }

    private void onLogout(AuthPrincipal principal, JsonObject env) {
        AgentRecord agent = agents.get(principal.identityId());
        if (agent != null) {
            agent.loggedIn = false;
            agent.state = AgentState.BREAK;
        }
        router.deliver(new Outbound(envelopes.ack(env), identity(principal.identityId())));
        broadcastRoster();
    }

    private void onStatus(AuthPrincipal principal, JsonObject env) {
        AgentRecord agent = agents.get(principal.identityId());
        if (agent == null || !agent.loggedIn) {
            router.deliver(new Outbound(envelopes.error(env, 404, "protocol", "agent-not-logged-in"),
                    identity(principal.identityId())));
            return;
        }
        Object payload = env.getValue("payload");
        Object state = payload instanceof JsonObject json ? json.getValue("state") : null;
        AgentState requestedState;
        if ("available".equals(state)) {
            requestedState = AgentState.AVAILABLE;
        } else if ("busy".equals(state)) {
            requestedState = AgentState.BUSY;
        } else if ("break".equals(state)) {
            requestedState = AgentState.BREAK;
        } else {
            router.deliver(new Outbound(envelopes.error(env, 400, "protocol", "invalid-agent-state"),
                    identity(principal.identityId())));
            return;
        }
        agent.state = requestedState;
        router.deliver(new Outbound(envelopes.ack(env), identity(principal.identityId())));
        broadcastRoster();
        tryAssign();
    }

    private void onEnqueue(AuthPrincipal principal, JsonObject env) {
        JsonObject payload = env.getJsonObject("payload", new JsonObject());
        String tenantId = payload.getString("tenant_id", "default");
        String policy = payload.getString("policy", "persistent");
        long idleCloseSecs = payload.getLong("idle_close_secs", 60L);
        JsonObject metadata = payload.getJsonObject("metadata");
        String cid = ids.prefixed("cid");
        String customerPartId = ids.prefixed("pid");
        WaitEntry entry = new WaitEntry(cid, principal.identityId(),
                toStringSet(payload.getJsonArray("skills", new JsonArray())),
                payload.getInteger("priority", 0), false, null, null);
        conversationService.broadcastConvOp(new JsonObject()
                .put("op", "create").put("cid", cid)
                .put("tenant_id", tenantId).put("policy", policy)
                .put("idle_close_secs", idleCloseSecs).put("metadata", metadata))
                .compose(ignored -> conversationService.broadcastConvOp(new JsonObject()
                        .put("op", "add-participant").put("cid", cid)
                        .put("participant_id", customerPartId)
                        .put("identity_id", principal.identityId()).put("kind", principal.kind())
                        .put("role", "customer").put("display_name", principal.displayName())))
                .onComplete(result -> {
                    synchronized (this) {
                        if (result.failed()) {
                            router.deliver(new Outbound(envelopes.error(env, 503, "persistence", "enqueue-save-failed"),
                                    identity(principal.identityId())));
                            return;
                        }
                        queue.addLast(entry);
                        JsonObject queued = envelopes.replyTo(env, "workforce.queued");
                        queued.put("payload", new JsonObject().put("cid", cid)
                                .put("customer_participant_id", customerPartId)
                                .put("priority", entry.priority).put("position", positionOf(entry))
                                .put("enqueued_at", entry.enqueuedAt.toString()));
                        router.deliver(new Outbound(queued, identity(principal.identityId())));
                        tryAssign();
                    }
                });
    }

    /**
     * 外部渠道客户入队（短信/邮件首联）：本节点同步建会话 + 加外部渠道参与者 + 入队路由。
     * 返回 cid 供渠道层做 channel:address → cid 绑定。
     */
    public synchronized String enqueueExternal(String identityId, String kind, String displayName,
                                  Set<String> skills, int priority, JsonObject metadata) {
        String cid = ids.prefixed("cid");
        String partId = ids.prefixed("pid");
        conversationService.createConversation(cid, "default", "persistent", 60L, metadata);
        WaitEntry entry = new WaitEntry(cid, identityId, skills, priority, false, null, null, true);
        conversationService.broadcastConvOp(new JsonObject().put("op", "add-participant")
                .put("cid", cid).put("participant_id", partId).put("identity_id", identityId)
                .put("kind", kind).put("role", "customer").put("display_name", displayName)
                .put("channel", metadata == null ? null : metadata.getString("channel"))
                .put("address", metadata == null ? null : metadata.getString("address")))
                .onComplete(result -> {
                    synchronized (this) {
                        if (result.failed()) {
                            log.warn("external enqueue save failed cid={}", cid, result.cause());
                            return;
                        }
                        externalCustomers.put(cid, identityId);
                        queue.addLast(entry);
                        tryAssign();
                    }
                });
        return cid;
    }

    private void onHandoff(AuthPrincipal principal, JsonObject env) {
        JsonObject payload = env.getJsonObject("payload", new JsonObject());
        String cid = payload.getString("cid");
        String customerIdentityId = payload.getString("customer_identity_id");
        if (cid == null || customerIdentityId == null) {
            router.deliver(new Outbound(envelopes.error(env, 400, "protocol", "missing-cid-or-customer"),
                    identity(principal.identityId())));
            return;
        }
        AgentRecord agent = agents.get(principal.identityId());
        if (agent == null || !agent.loggedIn || !agent.assignedConversations.contains(cid) || inFlight.containsKey(cid)) {
            router.deliver(new Outbound(envelopes.error(env, 403, "protocol", "handoff-not-authorized"),
                    identity(principal.identityId())));
            return;
        }
        WaitEntry entry = new WaitEntry(
                cid,
                customerIdentityId,
                toStringSet(payload.getJsonArray("skills", new JsonArray())),
                payload.getInteger("priority", 100),
                true,
                principal.identityId(),
                payload.getString("sid"),
                customerIdentityId.equals(externalCustomers.get(cid)));
        queue.removeIf(e -> e.cid.equals(cid) && e.transfer);
        queue.addLast(entry);
        JsonObject queued = envelopes.replyTo(env, "workforce.queued");
        queued.put("payload", new JsonObject()
                .put("cid", cid)
                .put("transfer", true)
                .put("reason", payload.getString("reason", "handoff")));
        router.deliver(new Outbound(queued, identity(principal.identityId())));
        tryAssign();
    }

    private void onAgentsQuery(AuthPrincipal principal, JsonObject env) {
        JsonObject reply = envelopes.replyTo(env, "workforce.roster");
        reply.put("payload", new JsonObject().put("agents", roster()));
        router.deliver(new Outbound(reply, identity(principal.identityId())));
    }

    // ------------------------------------------------------------------
    // 路由分配
    // ------------------------------------------------------------------

    private void tryAssign() {
        List<WaitEntry> ordered = queue.stream()
                .sorted(Comparator.comparingInt((WaitEntry entry) -> entry.priority).reversed()
                        .thenComparing(entry -> entry.enqueuedAt))
                .toList();
        for (WaitEntry entry : ordered) {
            if (!entry.external && !router.identityOnline(entry.customerIdentityId)) {
                continue;
            }
            AgentRecord agent = pickAgent(entry);
            if (agent == null) {
                continue;
            }
            queue.remove(entry);
            assign(agent, entry);
        }
    }

    /** 匹配技能、在线且可接待的坐席，按负载、上次分配时间和身份排序。 */
    private AgentRecord pickAgent(WaitEntry entry) {
        return agents.values().stream()
                .filter(a -> a.loggedIn && a.state == AgentState.AVAILABLE)
                .filter(a -> a.skills.containsAll(entry.skills))
                .filter(a -> router.identityOnline(a.identityId))
                .filter(a -> !a.identityId.equals(entry.fromIdentityId))
                .filter(a -> !a.assignedConversations.contains(entry.cid) && !a.pendingConversations.contains(entry.cid))
                .min(Comparator.comparingInt((AgentRecord a) -> a.assignedConversations.size() + a.pendingConversations.size())
                        .thenComparing(a -> a.lastAssignedAt)
                        .thenComparing(a -> a.identityId))
                .orElse(null);
    }

    private void assign(AgentRecord agent, WaitEntry entry) {
        agent.pendingConversations.add(entry.cid);
        inFlight.put(entry.cid, entry);
        conversationService.broadcastConvOp(new JsonObject().put("op", "assign-agent")
                .put("cid", entry.cid).put("identity_id", agent.identityId)
                .put("customer_identity_id", entry.customerIdentityId)
                .put("kind", agent.kind).put("role", "agent").put("display_name", agent.displayName)
                .put("from_identity_id", entry.fromIdentityId).put("sid", entry.sid))
                .onComplete(result -> {
                    synchronized (this) {
                        agent.pendingConversations.remove(entry.cid);
                        if (inFlight.remove(entry.cid) != entry) return;
                        if (result.failed()) {
                            queue.addLast(entry);
                            JsonObject error = envelopes.error(null, 503, "persistence", "assignment-save-failed");
                            error.getJsonObject("payload").put("cid", entry.cid);
                            router.deliver(new Outbound(error, identity(entry.transfer ? entry.fromIdentityId : entry.customerIdentityId)));
                            broadcastRoster();
                            return;
                        }
                        agent.assignedConversations.add(entry.cid);
                        agent.lastAssignedAt = Instant.now();
                        agent.handled++;
                        AgentRecord previous = agents.get(entry.fromIdentityId);
                        if (previous != null) previous.assignedConversations.remove(entry.cid);
                        notifyAssignment(agent, entry);
                    }
                });
    }

    private void notifyAssignment(AgentRecord agent, WaitEntry entry) {
        JsonObject toCustomer = envelopes.serverEnvelope("workforce.assigned", entry.cid, null, null);
        toCustomer.put("payload", new JsonObject()
                .put("agent_identity_id", agent.identityId)
                .put("agent_display_name", agent.displayName)
                .put("transfer", entry.transfer)
                .put("assigned_at", Instant.now().toString()));
        router.deliver(new Outbound(toCustomer, identity(entry.customerIdentityId)));

        // 通知坐席
        JsonObject toAgent = envelopes.serverEnvelope("workforce.assigned", entry.cid, null, null);
        toAgent.put("payload", new JsonObject()
                .put("customer_identity_id", entry.customerIdentityId)
                .put("transfer", entry.transfer)
                .put("assigned_at", Instant.now().toString()));
        router.deliver(new Outbound(toAgent, identity(agent.identityId)));
        broadcastRoster();
    }

    private synchronized void onSessionEnded(String cid, String sid) {
        broadcastRoster();
    }

    private synchronized void onConversationClosed(String cid) {
        agents.values().forEach(agent -> {
            agent.assignedConversations.remove(cid);
            agent.pendingConversations.remove(cid);
        });
        inFlight.remove(cid);
        externalCustomers.remove(cid);
        queue.removeIf(entry -> entry.cid.equals(cid));
        broadcastRoster();
        tryAssign();
    }

    private int positionOf(WaitEntry entry) {
        int position = 0;
        for (WaitEntry e : queue) {
            if (e == entry) {
                return position;
            }
            position++;
        }
        return -1;
    }

    private JsonArray roster() {
        JsonArray array = new JsonArray();
        agents.values().stream()
                .sorted(Comparator.comparing(a -> a.identityId))
                .forEach(a -> array.add(a.toJson()));
        return array;
    }

    private void broadcastRoster() {
        if (agents.isEmpty()) {
            return;
        }
        JsonObject rosterEnv = envelopes.serverEnvelope("workforce.roster", null, null, null);
        JsonArray waiting = new JsonArray();
        queue.forEach(e -> waiting.add(new JsonObject()
                .put("cid", e.cid)
                .put("priority", e.priority)
                .put("transfer", e.transfer)
                .put("enqueued_at", e.enqueuedAt.toString())));
        rosterEnv.put("payload", new JsonObject()
                .put("agents", roster())
                .put("waiting", waiting));
        agents.keySet().forEach(identityId ->
                router.deliver(new Outbound(rosterEnv.copy(), identity(identityId))));
    }

    private static Set<String> toStringSet(JsonArray array) {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            set.add(array.getString(i));
        }
        return set;
    }
}
