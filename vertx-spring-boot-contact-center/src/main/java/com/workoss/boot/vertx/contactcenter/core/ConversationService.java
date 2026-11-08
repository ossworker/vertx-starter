package com.workoss.boot.vertx.contactcenter.core;

import com.workoss.boot.vertx.contactcenter.ContactCenterProperties;
import com.workoss.boot.vertx.contactcenter.channel.ChannelConnector.OutboundMessage;
import com.workoss.boot.vertx.contactcenter.channel.ChannelRegistry;
import com.workoss.boot.vertx.contactcenter.channel.ExternalBindingStore;
import com.workoss.boot.vertx.contactcenter.channel.InMemoryExternalBindingStore;
import com.workoss.boot.vertx.contactcenter.journal.JournalRepository;
import com.workoss.boot.vertx.contactcenter.journal.SqlConversationRepository;
import com.workoss.boot.vertx.contactcenter.journal.TimelineEntry;
import com.workoss.boot.vertx.contactcenter.routing.WorkforceService;
import com.workoss.boot.vertx.contactcenter.uctp.AuthPrincipal;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Outbound;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Route.identity;

/**
 * UCTP v1 consultation owner actor (not Connect v2). WebRTC SDP is signaling only.
 * Commands and asynchronous continuations run on one fixed Context; a bounded FIFO
 * per cid spans persistence. Failed writes discard working state and notifications.
 *
 * <p>v1 media contract: session.invite payload {medium: voice|video, to: [identity]}
 * requires existing speaking members and explicit session.accept/reject. A connection.offer
 * may name payload.to (required with multiple accepted peers). connection.answer forwards
 * SDP and emits connection.negotiated only. Both peers submit connection.confirm payload
 * {state: connected} from onconnectionstatechange before connection.ready/session.started.
 * connection.ice-candidate relays trickle candidates to the other leg. connection.update
 * accepts boolean mute/camera/hold/screen-share fields for the caller's own leg.
 * Ended connids/sids cannot be reused; reconnect uses a new connid, or a new invitation
 * after the grace period. Consultation recovery never resumes a persisted media leg.
 */
public class ConversationService {
    private static final Logger log = LoggerFactory.getLogger(ConversationService.class);
    public static final String NODE_ADDRESS_PREFIX = "vertx.uctp.node.";
    public static final String CONVOP_ADDRESS = "vertx.uctp.convop";
    public static final String CONVERSATION_CLOSED_ADDRESS = "vertx.uctp.conversation.closed";
    private static final String OWNERS_MAP = "uctp.conversation.owners.v1";
    static final int MAX_PENDING_PER_CID = 64;
    private static final int MAX_PENDING_TOTAL = 4096;
    private static final long REQUEST_TIMEOUT_MS = 10000;
    private static final Comparator<TimelineEntry> TIMELINE_ORDER = Comparator.comparingLong(TimelineEntry::occurredAt)
            .thenComparing(TimelineEntry::sourceId).thenComparing(TimelineEntry::kind);

    public interface MessageStore {
        Future<Boolean> append(String cid, JsonObject envelope);
        Future<List<JsonObject>> history(String cid, int limit);
        default Future<List<TimelineEntry>> timeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
            return Future.failedFuture("message-timeline-store-unavailable");
        }
    }
    public interface ConversationStore {
        Future<Void> save(JsonObject snapshot);
        default Future<Void> save(JsonObject snapshot, List<JsonObject> sessionHistory) {
            return sessionHistory.isEmpty() ? save(snapshot) : Future.failedFuture("session-history-store-unavailable");
        }
        Future<JsonObject> load(String cid);
        Future<List<JsonObject>> findByIdentity(String identity, int limit);
        default Future<List<JsonObject>> sessionHistory(String cid, int limit) {
            return Future.failedFuture("session-history-store-unavailable");
        }
        default Future<List<TimelineEntry>> sessionTimeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
            return Future.failedFuture("session-timeline-store-unavailable");
        }
    }
    private final Vertx vertx;
    private final Context context;
    private final UctpEventRouter router;
    private final ContactCenterProperties properties;
    private final UctpIds ids;
    private final UctpEnvelope envelopes;
    private final Map<String, Conversation> conversations = new ConcurrentHashMap<>();
    // Detached handles retain synchronous workforce/channel signatures; registration is queued.
    private final Map<String, Conversation> creationShells = new ConcurrentHashMap<>();
    private final Map<String, String> knownOwners = new ConcurrentHashMap<>();
    private final Map<String, ArrayDeque<Work>> queues = new HashMap<>();
    private final AtomicInteger pending = new AtomicInteger();
    /** No-SQL test/development fallback; SQL remains authoritative when installed. */
    private final Map<String, JsonObject> memoryMessageIds = new LinkedHashMap<>();
    private static final int MAX_MEMORY_MESSAGE_IDS = 1024;
    private volatile java.util.function.Consumer<String> conversationClosedListener = cid -> {};
    private volatile BiConsumer<String, String> sessionEndedListener = (cid, sid) -> {};
    private volatile ChannelRegistry channelRegistry;
    private volatile WorkforceService workforceService;
    private volatile MessageStore messageStore;
    private volatile ConversationStore conversationStore;
    private volatile ExternalBindingStore bindingStore = new InMemoryExternalBindingStore();

    public ConversationService(Vertx vertx, UctpEventRouter router, ContactCenterProperties properties,
                               UctpIds ids, UctpEnvelope envelopes) {
        this.vertx = vertx;
        this.context = vertx.getOrCreateContext();
        this.router = router;
        this.properties = properties;
        this.ids = ids;
        this.envelopes = envelopes;
    }
    public void conversationClosedListener(java.util.function.Consumer<String> listener) { conversationClosedListener = listener; }
    public void sessionEndedListener(BiConsumer<String, String> listener) { sessionEndedListener = listener; }
    public void channelRegistry(ChannelRegistry registry) { channelRegistry = registry; }
    public void workforceService(WorkforceService service) { workforceService = service; }
    public void bindingStore(ExternalBindingStore store) { bindingStore = store; }
    public void messageStore(MessageStore store) { messageStore = store; }
    public void conversationStore(ConversationStore store) { conversationStore = store; }

    public void journalRepository(JournalRepository repository) {
        messageStore = repository == null ? null : new MessageStore() {
            public Future<Boolean> append(String cid, JsonObject envelope) { return repository.append(cid, envelope); }
            public Future<List<JsonObject>> history(String cid, int limit) { return repository.history(cid, limit); }
            public Future<List<TimelineEntry>> timeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) { return repository.timeline(cid, beforeAt, beforeId, beforeKind, limit); }
        };
    }
    public void conversationRepository(SqlConversationRepository repository) {
        conversationStore = repository == null ? null : new ConversationStore() {
            public Future<Void> save(JsonObject snapshot) { return repository.save(snapshot); }
            public Future<Void> save(JsonObject snapshot, List<JsonObject> sessionHistory) { return repository.save(snapshot, sessionHistory); }
            public Future<JsonObject> load(String cid) { return repository.load(cid); }
            public Future<List<JsonObject>> findByIdentity(String identity, int limit) { return repository.findByIdentity(identity, limit); }
            public Future<List<JsonObject>> sessionHistory(String cid, int limit) { return repository.sessionHistory(cid, limit); }
            public Future<List<TimelineEntry>> sessionTimeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) { return repository.sessionTimeline(cid, beforeAt, beforeId, beforeKind, limit); }
        };
    }
    private record Work(Supplier<Future<Void>> action, Promise<Void> completion) {}
    private Future<Void> submit(String key, Supplier<Future<Void>> action) {
        Promise<Void> result = Promise.promise();
        if (pending.incrementAndGet() > MAX_PENDING_TOTAL) {
            pending.decrementAndGet();
            return Future.failedFuture(unavailable("command-queue-full"));
        }
        context.runOnContext(v -> {
            ArrayDeque<Work> queue = queues.computeIfAbsent(key, ignored -> new ArrayDeque<>());
            if (queue.size() >= MAX_PENDING_PER_CID) {
                pending.decrementAndGet(); result.fail(unavailable("conversation-queue-full")); return;
            }
            queue.addLast(new Work(action, result));
            if (queue.size() == 1) drain(key, queue);
        });
        return result.future();
    }
    private void drain(String key, ArrayDeque<Work> queue) {
        Work work = queue.getFirst();
        Future<Void> operation;
        try { operation = work.action().get(); } catch (Throwable e) { operation = Future.failedFuture(e); }
        onLoop(operation).onComplete(result -> {
            queue.removeFirst(); pending.decrementAndGet();
            if (result.succeeded()) work.completion().complete(); else work.completion().fail(result.cause());
            if (queue.isEmpty()) queues.remove(key); else context.runOnContext(v -> drain(key, queue));
        });
    }
    private <T> Future<T> onLoop(Future<T> future) {
        Promise<T> result = Promise.promise();
        future.onComplete(ar -> context.runOnContext(v -> {
            if (ar.succeeded()) result.complete(ar.result()); else result.fail(ar.cause());
        }));
        return result.future();
    }
    public void registerNodeConsumers() {
        context.runOnContext(v -> {
            vertx.eventBus().<String>consumer(CONVERSATION_CLOSED_ADDRESS, m -> conversationClosedListener.accept(m.body()));
            vertx.eventBus().<JsonObject>consumer(NODE_ADDRESS_PREFIX + selfNodeId(), m -> {
                JsonObject b = m.body();
                if ("list".equals(b.getString("action"))) {
                    submit("remote-query:" + b.getString("identity_id"), () -> localList(b.getString("identity_id"), b.getInteger("limit", 100))
                            .map(list -> { m.reply(new JsonArray(list)); return (Void) null; }))
                            .onFailure(e -> m.fail(503, "conversation-store-unavailable"));
                    return;
                }
                String cid = b.getString("cid", b.getJsonObject("env", new JsonObject()).getString("cid"));
                submit(cid, () -> resolveOwner(cid, null).compose(owner -> {
                    if (!selfNodeId().equals(owner)) return Future.failedFuture(unavailable("owner-mismatch"));
                    return "convop".equals(b.getString("action")) ? applyConvOp(b.getJsonObject("op"))
                            : execute(AuthPrincipal.fromJson(b.getJsonObject("principal")), b.getJsonObject("env"));
                })).onComplete(ar -> {
                    if (ar.succeeded()) m.reply(new JsonObject());
                    else m.fail(ar.cause() instanceof DomainException e ? e.code : 503, ar.cause().getMessage());
                });
            });
            // Backward internal publish path: exactly the directory owner claims each operation.
            vertx.eventBus().<JsonObject>consumer(CONVOP_ADDRESS, m -> {
                JsonObject op = m.body().copy();
                submit(op.getString("cid"), () -> resolveOwner(op.getString("cid"), null, "create".equals(op.getString("op"))).compose(owner ->
                        selfNodeId().equals(owner) ? applyConvOp(op) : Future.succeededFuture()))
                        .onFailure(e -> log.warn("convop failed cid={}", op.getString("cid"), e));
            });
        });
    }
    public void dispatch(AuthPrincipal principal, JsonObject envelope, String wsSessionId) {
        JsonObject env = envelope.copy();
        if (principal == null || (wsSessionId != null && !principal.identityId().equals(router.identityOf(wsSessionId)))) {
            if (principal != null) replyError(principal, env, forbidden("identity-binding-mismatch"));
            return;
        }
        // No subscription based on an untrusted cid. Domain broadcasts target authorized identities.
        String type = UctpEnvelope.type(env);
        String cid = "conversation.create".equals(type) ? ids.prefixed("cid") : env.getString("cid");
        final String key = "conversation.list".equals(type) || cid == null ? "query:" + principal.identityId() : cid;
        if ("conversation.create".equals(type)) env.put("cid", key);
        submit(key, () -> {
            if ("conversation.list".equals(type)) return onConversationList(principal, env);
            if (env.getString("cid") == null) return Future.failedFuture(badRequest("missing-cid"));
            return resolveOwner(key, null, "conversation.create".equals(type)).compose(owner -> {
                if (Set.of("message.history", "conversation.timeline").contains(type) && !clusterNodeIds().contains(owner))
                    return "message.history".equals(type) ? offlineHistory(principal, env) : offlineTimeline(principal, env);
                return selfNodeId().equals(owner) ? execute(principal, env)
                        : sendNode(owner, new JsonObject().put("principal", principal.toJson()).put("env", env));
            });
        }).onFailure(e -> replyError(principal, env, e));
    }
    void process(AuthPrincipal principal, JsonObject env) { dispatch(principal, env, null); }
    /** Hashing is only a proposal for unclaimed cids. Existing directory ownership never rehashes. */
    public String ownerNodeOf(String cid) {
        String cached = knownOwners.get(cid);
        if (cached != null) return cached;
        List<String> nodes = clusterNodeIds();
        return nodes.size() == 1 && "local".equals(nodes.getFirst()) ? null : nodes.get(Math.floorMod(cid.hashCode(), nodes.size()));
    }
    protected List<String> clusterNodeIds() {
        if (vertx instanceof VertxInternal internal && internal.clusterManager() != null) {
            List<String> nodes = new ArrayList<>(internal.clusterManager().getNodes()); nodes.sort(String::compareTo);
            if (!nodes.isEmpty()) return nodes;
        }
        return List.of(selfNodeId());
    }
    protected String selfNodeId() {
        if (vertx instanceof VertxInternal internal && internal.clusterManager() != null) return internal.clusterManager().getNodeId();
        return "local";
    }
    private Future<String> resolveOwner(String cid, String preferred) {
        return resolveOwner(cid, preferred, preferred != null);
    }
    private Future<String> resolveOwner(String cid, String preferred, boolean creating) {
        String cached = knownOwners.get(cid);
        if (cached != null) return Future.succeededFuture(cached);
        return onLoop(vertx.sharedData().<String, String>getAsyncMap(OWNERS_MAP)).compose(map -> onLoop(map.get(cid)).compose(existing -> {
            if (existing != null) { knownOwners.put(cid, existing); return Future.succeededFuture(existing); }
            ConversationStore store = conversationStore;
            Future<JsonObject> saved = store == null ? Future.succeededFuture(null) : onLoop(store.load(cid));
            return saved.compose(snapshot -> {
                if (!creating && snapshot == null && !conversations.containsKey(cid) && !creationShells.containsKey(cid))
                    return Future.failedFuture(notFound("conversation"));
                String durableOwner = snapshot == null ? null : snapshot.getJsonObject("metadata", new JsonObject()).getString("_owner_node_id");
                String proposal = durableOwner != null ? durableOwner : preferred != null ? preferred : ownerNodeOf(cid);
                final String candidate = proposal == null ? selfNodeId() : proposal;
                return onLoop(map.putIfAbsent(cid, candidate)).map(previous -> {
                    String owner = previous == null ? candidate : previous; knownOwners.put(cid, owner); return owner;
                });
            });
        }));
    }
    private Future<Void> sendNode(String owner, JsonObject body) {
        return onLoop(vertx.eventBus().<JsonObject>request(NODE_ADDRESS_PREFIX + owner, body,
                new DeliveryOptions().setSendTimeout(REQUEST_TIMEOUT_MS))).mapEmpty();
    }
    private static final class DomainException extends RuntimeException {
        final int code; final String category;
        DomainException(int code, String category, String reason) { super(reason); this.code = code; this.category = category; }
    }
    private static DomainException notFound(String what) { return new DomainException(404, "protocol", what + "-not-found"); }
    private static DomainException badRequest(String reason) { return new DomainException(400, "protocol", reason); }
    private static DomainException forbidden(String reason) { return new DomainException(403, "policy", reason); }
    private static DomainException conflict(String reason) { return new DomainException(409, "protocol", reason); }
    private static DomainException unavailable(String reason) { return new DomainException(503, "protocol", reason); }
    private void replyError(AuthPrincipal principal, JsonObject env, Throwable cause) {
        if (Vertx.currentContext() != context) {
            context.runOnContext(v -> replyError(principal, env, cause));
            return;
        }
        int code = cause instanceof DomainException e ? e.code : 503;
        if (!(cause instanceof DomainException)) log.warn("uctp {} failed cid={}", UctpEnvelope.type(env), env.getString("cid"), cause);
        String category = cause instanceof DomainException e ? e.category : "protocol";
        if (cause instanceof io.vertx.core.eventbus.ReplyException e && e.failureCode() > 0) code = e.failureCode();
        router.deliver(new Outbound(envelopes.error(env, code, category, code == 503 ? "service-unavailable" : cause.getMessage()), identity(principal.identityId())));
    }
    private final class Command {
        final Conversation conversation;
        final List<Outbound> out = new ArrayList<>();
        final List<Runnable> committed = new ArrayList<>();
        final Map<String, JsonObject> sessionHistory = new LinkedHashMap<>();
        Command(Conversation c) { conversation = c; }
        void to(JsonObject env, Participant p) { out.add(new Outbound(env, identity(p.identityId()))); }
        void broadcast(JsonObject env) { conversation.participants().values().forEach(p -> to(env.copy(), p)); }
        void ack(AuthPrincipal principal, JsonObject env) { out.add(new Outbound(envelopes.ack(env), identity(principal.identityId()))); }
        void record(JsonObject env) { conversation.appendJournal(env, properties.getJournalCapacity()); }
        void recordSession(SessionState session) { sessionHistory.put(session.sid(), session.historySnapshot()); }
    }
    private Future<Conversation> loadConversation(String cid) {
        Conversation local = conversations.get(cid);
        if (local != null) return Future.succeededFuture(local);
        Conversation shell = creationShells.get(cid);
        if (shell != null) return Future.succeededFuture(shell);
        ConversationStore store = conversationStore;
        if (store == null) return Future.failedFuture(notFound("conversation"));
        return onLoop(store.load(cid)).map(snapshot -> {
            if (snapshot == null) throw notFound("conversation");
            Conversation restored = Conversation.fromSnapshot(snapshot); restored.ownerNodeId(knownOwners.get(cid));
            conversations.put(cid, restored); return restored;
        });
    }
    private Future<Void> transaction(String cid, boolean allowClosed, boolean save, Function<Command, Future<Void>> action) {
        return loadConversation(cid).compose(original -> {
            if (original.closed() && !allowClosed) return Future.failedFuture(conflict("conversation-closed"));
            Command command = new Command(original.copy());
            Future<Void> applied;
            try { applied = action.apply(command); } catch (Throwable e) { return Future.failedFuture(e); }
            return onLoop(applied).compose(v -> {
                ConversationStore store = conversationStore;
                if (!save || store == null) return Future.succeededFuture();
                List<JsonObject> sessionHistory = new ArrayList<>(command.sessionHistory.values());
                return onLoop(sessionHistory.isEmpty() ? store.save(command.conversation.describe())
                        : store.save(command.conversation.describe(), sessionHistory));
            }).map(v -> {
                conversations.put(cid, command.conversation); creationShells.remove(cid);
                Set<String> members = new LinkedHashSet<>();
                if (!command.conversation.closed()) command.conversation.participants().values().forEach(p -> members.add(p.identityId()));
                router.conversationMembers(cid, members);
                // Post-commit notification/provider failures cannot roll back authoritative state.
                for (Runnable effect : command.committed) {
                    try { effect.run(); } catch (RuntimeException e) { log.warn("post-commit effect failed cid={}", cid, e); }
                }
                router.deliver(command.out);
                return (Void) null;
            });
        });
    }
    private Future<Void> execute(AuthPrincipal principal, JsonObject env) {
        String type = UctpEnvelope.type(env);
        if ("conversation.create".equals(type)) return onConversationCreate(principal, env);
        boolean allowClosed = Set.of("message.history", "conversation.timeline", "message.read", "message.received", "conversation.close").contains(type);
        // Message persistence is the journal commit, not an unrelated second snapshot write.
        boolean save = !Set.of("message.send", "message.history", "conversation.timeline", "message.read", "message.received", "message.typing", "capability.advertise").contains(type);
        return transaction(env.getString("cid"), allowClosed, save, command -> {
            requireParticipant(command.conversation, principal);
            switch (type) {
                case "conversation.close" -> { requireSpeaker(command.conversation, principal); close(command, "explicit-close"); command.ack(principal, env); }
                case "session.invite" -> invite(command, principal, env);
                case "session.accept" -> accept(command, principal, env);
                case "session.reject" -> reject(command, principal, env);
                case "session.cancel" -> cancel(command, principal, env);
                case "session.end" -> { SessionState s = requireSession(command.conversation, env); requireAccepted(command.conversation, s, principal); endSession(command, s, "normal-clearing"); command.ack(principal, env); }
                case "session.update" -> { SessionState s = requireSession(command.conversation, env); requireAccepted(command.conversation, s, principal); throw forbidden("role-assignment-is-workforce-only"); }
                case "connection.offer" -> offer(command, principal, env);
                case "connection.answer" -> answer(command, principal, env);
                case "connection.confirm" -> confirm(command, principal, env);
                case "connection.ice-candidate", "connection.update", "connection.end" -> connectionCommand(command, principal, env);
                case "message.send" -> { return sendMessage(command, principal, env); }
                case "message.read", "message.received" -> { return receipt(command, principal, env); }
                case "message.history" -> { return history(command, principal, env); }
                case "conversation.timeline" -> { return timeline(command, principal, env); }
                case "message.typing" -> { Participant p = requireSpeaker(command.conversation, principal); command.broadcast(envelopes.replyTo(env, type).put("payload", env.getJsonObject("payload", new JsonObject()).copy().put("from", p.participantId()))); command.ack(principal, env); }
                case "capability.advertise" -> { SessionState s = requireSession(command.conversation, env); requireSessionMember(command.conversation, s, principal); relaySession(command, s, env); command.ack(principal, env); }
                default -> throw new DomainException(501, "protocol", "not-implemented");
            }
            return Future.succeededFuture();
        });
    }
    private Participant requireParticipant(Conversation c, AuthPrincipal principal) {
        Participant p = c.participantByIdentity(principal.identityId()); if (p == null) throw forbidden("not-conversation-member"); return p;
    }
    private Participant requireSpeaker(Conversation c, AuthPrincipal principal) {
        Participant p = requireParticipant(c, principal); if (!p.canSpeak()) throw forbidden("observer-read-only"); return p;
    }
    Participant ensureParticipant(Conversation c, AuthPrincipal principal, String role) {
        Participant p = c.participantByIdentity(principal.identityId());
        if (p == null) { p = new Participant(ids.prefixed("pid"), principal.identityId(), principal.kind(), role == null ? principal.defaultRole() : role, principal.displayName()); c.addParticipant(p); }
        return p;
    }
    private Participant resolveParticipant(Conversation c, String target) {
        if (target == null) return null; Participant p = c.participant(target); return p == null ? c.participantByIdentity(target) : p;
    }
    public Conversation createConversation(String tenantId, String policy, long idleCloseSecs, JsonObject metadata) {
        return createConversation(ids.prefixed("cid"), tenantId, policy, idleCloseSecs, metadata);
    }
    public Conversation createConversation(String cid, String tenantId, String policy, long idleCloseSecs, JsonObject metadata) {
        Conversation existing = conversations.get(cid); if (existing != null) return existing;
        Conversation shell = creationShells.computeIfAbsent(cid, id -> new Conversation(id, tenantId, policy, idleCloseSecs, metadata == null ? null : metadata.copy()));
        submit(cid, () -> resolveOwner(cid, selfNodeId()).compose(owner -> {
            JsonObject op = new JsonObject().put("op", "create").put("cid", cid).put("tenant_id", tenantId).put("policy", policy).put("idle_close_secs", idleCloseSecs).put("metadata", metadata);
            return selfNodeId().equals(owner) ? applyConvOp(op) : sendNode(owner, new JsonObject().put("action", "convop").put("cid", cid).put("op", op));
        })).onSuccess(v -> { if (!selfNodeId().equals(knownOwners.get(cid))) creationShells.remove(cid); })
                .onFailure(e -> { creationShells.remove(cid); log.warn("create failed cid={}", cid, e); });
        return shell;
    }
    private Future<Void> onConversationCreate(AuthPrincipal principal, JsonObject env) {
        String cid = env.getString("cid"); if (conversations.containsKey(cid)) return Future.failedFuture(conflict("conversation-exists"));
        JsonObject p = env.getJsonObject("payload", new JsonObject());
        Conversation c = new Conversation(cid, p.getString("tenant_id", "default"), p.getString("policy", "persistent"), p.getLong("idle_close_secs", 60L), p.getJsonObject("metadata"));
        c.ownerNodeId(knownOwners.get(cid)); ensureParticipant(c, principal, "customer");
        // Clients cannot mint agent/supervisor privileges through initial_participants.
        for (Object value : p.getJsonArray("initial_participants", new JsonArray())) {
            JsonObject spec = (JsonObject) value; String identityId = spec.getString("identity_id");
            if (identityId == null) return Future.failedFuture(badRequest("missing-identity-id"));
            if (c.participantByIdentity(identityId) == null) c.addParticipant(new Participant(ids.prefixed("pid"), identityId, spec.getString("kind", "human"), "observer", spec.getString("display_name", identityId)));
        }
        creationShells.put(cid, c);
        return transaction(cid, false, true, command -> { command.broadcast(openedEnvelope(command.conversation, env.getString("id"))); return Future.succeededFuture(); })
                .onFailure(e -> creationShells.remove(cid));
    }
    private JsonObject openedEnvelope(Conversation c, String replyTo) {
        JsonObject env = envelopes.serverEnvelope(c.closed() ? "conversation.closed" : "conversation.opened", c.cid(), null, null);
        if (replyTo != null) env.put("in_reply_to", replyTo);
        JsonObject p = c.describe();
        p.remove("cid");
        p.put("opened_at", c.openedAt().toString()).put("closed_at", c.closedAt() == null ? null : c.closedAt().toString());
        p.getJsonObject("metadata").remove("_owner_node_id");
        return env.put("payload", p);
    }
    private Future<List<JsonObject>> localList(String identityId, int limit) {
        ConversationStore store = conversationStore;
        Future<List<JsonObject>> saved = store == null ? Future.succeededFuture(List.of()) : onLoop(store.findByIdentity(identityId, limit));
        return saved.compose(snapshots -> {
            Map<String, JsonObject> result = new LinkedHashMap<>();
            Map<String, JsonObject> savedByCid = new HashMap<>();
            Set<String> cids = new LinkedHashSet<>();
            snapshots.forEach(s -> {
                cids.add(s.getString("cid"));
                savedByCid.put(s.getString("cid"), s);
            });
            conversations.values().stream().filter(c -> c.participantByIdentity(identityId) != null).forEach(c -> cids.add(c.cid()));
            Future<Void> chain = Future.succeededFuture();
            for (String cid : cids) {
                chain = chain.compose(v -> resolveOwner(cid, null).compose(owner -> {
                    if (!clusterNodeIds().contains(owner)) {
                        JsonObject snapshot = savedByCid.get(cid);
                        if (snapshot == null) return Future.succeededFuture();
                        // Recovery is read-only until the stable owner returns; never claim or cache an offline owner's actor here.
                        return submit(cid, () -> addListEntry(Conversation.fromSnapshot(snapshot), identityId, limit, result));
                    }
                    return !selfNodeId().equals(owner) ? Future.succeededFuture()
                            : submit(cid, () -> loadConversation(cid).compose(c -> addListEntry(c, identityId, limit, result)));
                }));
            }
            return chain.map(v -> result.values().stream().sorted(Comparator.comparingLong((JsonObject j) -> j.getLong("opened_at")).reversed()).limit(limit).toList());
        });
    }
    private Future<Void> addListEntry(Conversation c, String identityId, int limit, Map<String, JsonObject> result) {
        Participant viewer = c.participantByIdentity(identityId);
        if (viewer == null) return Future.succeededFuture();
        return fetchHistory(c, limit).map(entries -> {
            // Internal node replies are already audience-filtered. Do not rely on caller-side filtering alone.
            JsonArray history = new JsonArray(entries.stream().filter(e -> visible(c, viewer, e)).map(JsonObject::copy).toList());
            result.put(c.cid(), c.describe().put("_history", history));
            return (Void) null;
        });
    }
    private Future<Void> onConversationList(AuthPrincipal principal, JsonObject env) {
        int limit = historyLimit(env); Map<String, JsonObject> snapshots = new LinkedHashMap<>(); Future<Void> chain = Future.succeededFuture();
        for (String node : clusterNodeIds()) {
            chain = chain.compose(v -> {
                Future<List<JsonObject>> query = selfNodeId().equals(node) ? localList(principal.identityId(), limit)
                        : onLoop(vertx.eventBus().<JsonArray>request(NODE_ADDRESS_PREFIX + node, new JsonObject().put("action", "list").put("identity_id", principal.identityId()).put("limit", limit), new DeliveryOptions().setSendTimeout(REQUEST_TIMEOUT_MS)))
                        .map(reply -> { List<JsonObject> list = new ArrayList<>(); reply.body().forEach(x -> list.add((JsonObject) x)); return list; });
                return query.map(list -> { list.forEach(s -> snapshots.put(s.getString("cid"), s)); return (Void) null; });
            });
        }
        List<Outbound> out = new ArrayList<>();
        return chain.map(v -> {
            for (JsonObject snapshot : snapshots.values().stream().limit(limit).toList()) {
                Conversation c = Conversation.fromSnapshot(snapshot); Participant member = c.participantByIdentity(principal.identityId());
                if (member == null) continue;
                out.add(new Outbound(openedEnvelope(c, env.getString("id")), identity(principal.identityId())));
                List<JsonObject> entries = new ArrayList<>();
                snapshot.getJsonArray("_history", new JsonArray()).forEach(e -> entries.add((JsonObject) e));
                appendVisibleHistory(out, c, member, entries, env.getString("id"));
            }
            out.add(new Outbound(envelopes.ack(env), identity(principal.identityId())));
            router.deliver(out);
            return (Void) null;
        });
    }
    private void close(Command command, String reason) {
        Conversation c = command.conversation; if (c.closed()) return;
        c.markClosed(); // Prevent endSession from arming an idle timer during explicit close.
        c.sessions().values().forEach(s -> endSession(command, s, "conversation-closed")); cancelIdle(command);
        JsonObject closed = envelopes.serverEnvelope("conversation.closed", c.cid(), null, null).put("payload", new JsonObject().put("reason_code", 200).put("reason", reason).put("closed_at", c.closedAt().toString()));
        command.record(closed); command.broadcast(closed);
        command.committed.add(() -> vertx.eventBus().publish(CONVERSATION_CLOSED_ADDRESS, c.cid()));
    }
    private void cancelIdle(Command command) {
        long timer = command.conversation.idleTimerId(); command.conversation.idleTimerId(-1);
        if (timer >= 0) command.committed.add(() -> vertx.cancelTimer(timer));
    }
    private void resetIdle(Command command) {
        Conversation c = command.conversation; cancelIdle(command);
        if (c.closed() || !"ephemeral".equals(c.policy()) || c.sessions().values().stream().anyMatch(s -> s.status() != SessionState.Status.ENDED)) return;
        command.committed.add(() -> c.idleTimerId(vertx.setTimer(Math.max(c.idleCloseSecs(), 5) * 1000, timer -> submit(c.cid(), () -> transaction(c.cid(), false, true, next -> {
            if (next.conversation.idleTimerId() == timer) close(next, "idle-timeout"); return Future.succeededFuture();
        })).onFailure(e -> log.warn("idle close failed cid={}", c.cid(), e)))));
    }
    private SessionState requireSession(Conversation c, JsonObject env) {
        SessionState s = env.getString("sid") == null ? null : c.sessions().get(env.getString("sid"));
        if (s == null) throw notFound("session"); if (s.status() == SessionState.Status.ENDED) throw conflict("session-ended"); return s;
    }
    private Participant requireSessionMember(Conversation c, SessionState s, AuthPrincipal principal) {
        Participant p = requireSpeaker(c, principal);
        if (!s.acceptedParticipants().contains(p.participantId()) && !s.pendingInvitees().contains(p.participantId())) throw forbidden("not-session-member"); return p;
    }
    private Participant requireAccepted(Conversation c, SessionState s, AuthPrincipal principal) {
        Participant p = requireSpeaker(c, principal); if (!s.acceptedParticipants().contains(p.participantId())) throw forbidden("not-accepted-participant"); return p;
    }
    private void invite(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; Participant initiator = requireSpeaker(c, principal); JsonObject p = env.getJsonObject("payload", new JsonObject());
        String sid = env.getString("sid"); if (sid == null || sid.isBlank()) throw badRequest("missing-sid"); if (c.sessions().containsKey(sid)) throw conflict("session-exists");
        String medium = p.getString("medium", "voice"); if (!Set.of("voice", "video").contains(medium)) throw badRequest("invalid-medium");
        JsonArray audience = p.getJsonArray("to", new JsonArray()); if (audience.isEmpty()) throw badRequest("empty-invite-audience");
        SessionState s = new SessionState(sid, c.cid(), medium, initiator.participantId()); s.acceptedParticipants().add(initiator.participantId());
        Set<String> invitees = new LinkedHashSet<>();
        for (Object target : audience) {
            Participant invitee = resolveParticipant(c, (String) target); if (invitee == null) throw notFound("invitee");
            if (!invitee.canSpeak() || invitee.participantId().equals(initiator.participantId())) throw forbidden("invalid-invitee"); invitees.add(invitee.participantId());
        }
        for (String pid : invitees) {
            s.pendingInvitees().add(pid); s.inviteEnvelopeByPartId().put(pid, env.getString("id"));
            command.to(envelopes.serverEnvelope("session.invite", c.cid(), sid, null).put("payload", p.copy().put("from", initiator.participantId())), c.participant(pid));
        }
        c.sessions().put(sid, s); cancelIdle(command); command.record(env); command.recordSession(s); command.ack(principal, env);
    }
    private void accept(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; SessionState s = requireSession(c, env); Participant p = requireSpeaker(c, principal);
        if (!s.pendingInvitees().remove(p.participantId())) throw forbidden("not-pending-invitee"); s.acceptedParticipants().add(p.participantId()); s.accept(Instant.now());
        command.to(envelopes.replyTo(env, "session.accept").put("payload", new JsonObject().put("from", p.participantId())), c.participant(s.initiatorPartId())); command.record(env); command.recordSession(s); command.ack(principal, env);
    }
    private void reject(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; SessionState s = requireSession(c, env); Participant p = requireParticipant(c, principal);
        if (!s.pendingInvitees().remove(p.participantId())) throw forbidden("not-pending-invitee");
        command.to(envelopes.replyTo(env, "session.reject").put("payload", new JsonObject().put("from", p.participantId())), c.participant(s.initiatorPartId())); command.recordSession(s);
        if (s.pendingInvitees().isEmpty() && s.acceptedParticipants().stream().noneMatch(pid -> !pid.equals(s.initiatorPartId()))) endSession(command, s, "all-invitees-declined"); command.ack(principal, env);
    }
    private void cancel(Command command, AuthPrincipal principal, JsonObject env) {
        SessionState s = requireSession(command.conversation, env); Participant p = requireSpeaker(command.conversation, principal);
        if (!p.participantId().equals(s.initiatorPartId())) throw forbidden("only-initiator-can-cancel");
        if (s.status() != SessionState.Status.INVITING || s.acceptedParticipants().size() > 1) throw conflict("already-accepted-use-session.end"); endSession(command, s, "canceled"); command.ack(principal, env);
    }
    private void relaySession(Command command, SessionState s, JsonObject env) {
        Set<String> audience = new LinkedHashSet<>(s.acceptedParticipants()); audience.addAll(s.pendingInvitees());
        audience.forEach(pid -> { Participant p = command.conversation.participant(pid); if (p != null) command.to(env.copy(), p); });
    }
    private void endSession(Command command, SessionState s, String reason) {
        if (s.status() == SessionState.Status.ENDED) return;
        long timer = s.graceTimerId(); s.graceTimerId(-1); if (timer >= 0) command.committed.add(() -> vertx.cancelTimer(timer));
        for (SessionState.ConnectionState connection : s.connections().values()) {
            if (connection.status == SessionState.ConnectionStatus.ENDED) continue;
            connection.status = SessionState.ConnectionStatus.ENDED; connection.connectedParticipants.clear();
            relayConnection(command, connection, envelopes.serverEnvelope("connection.ended", command.conversation.cid(), s.sid(), connection.connId).put("payload", new JsonObject().put("reason", reason)));
        }
        Instant endedAt = Instant.now();
        s.end(endedAt, reason);
        JsonObject ended = envelopes.serverEnvelope("session.ended", command.conversation.cid(), s.sid(), null).put("payload", new JsonObject().put("ended_at", endedAt.toString()).put("reason_code", 200).put("reason", reason));
        Set<String> endedAudience = new LinkedHashSet<>(s.acceptedParticipants());
        endedAudience.addAll(s.pendingInvitees());
        endedAudience.addAll(s.inviteEnvelopeByPartId().keySet());
        endedAudience.forEach(pid -> { Participant p = command.conversation.participant(pid); if (p != null) command.to(ended.copy(), p); });
        command.record(ended); command.recordSession(s); s.pendingInvitees().clear(); s.inviteEnvelopeByPartId().clear();
        command.committed.add(() -> sessionEndedListener.accept(command.conversation.cid(), s.sid())); resetIdle(command);
    }
    private void offer(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; SessionState s = requireSession(c, env); Participant sender = requireAccepted(c, s, principal);
        String connId = env.getString("connid"); if (connId == null || connId.isBlank()) throw badRequest("missing-connid"); if (s.connections().containsKey(connId)) throw conflict("connection-exists-use-new-connid");
        JsonObject p = env.getJsonObject("payload", new JsonObject()); Participant peer = resolveParticipant(c, p.getString("to"));
        if (peer == null && !p.containsKey("to")) {
            List<Participant> candidates = s.acceptedParticipants().stream().filter(pid -> !pid.equals(sender.participantId())).map(c::participant).filter(member -> member != null && member.canSpeak()).toList();
            if (candidates.size() != 1) throw badRequest("explicit-peer-required"); peer = candidates.getFirst();
        }
        if (peer == null || !peer.canSpeak() || peer.participantId().equals(sender.participantId()) || !s.acceptedParticipants().contains(peer.participantId())) throw forbidden("invalid-connection-peer");
        SessionState.ConnectionState connection = new SessionState.ConnectionState(connId, sender.participantId(), p.getString("substrate", "websocket+webrtc")); connection.answererPartId = peer.participantId(); s.connections().put(connId, connection);
        command.to(env.copy().put("payload", p.copy().put("from", sender.participantId())), peer); command.ack(principal, env);
    }
    private SessionState.ConnectionState requireConnection(SessionState s, JsonObject env) {
        SessionState.ConnectionState connection = env.getString("connid") == null ? null : s.connections().get(env.getString("connid"));
        if (connection == null) throw notFound("connection"); if (connection.status == SessionState.ConnectionStatus.ENDED) throw conflict("connection-ended"); return connection;
    }
    private Participant requireLeg(Conversation c, SessionState s, SessionState.ConnectionState connection, AuthPrincipal principal) {
        Participant p = requireAccepted(c, s, principal); if (!connection.covers(p.participantId())) throw forbidden("not-connection-peer"); return p;
    }
    private void relayConnection(Command command, SessionState.ConnectionState connection, JsonObject env) {
        for (String pid : List.of(connection.participantId, connection.answererPartId)) { Participant p = command.conversation.participant(pid); if (p != null) command.to(env.copy(), p); }
    }
    private void answer(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; SessionState s = requireSession(c, env); SessionState.ConnectionState connection = requireConnection(s, env); Participant p = requireLeg(c, s, connection, principal);
        if (!p.participantId().equals(connection.answererPartId)) throw forbidden("only-offered-peer-can-answer"); if (connection.status != SessionState.ConnectionStatus.OFFERED) throw conflict("connection-already-answered");
        connection.status = SessionState.ConnectionStatus.ANSWERED; command.to(env.copy(), c.participant(connection.participantId));
        relayConnection(command, connection, envelopes.serverEnvelope("connection.negotiated", c.cid(), s.sid(), connection.connId).put("payload", new JsonObject().put("substrate", connection.substrate))); command.ack(principal, env);
    }
    private void confirm(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; SessionState s = requireSession(c, env); SessionState.ConnectionState connection = requireConnection(s, env); Participant p = requireLeg(c, s, connection, principal);
        if (!"connected".equals(env.getJsonObject("payload", new JsonObject()).getString("state"))) throw badRequest("confirmation-must-be-connected"); if (connection.status == SessionState.ConnectionStatus.OFFERED) throw conflict("connection-not-negotiated");
        connection.connectedParticipants.add(p.participantId());
        if (connection.status != SessionState.ConnectionStatus.READY && connection.connectedParticipants.containsAll(Set.of(connection.participantId, connection.answererPartId))) {
            connection.status = SessionState.ConnectionStatus.READY;
            relayConnection(command, connection, envelopes.serverEnvelope("connection.ready", c.cid(), s.sid(), connection.connId).put("payload", new JsonObject().put("substrate", connection.substrate).put("ready_at", Instant.now().toString())));
            if (s.graceTimerId() >= 0) { long timer = s.graceTimerId(); s.graceTimerId(-1); command.committed.add(() -> vertx.cancelTimer(timer)); }
            if (s.status() == SessionState.Status.INVITING && s.negotiationSatisfied()) {
                s.status(SessionState.Status.ACTIVE);
                JsonObject started = envelopes.serverEnvelope("session.started", c.cid(), s.sid(), null).put("payload", new JsonObject().put("medium", s.medium()).put("started_at", Instant.now().toString()).put("participants_present", new JsonArray(new ArrayList<>(s.acceptedParticipants()))));
                JsonArray activeConnections = new JsonArray();
                s.connections().values().stream().filter(conn -> conn.status == SessionState.ConnectionStatus.READY)
                        .forEach(conn -> activeConnections.add(new JsonObject().put("connid", conn.connId)
                                .put("participant_id", conn.participantId).put("peer_participant_id", conn.answererPartId).put("transport", conn.substrate)));
                started.getJsonObject("payload").put("active_connections", activeConnections);
                s.acceptedParticipants().forEach(pid -> command.to(started.copy(), c.participant(pid)));
                command.record(started); command.recordSession(s);
            }
        }
        command.ack(principal, env);
    }
    private void connectionCommand(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; SessionState s = requireSession(c, env); SessionState.ConnectionState connection = requireConnection(s, env); Participant p = requireLeg(c, s, connection, principal);
        String type = UctpEnvelope.type(env); JsonObject payload = env.getJsonObject("payload", new JsonObject()).copy();
        if ("connection.update".equals(type)) {
            JsonObject state = connection.mediaByParticipant.computeIfAbsent(p.participantId(), ignored -> new JsonObject()); if (payload.isEmpty()) throw badRequest("empty-media-state");
            for (String key : payload.fieldNames()) {
                if (!Set.of("mute", "camera", "hold", "screen-share").contains(key) || !(payload.getValue(key) instanceof Boolean)) throw badRequest("invalid-media-state"); state.put(key, payload.getValue(key));
            }
        }
        if ("connection.end".equals(type)) { connection.status = SessionState.ConnectionStatus.ENDED; connection.connectedParticipants.clear(); }
        JsonObject relay = env.copy().put("payload", payload.put("from", p.participantId()));
        if ("connection.ice-candidate".equals(type)) command.to(relay, c.participant(p.participantId().equals(connection.participantId) ? connection.answererPartId : connection.participantId));
        else relayConnection(command, connection, relay);
        command.ack(principal, env);
        if ("connection.end".equals(type) && !s.hasLiveConnection()) {
            long old = s.graceTimerId(); if (old >= 0) command.committed.add(() -> vertx.cancelTimer(old));
            command.committed.add(() -> s.graceTimerId(vertx.setTimer(Math.max(1, properties.getReconnectGraceSeconds()) * 1000L, timer -> submit(c.cid(), () -> transaction(c.cid(), false, true, next -> {
                SessionState current = next.conversation.sessions().get(s.sid());
                if (current != null && current.status() != SessionState.Status.ENDED && current.graceTimerId() == timer && !current.hasLiveConnection()) endSession(next, current, "all-connections-ended"); return Future.succeededFuture();
            })).onFailure(e -> log.warn("session grace failed cid={} sid={}", c.cid(), s.sid(), e)))));
        }
    }
    private Future<Void> sendMessage(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; Participant sender = requireSpeaker(c, principal); JsonObject payload = env.getJsonObject("payload", new JsonObject()).copy();
        String msgId = payload.getString("msg_id", env.getString("id")); if (msgId == null || msgId.isBlank()) return Future.failedFuture(badRequest("missing-msg-id"));
        Set<String> audience = new LinkedHashSet<>(); Object to = payload.getValue("to", "all");
        if ("all".equals(to)) c.participants().values().stream().filter(p -> !p.participantId().equals(sender.participantId())).forEach(p -> audience.add(p.participantId()));
        else if (to instanceof JsonArray targets) {
            for (Object target : targets) { Participant p = resolveParticipant(c, (String) target); if (p == null) return Future.failedFuture(notFound("recipient")); audience.add(p.participantId()); }
        } else return Future.failedFuture(badRequest("invalid-message-audience"));
        payload.put("msg_id", msgId).put("from", sender.participantId()).put("to", "all".equals(to) ? "all" : new JsonArray(audience.stream().sorted().toList()));
        JsonObject delivery = envelopes.serverEnvelope("message.send", c.cid(), null, null).put("payload", payload)
                .put("_recipients", new JsonArray(audience.stream().sorted().toList()));
        JsonObject previous = c.journalSnapshot().stream().filter(e -> "message.send".equals(e.getString("type")) && msgId.equals(e.getJsonObject("payload").getString("msg_id"))).findFirst().orElse(null);
        if (previous != null && !previous.getJsonObject("payload").equals(payload)) return Future.failedFuture(conflict("msg-id-content-conflict"));
        MessageStore store = messageStore;
        Future<Boolean> appended = store == null ? appendMemory(delivery, previous) : onLoop(store.append(c.cid(), previous == null ? delivery : previous));
        return appended.recover(failure -> failure.getMessage() != null && failure.getMessage().startsWith("Conflicting journal msg_id:")
                ? Future.failedFuture(conflict("msg-id-content-conflict")) : Future.failedFuture(failure)).map(inserted -> {
            if (inserted == null) throw unavailable("invalid-journal-result");
            if (inserted) {
                command.record(delivery);
                for (String pid : audience) {
                    Participant recipient = c.participant(pid);
                    if (recipient.external() && channelRegistry != null && channelRegistry.get(recipient.channel()).isPresent()) command.committed.add(() -> channelRegistry.get(recipient.channel()).ifPresent(connector -> connector.sendMessage(new OutboundMessage(c.cid(), recipient.participantId(), recipient.identityId(), recipient.address(), msgId, payload.getString("content_type", "text/plain"), channelBody(payload.getValue("body")), sender.participantId()))));
                    else command.to(delivery.copy(), recipient);
                }
            }
            // Provider/router success cannot manufacture a delivered receipt.
            command.ack(principal, env); resetIdle(command); return (Void) null;
        });
    }
    private Future<Boolean> appendMemory(JsonObject delivery, JsonObject previous) {
        String msgId = delivery.getJsonObject("payload").getString("msg_id");
        JsonObject retained = memoryMessageIds.get(msgId);
        if (retained != null && (!retained.getString("cid").equals(delivery.getString("cid"))
                || !retained.getJsonObject("payload").equals(delivery.getJsonObject("payload"))))
            return Future.failedFuture(conflict("msg-id-content-conflict"));
        memoryMessageIds.put(msgId, delivery.copy());
        while (memoryMessageIds.size() > MAX_MEMORY_MESSAGE_IDS) memoryMessageIds.remove(memoryMessageIds.keySet().iterator().next());
        return Future.succeededFuture(retained == null && previous == null);
    }
    private static String channelBody(Object body) {
        return body == null ? null : body instanceof String text ? text : io.vertx.core.json.Json.encode(body);
    }
    private Future<List<JsonObject>> fetchHistory(Conversation c, int limit) {
        MessageStore store = messageStore; if (store != null) return onLoop(store.history(c.cid(), limit));
        List<JsonObject> entries = c.journalSnapshot().stream().filter(e -> "message.send".equals(e.getString("type"))).toList();
        return Future.succeededFuture(entries.subList(Math.max(0, entries.size() - limit), entries.size()));
    }
    private int historyLimit(JsonObject env) {
        Object value = env.getJsonObject("payload", new JsonObject()).getValue("limit", 100);
        if (!(value instanceof Number n) || n.intValue() <= 0) throw badRequest("invalid-history-limit"); return Math.min(n.intValue(), 200);
    }
    private boolean visible(Conversation c, Participant viewer, JsonObject entry) {
        if (!"message.send".equals(entry.getString("type")) || !c.cid().equals(entry.getString("cid"))) return false;
        JsonObject p = entry.getJsonObject("payload", new JsonObject()); if (viewer.participantId().equals(p.getString("from"))) return true;
        Object to = p.getValue("to"); return "all".equals(to) || to instanceof JsonArray targets && (targets.contains(viewer.participantId()) || targets.contains(viewer.identityId()));
    }
    private void appendVisibleHistory(List<Outbound> out, Conversation c, Participant viewer, List<JsonObject> entries, String replyTo) {
        for (JsonObject entry : entries) if (visible(c, viewer, entry)) out.add(new Outbound(entry.copy().put("in_reply_to", replyTo), identity(viewer.identityId())));
    }
    private Future<Void> history(Command command, AuthPrincipal principal, JsonObject env) {
        Participant viewer = requireParticipant(command.conversation, principal);
        return fetchHistory(command.conversation, historyLimit(env)).map(entries -> { appendVisibleHistory(command.out, command.conversation, viewer, entries, env.getString("id")); command.ack(principal, env); return (Void) null; });
    }
    private record TimelineCursor(long at, String sourceId, String kind) {}
    private record TimelinePage(List<TimelineEntry> entries, String nextCursor) {}
    private static final class TimelineScan {
        final Conversation conversation;
        final Participant viewer;
        final int limit;
        final List<TimelineEntry> visible = new ArrayList<>();
        TimelineCursor messageCursor;
        TimelineCursor sessionCursor;
        boolean messageDone;
        boolean sessionDone;
        TimelineScan(Conversation conversation, Participant viewer, int limit, TimelineCursor cursor) {
            this.conversation = conversation;
            this.viewer = viewer;
            this.limit = limit;
            this.messageCursor = cursor;
            this.sessionCursor = cursor;
        }
    }

    private Future<Void> timeline(Command command, AuthPrincipal principal, JsonObject env) {
        Participant viewer = requireParticipant(command.conversation, principal);
        return appendTimeline(command.out, command.conversation, viewer, env);
    }

    private Future<Void> appendTimeline(List<Outbound> out, Conversation conversation, Participant viewer, JsonObject env) {
        TimelineCursor cursor = parseTimelineCursor(env.getJsonObject("payload", new JsonObject()).getValue("cursor"));
        return scanTimeline(new TimelineScan(conversation, viewer, historyLimit(env), cursor)).map(page -> {
            for (TimelineEntry entry : page.entries()) {
                JsonObject item = envelopes.replyTo(env, "conversation.timeline.item").put("payload", new JsonObject()
                        .put("kind", entry.kind()).put("occurred_at", entry.occurredAt())
                        .put("source_id", entry.sourceId()).put("entry", entry.value().copy()));
                out.add(new Outbound(item, identity(viewer.identityId())));
            }
            JsonObject ack = envelopes.ack(env);
            if (page.nextCursor() != null) ack.getJsonObject("payload").put("next_cursor", page.nextCursor());
            out.add(new Outbound(ack, identity(viewer.identityId())));
            return (Void) null;
        });
    }

    private Future<TimelinePage> scanTimeline(TimelineScan scan) {
        if (scan.visible.size() > scan.limit || scan.messageDone && scan.sessionDone) {
            return Future.succeededFuture(timelinePage(scan));
        }
        int batch = scan.limit + 1;
        Future<List<TimelineEntry>> messages = scan.messageDone ? Future.succeededFuture(List.of())
                : messageTimeline(scan.conversation, scan.messageCursor, batch);
        return messages.compose(messageEntries -> {
            Future<List<TimelineEntry>> sessions = scan.sessionDone ? Future.succeededFuture(List.of())
                    : sessionTimeline(scan.conversation, scan.sessionCursor, batch);
            return sessions.compose(sessionEntries -> {
                scan.messageDone = messageEntries.size() < batch;
                scan.sessionDone = sessionEntries.size() < batch;
                if (!messageEntries.isEmpty()) scan.messageCursor = timelineCursor(messageEntries.getFirst());
                if (!sessionEntries.isEmpty()) scan.sessionCursor = timelineCursor(sessionEntries.getFirst());
                List<TimelineEntry> candidates = new ArrayList<>(messageEntries.size() + sessionEntries.size());
                candidates.addAll(messageEntries);
                candidates.addAll(sessionEntries);
                for (TimelineEntry entry : candidates) {
                    if (timelineVisible(scan.conversation, scan.viewer, entry)) scan.visible.add(entry);
                }
                return scanTimeline(scan);
            });
        });
    }

    private TimelinePage timelinePage(TimelineScan scan) {
        List<TimelineEntry> ordered = scan.visible.stream().sorted(TIMELINE_ORDER).toList();
        boolean more = ordered.size() > scan.limit;
        List<TimelineEntry> page = ordered.subList(Math.max(0, ordered.size() - scan.limit), ordered.size());
        String nextCursor = more ? encodeTimelineCursor(timelineCursor(page.getFirst())) : null;
        return new TimelinePage(page.stream().map(entry -> new TimelineEntry(entry.kind(), entry.occurredAt(), entry.sourceId(), entry.value().copy())).toList(), nextCursor);
    }

    private Future<List<TimelineEntry>> messageTimeline(Conversation conversation, TimelineCursor cursor, int limit) {
        MessageStore store = messageStore;
        if (store != null) {
            return onLoop(store.timeline(conversation.cid(), cursor == null ? null : cursor.at(), cursor == null ? null : cursor.sourceId(),
                    cursor == null ? null : cursor.kind(), limit));
        }
        List<TimelineEntry> entries = conversation.journalSnapshot().stream()
                .filter(entry -> "message.send".equals(entry.getString("type"))).map(entry -> {
                    JsonObject payload = entry.getJsonObject("payload", new JsonObject());
                    return new TimelineEntry("message", Instant.parse(entry.getString("ts")).toEpochMilli(), payload.getString("msg_id"), entry.copy());
                }).toList();
        return Future.succeededFuture(memoryTimeline(entries, cursor, limit));
    }

    private Future<List<TimelineEntry>> sessionTimeline(Conversation conversation, TimelineCursor cursor, int limit) {
        ConversationStore store = conversationStore;
        if (store != null) {
            return onLoop(store.sessionTimeline(conversation.cid(), cursor == null ? null : cursor.at(), cursor == null ? null : cursor.sourceId(),
                    cursor == null ? null : cursor.kind(), limit));
        }
        List<TimelineEntry> entries = conversation.sessions().values().stream().map(SessionState::historySnapshot)
                .map(snapshot -> new TimelineEntry("session", snapshot.getLong("invited_at"), snapshot.getString("sid"), snapshot)).toList();
        return Future.succeededFuture(memoryTimeline(entries, cursor, limit));
    }

    private List<TimelineEntry> memoryTimeline(List<TimelineEntry> source, TimelineCursor cursor, int limit) {
        List<TimelineEntry> entries = source.stream().filter(entry -> beforeTimelineCursor(entry, cursor)).sorted(TIMELINE_ORDER).toList();
        return entries.subList(Math.max(0, entries.size() - limit), entries.size());
    }

    private boolean timelineVisible(Conversation conversation, Participant viewer, TimelineEntry entry) {
        if ("message".equals(entry.kind())) return visible(conversation, viewer, entry.value());
        if (!"session".equals(entry.kind())) return false;
        return entry.value().getJsonArray("participant_ids", new JsonArray()).contains(viewer.participantId());
    }

    private boolean beforeTimelineCursor(TimelineEntry entry, TimelineCursor cursor) {
        if (cursor == null || entry.occurredAt() < cursor.at()) return true;
        if (entry.occurredAt() != cursor.at()) return false;
        int sourceOrder = entry.sourceId().compareTo(cursor.sourceId());
        return sourceOrder < 0 || sourceOrder == 0 && entry.kind().compareTo(cursor.kind()) < 0;
    }

    private TimelineCursor timelineCursor(TimelineEntry entry) {
        return new TimelineCursor(entry.occurredAt(), entry.sourceId(), entry.kind());
    }

    private TimelineCursor parseTimelineCursor(Object value) {
        if (value == null) return null;
        if (!(value instanceof String encoded) || encoded.isBlank() || encoded.length() > 1024) throw badRequest("invalid-history-cursor");
        try {
            JsonObject cursor = new JsonObject(new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8));
            Number at = cursor.getNumber("at");
            String sourceId = cursor.getString("source_id");
            String kind = cursor.getString("kind");
            if (cursor.getInteger("v") != 1 || at == null || at.longValue() < 0 || sourceId == null || sourceId.isBlank() || sourceId.length() > 64
                    || !Set.of("message", "session").contains(kind))
                throw new IllegalArgumentException("invalid cursor");
            return new TimelineCursor(at.longValue(), sourceId, kind);
        } catch (RuntimeException failure) {
            throw badRequest("invalid-history-cursor");
        }
    }

    private String encodeTimelineCursor(TimelineCursor cursor) {
        String json = new JsonObject().put("v", 1).put("at", cursor.at()).put("source_id", cursor.sourceId()).put("kind", cursor.kind()).encode();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    private Future<Void> offlineTimeline(AuthPrincipal principal, JsonObject env) {
        ConversationStore store = conversationStore;
        if (store == null) return Future.failedFuture(unavailable("conversation-owner-unavailable"));
        return onLoop(store.load(env.getString("cid"))).compose(snapshot -> {
            if (snapshot == null) return Future.failedFuture(notFound("conversation"));
            Conversation conversation = Conversation.fromSnapshot(snapshot);
            Participant viewer = requireParticipant(conversation, principal);
            List<Outbound> out = new ArrayList<>();
            return appendTimeline(out, conversation, viewer, env).map(v -> {
                router.deliver(out);
                return (Void) null;
            });
        });
    }

    private Future<Void> offlineHistory(AuthPrincipal principal, JsonObject env) {
        ConversationStore store = conversationStore;
        if (store == null) return Future.failedFuture(unavailable("conversation-owner-unavailable"));
        return onLoop(store.load(env.getString("cid"))).compose(snapshot -> {
            if (snapshot == null) return Future.failedFuture(notFound("conversation"));
            Conversation conversation = Conversation.fromSnapshot(snapshot);
            Participant viewer = requireParticipant(conversation, principal);
            return fetchHistory(conversation, historyLimit(env)).map(entries -> {
                List<Outbound> out = new ArrayList<>();
                appendVisibleHistory(out, conversation, viewer, entries, env.getString("id"));
                out.add(new Outbound(envelopes.ack(env), identity(principal.identityId())));
                router.deliver(out);
                return (Void) null;
            });
        });
    }
    private Set<String> receiptMessageIds(JsonObject payload) {
        JsonArray values;
        if (payload.containsKey("msg_ids")) {
            if (payload.containsKey("msg_id") || !(payload.getValue("msg_ids") instanceof JsonArray batch)
                    || batch.isEmpty() || batch.size() > 200) throw badRequest("invalid-msg-ids");
            values = batch;
        } else {
            if (!payload.containsKey("msg_id")) throw badRequest("missing-msg-id");
            values = new JsonArray().add(payload.getValue("msg_id"));
        }
        Set<String> msgIds = new LinkedHashSet<>();
        for (Object value : values) {
            if (!(value instanceof String msgId) || msgId.isBlank()) throw badRequest("invalid-msg-id");
            msgIds.add(msgId);
        }
        return msgIds;
    }
    private Future<Void> receipt(Command command, AuthPrincipal principal, JsonObject env) {
        Conversation c = command.conversation; Participant reader = requireParticipant(c, principal);
        Set<String> msgIds = receiptMessageIds(env.getJsonObject("payload", new JsonObject()));
        return fetchHistory(c, 200).map(entries -> {
            Map<String, JsonObject> messages = new LinkedHashMap<>();
            for (JsonObject entry : entries) {
                String msgId = entry.getJsonObject("payload", new JsonObject()).getString("msg_id");
                if (c.cid().equals(entry.getString("cid")) && msgIds.contains(msgId)) messages.putIfAbsent(msgId, entry);
            }
            Map<String, Participant> senders = new LinkedHashMap<>();
            // Validate the entire batch before queuing any receipt or acknowledgement.
            for (String msgId : msgIds) {
                JsonObject message = messages.get(msgId); if (message == null) throw notFound("message");
                JsonObject payload = message.getJsonObject("payload"); String senderId = payload.getString("from");
                Object recordedAudience = message.getValue("_recipients", payload.getValue("to"));
                boolean actualRecipient = recordedAudience instanceof JsonArray audience
                        && (audience.contains(reader.participantId()) || audience.contains(reader.identityId()));
                if (reader.participantId().equals(senderId) || !actualRecipient || !visible(c, reader, message)) throw forbidden("not-message-recipient");
                Participant sender = c.participant(senderId); if (sender == null) throw notFound("message-sender");
                senders.put(msgId, sender);
            }
            String type = "message.received".equals(UctpEnvelope.type(env)) ? "message.delivered" : "message.read";
            String timestamp = Instant.now().toString();
            senders.forEach((msgId, sender) -> command.to(envelopes.serverEnvelope(type, c.cid(), null, null).put("payload", new JsonObject().put("msg_id", msgId).put("by_participant", reader.participantId()).put("to_participant", reader.participantId()).put(type.equals("message.read") ? "read_at" : "delivered_at", timestamp)), sender));
            command.ack(principal, env); return (Void) null;
        });
    }
    public Future<Void> broadcastConvOp(JsonObject operation) {
        JsonObject op = operation.copy(); String cid = op.getString("cid");
        return submit(cid, () -> resolveOwner(cid, null, "create".equals(op.getString("op"))).compose(owner -> selfNodeId().equals(owner) ? applyConvOp(op) : sendNode(owner, new JsonObject().put("action", "convop").put("cid", cid).put("op", op))))
                .onFailure(e -> log.warn("convop failed cid={}", cid, e));
    }
    public List<Outbound> addParticipant(Conversation c, String pid, String identityId, String kind, String role, String displayName, String channel, String address) {
        broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", c.cid()).put("participant_id", pid).put("identity_id", identityId).put("kind", kind).put("role", role).put("display_name", displayName).put("channel", channel).put("address", address));
        return List.of(); // Owner emits membership notifications only after save.
    }
    private Future<Void> applyConvOp(JsonObject op) {
        String cid = op.getString("cid");
        if ("create".equals(op.getString("op"))) {
            if (conversations.containsKey(cid)) return Future.succeededFuture();
            ConversationStore store = conversationStore;
            Future<JsonObject> existing = store == null ? Future.succeededFuture(null) : onLoop(store.load(cid));
            return existing.compose(snapshot -> {
                if (snapshot != null) {
                    Conversation restored = Conversation.fromSnapshot(snapshot);
                    restored.ownerNodeId(knownOwners.get(cid));
                    conversations.put(cid, restored);
                    creationShells.remove(cid);
                    return Future.succeededFuture();
                }
                creationShells.computeIfAbsent(cid, id -> new Conversation(id, op.getString("tenant_id", "default"), op.getString("policy", "persistent"), op.getLong("idle_close_secs", 60L), op.getJsonObject("metadata")));
                return transaction(cid, false, true, command -> { command.conversation.ownerNodeId(knownOwners.get(cid)); return Future.succeededFuture(); })
                        .onFailure(e -> creationShells.remove(cid));
            });
        }
        return transaction(cid, false, true, command -> {
            Conversation c = command.conversation;
            switch (op.getString("op")) {
                case "add-participant" -> {
                    String identityId = op.getString("identity_id"); if (identityId == null) throw badRequest("missing-identity-id");
                    boolean added = c.participantByIdentity(identityId) == null;
                    if (added) {
                        String pid = op.getString("participant_id", ids.prefixed("pid")); if (c.participant(pid) != null) throw conflict("participant-id-exists");
                        c.addParticipant(new Participant(pid, identityId, op.getString("kind", "human"), op.getString("role", "agent"), op.getString("display_name", identityId), op.getString("channel"), op.getString("address")));
                    }
                    command.broadcast(openedEnvelope(c, null));
                    if (added) {
                        Participant newcomer = c.participantByIdentity(identityId);
                        return fetchHistory(c, 100).map(entries -> {
                            // A queued first message is still visible when an assigned agent joins later.
                            // This is authorized message catch-up, never restoration of media sessions.
                            appendVisibleHistory(command.out, c, newcomer, entries, null);
                            return (Void) null;
                        });
                    }
                }
                case "assign-agent" -> {
                    Participant customer = c.participantByIdentity(op.getString("customer_identity_id"));
                    if (customer == null || !"customer".equals(customer.role())) throw forbidden("assignment-customer-not-member");
                    String identityId = op.getString("identity_id");
                    if (identityId == null || identityId.isBlank()) throw badRequest("missing-identity-id");
                    if (!"agent".equals(op.getString("role", "agent"))) throw badRequest("invalid-role");
                    String fromIdentityId = op.getString("from_identity_id");
                    Participant previous = null;
                    if (fromIdentityId != null) {
                        if (fromIdentityId.isBlank()) throw badRequest("invalid-from-identity-id");
                        if (fromIdentityId.equals(identityId)) throw conflict("same-assignment-identity");
                        previous = c.participantByIdentity(fromIdentityId);
                        if (previous == null) throw notFound("participant");
                    }
                    Participant target = c.participantByIdentity(identityId); boolean added = target == null;
                    if (added) {
                        String pid = ids.prefixed("pid"); if (c.participant(pid) != null) throw conflict("participant-id-exists");
                        target = new Participant(pid, identityId, op.getString("kind", "human"), "agent", op.getString("display_name", identityId));
                        c.addParticipant(target);
                    }
                    changeRole(command, target, "agent", op.getString("sid"));
                    if (previous != null) changeRole(command, previous, "observer", op.getString("sid"));
                    command.broadcast(openedEnvelope(c, null));
                    if (added) {
                        Participant newcomer = target;
                        return fetchHistory(c, 100).map(entries -> {
                            appendVisibleHistory(command.out, c, newcomer, entries, null);
                            return (Void) null;
                        });
                    }
                }
                case "role-change" -> {
                    Participant target = resolveParticipant(c, op.getString("participant_id", op.getString("identity_id"))); if (target == null) throw notFound("participant");
                    changeRole(command, target, op.getString("new_role"), op.getString("sid"));
                }
                default -> throw badRequest("unknown-convop");
            }
            return Future.succeededFuture();
        });
    }
    private void changeRole(Command command, Participant target, String role, String sid) {
        Conversation c = command.conversation;
        if (!Set.of("customer", "agent", "supervisor", "observer").contains(role)) throw badRequest("invalid-role");
        target.role(role);
        command.broadcast(envelopes.serverEnvelope("session.update", c.cid(), sid, null).put("payload", new JsonObject().put("kind", "role-change").put("details", new JsonObject().put("participant_id", target.participantId()).put("identity_id", target.identityId()).put("new_role", role))));
        if (target.canSpeak()) return;
        for (SessionState s : c.sessions().values()) {
            if (s.status() == SessionState.Status.ENDED) continue;
            for (SessionState.ConnectionState connection : s.connections().values()) {
                if (connection.covers(target.participantId()) && connection.status != SessionState.ConnectionStatus.ENDED) {
                    connection.status = SessionState.ConnectionStatus.ENDED; connection.connectedParticipants.clear();
                    relayConnection(command, connection, envelopes.serverEnvelope("connection.ended", c.cid(), s.sid(), connection.connId).put("payload", new JsonObject().put("reason", "role-change")));
                }
            }
            s.pendingInvitees().remove(target.participantId()); s.acceptedParticipants().remove(target.participantId());
            if (s.pendingInvitees().isEmpty() && s.acceptedParticipants().size() < 2) endSession(command, s, "role-change");
        }
    }
    public Future<List<JsonObject>> sessionHistory(String cid, int limit) {
        if (limit < 1) return Future.failedFuture(badRequest("invalid-history-limit"));
        int bounded = Math.min(limit, 200);
        ConversationStore store = conversationStore;
        if (store != null) return onLoop(store.sessionHistory(cid, bounded));
        Conversation conversation = conversations.get(cid);
        if (conversation == null) return Future.failedFuture(notFound("conversation"));
        List<JsonObject> history = conversation.sessions().values().stream().map(SessionState::historySnapshot)
                .sorted(Comparator.comparingLong((JsonObject session) -> session.getLong("invited_at"))
                        .thenComparing(session -> session.getString("sid"))).toList();
        return Future.succeededFuture(history.subList(Math.max(0, history.size() - bounded), history.size()));
    }
    public boolean identityOnline(String identityId) { return router.identityOnline(identityId); }
    public Conversation conversation(String cid) { Conversation c = conversations.get(cid); return c == null ? creationShells.get(cid) : c; }
    /** Existing channel API; internal membership commands precede the first message in the cid FIFO. */
    public String ingestExternalMessage(String channel, String address, String displayName, String contentType, String body, String externalMsgId) {
        String identityId = "ext:" + channel + ":" + address;
        AuthPrincipal principal = new AuthPrincipal(identityId, displayName, "external", "customer", new JsonObject(), "identified", null);
        String cid = bindingStore.find(channel, address); Conversation existing = cid == null ? null : conversation(cid);
        if (existing == null || existing.closed()) {
            if (workforceService == null) throw unavailable("workforce-unavailable");
            JsonObject metadata = new JsonObject().put("channel", channel).put("address", address).put("source", "external-" + channel);
            cid = workforceService.enqueueExternal(identityId, "external:" + channel, displayName == null ? address : displayName, Set.of("general"), 0, metadata);
            bindingStore.bind(channel, address, cid);
        }
        String mutationId = externalMsgId == null ? ids.prefixed("msgid") : "extmsg_" + channel + "_" + externalMsgId;
        JsonObject env = new JsonObject().put("v", 1).put("type", "message.send").put("id", mutationId).put("ts", Instant.now().toString()).put("cid", cid).put("payload", new JsonObject().put("msg_id", mutationId).put("to", "all").put("content_type", contentType == null ? "text/plain" : contentType).put("body", body).put("body_encoding", "utf8"));
        dispatch(principal, env, null); return cid;
    }
}
