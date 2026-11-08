package com.workoss.boot.vertx.contactcenter;

import com.workoss.boot.vertx.contactcenter.core.ConversationService;
import com.workoss.boot.vertx.contactcenter.core.UctpEventRouter;
import com.workoss.boot.vertx.contactcenter.routing.WorkforceService;
import com.workoss.boot.vertx.contactcenter.uctp.AuthPrincipal;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * UCTP over WebSocket 端点（fallback 载体，UCTP §4.3）：每个文本帧承载一个 JSON 信封。
 *
 * <p>连接级职责：
 * <ul>
 *   <li>auth 状态机（§5.2）：auth.hello → auth.challenge → auth.response → auth.session；
 *       开发级 bearer 校验走配置令牌表；auth.hello 携带 resume_token 时免挑战直接恢复；</li>
 *   <li>信封 v 版本校验：不支持回 505（§11.2）；</li>
 *   <li>幂等去重：按信封 id / 请求哈希重放有界的非认证响应（§3.2 / §11.3）；
 *       处理中不重执行，过期或无法完整缓存时重试返回 503；</li>
 *   <li>认证后按类型分发：workforce.* → 路由节点；其余带 cid 的信封按归属节点转发
 *       （{@link ConversationService#dispatch}）。</li>
 * </ul>
 * 媒体不经过本端点：WebSocket+WebRTC 模式下 SDP 由 ConversationService 在通话方之间中转，
 * 媒体面是浏览器之间的 PeerConnection（§10.2）。
 */
public class ContactCenterWebSocketHandler implements WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ContactCenterWebSocketHandler.class);
    private static final int MAX_IDEMPOTENT_ENTRIES = 512;
    private static final long REPLAY_TTL_MILLIS = 300_000;
    private static final int MAX_REPLY_BYTES = 256 * 1024;
    private static final int MAX_CONNECTION_REPLY_BYTES = 4 * 1024 * 1024;
    private static final int MAX_REPLY_FRAMES = 256;
    private final LongSupplier replayClock;

    private final Vertx vertx;
    private final UctpEventRouter router;
    private final ConversationService conversationService;
    private final WorkforceService workforceService;
    private final ContactCenterProperties properties;
    private final UctpEnvelope envelopes;

    /** 已签发会话令牌 → 主体（断线重连凭据）。 */
    private record AuthSession(AuthPrincipal principal, Instant expiresAt) {}

    private final Map<String, AuthSession> tokenStore = new ConcurrentHashMap<>();

    public ContactCenterWebSocketHandler(Vertx vertx, UctpEventRouter router,
                                         ConversationService conversationService,
                                         WorkforceService workforceService,
                                         ContactCenterProperties properties,
                                         UctpEnvelope envelopes) {
        this(vertx, router, conversationService, workforceService, properties, envelopes, System::currentTimeMillis);
    }

    ContactCenterWebSocketHandler(Vertx vertx, UctpEventRouter router,
                                  ConversationService conversationService, WorkforceService workforceService,
                                  ContactCenterProperties properties, UctpEnvelope envelopes, LongSupplier replayClock) {
        this.replayClock = replayClock;
        this.vertx = vertx;
        this.router = router;
        this.conversationService = conversationService;
        this.workforceService = workforceService;
        this.properties = properties;
        this.envelopes = envelopes;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        if (!originAllowed(session.getHandshakeInfo().getUri(), session.getHandshakeInfo().getHeaders().getOrigin())) {
            return session.close(CloseStatus.POLICY_VIOLATION);
        }
        ConnectionContext context = new ConnectionContext(session, replayClock);
        // 注册进广播器的本地会话表（写链串行化），出站统一走 broadcaster.sendToLocal
        router.registerLocalSession(session);
        router.registerReplyObserver(session.getId(), context::captureReply);
        log.info("uctp ws connected id={}", session.getId());
        Mono<Void> receive = session.receive()
                .doOnNext(message -> onFrame(context, message))
                .doOnError(t -> log.warn("uctp ws receive error id={}", session.getId(), t))
                .doFinally(signal -> onClose(context))
                .then();
        // 出站统一走 UctpEventRouter → ClusterWebSocketBroadcaster（串行写链），这里只挂接收流；
        // 连接保活由 VertxWebSocketSession 的 ping/pong 心跳负责
        return receive;
    }

    static boolean originAllowed(URI request, String origin) {
        if (origin == null) return true;
        try {
            URI source = URI.create(origin);
            boolean secure = List.of("https", "wss").contains(request.getScheme());
            int requestPort = request.getPort() < 0 ? (secure ? 443 : 80) : request.getPort();
            int sourcePort = source.getPort() < 0 ? ("https".equals(source.getScheme()) ? 443 : 80) : source.getPort();
            return (secure ? "https" : "http").equals(source.getScheme())
                    && request.getHost() != null && request.getHost().equalsIgnoreCase(source.getHost())
                    && requestPort == sourcePort;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private void onFrame(ConnectionContext context, WebSocketMessage message) {
        if (!message.getType().equals(WebSocketMessage.Type.TEXT)) {
            return;
        }
        if (message.getPayload().readableByteCount() > 64 * 1024) {
            sendUnobserved(context, envelopes.error(null, 413, "protocol", "envelope-too-large"));
            return;
        }
        JsonObject env;
        try {
            env = UctpEnvelope.parse(message.getPayloadAsText());
        } catch (IllegalArgumentException e) {
            sendUnobserved(context, envelopes.error(null, 400, "protocol", "malformed-envelope"));
            return;
        }
        Integer version = env.getInteger("v");
        if (version == null || version != UctpEnvelope.PROTOCOL_VERSION) {
            JsonObject error = envelopes.error(env, 505, "protocol", "version-not-supported");
            error.getJsonObject("payload").put("details", new JsonObject().put("supported",
                    List.of(UctpEnvelope.PROTOCOL_VERSION)));
            sendUnobserved(context, error);
            return;
        }
        String type = UctpEnvelope.type(env);
        if (type.startsWith("auth.")) {
            onAuthEnvelope(context, env);
            return;
        }
        if (context.principal == null || !context.expiresAt.isAfter(Instant.now())) {
            sendUnobserved(context, envelopes.error(env, 401, "auth", "unauthenticated"));
            return;
        }
        Admission admission = context.admit(env);
        if (admission.reason() != null) {
            sendUnobserved(context, envelopes.error(env, admission.conflict() ? 409 : 503,
                    "protocol", admission.reason()));
            return;
        }
        if (!admission.execute()) {
            for (byte[] reply : admission.replies()) {
                sendUnobserved(context, new JsonObject(new String(reply, StandardCharsets.UTF_8)));
            }
            return;
        }
        if (type.startsWith("workforce.")) {
            if (!"workforce.enqueue".equals(type) && !"workforce.cancel".equals(type)
                    && !List.of("agent", "supervisor").contains(context.principal.defaultRole())) {
                send(context, envelopes.error(env, 403, "policy", "agent-role-required"));
                return;
            }
            workforceService.handle(context.principal, env);
            return;
        }
        conversationService.dispatch(context.principal, env, context.session.getId());
    }

    // ------------------------------------------------------------------
    // auth 状态机（UCTP §5）
    // ------------------------------------------------------------------

    private void onAuthEnvelope(ConnectionContext context, JsonObject env) {
        switch (UctpEnvelope.type(env)) {
            case "auth.hello" -> onAuthHello(context, env);
            case "auth.response" -> onAuthResponse(context, env);
            case "auth.keepalive" -> sendUnobserved(context, context.principal != null && context.expiresAt.isAfter(Instant.now())
                    ? envelopes.ack(env) : envelopes.error(env, 401, "auth", "unauthenticated"));
            case "auth.bye" -> context.session.close().subscribe();
            default -> sendUnobserved(context, envelopes.error(env, 501, "protocol", "not-implemented"));
        }
    }

    private void onAuthHello(ConnectionContext context, JsonObject env) {
        if (context.principal != null) {
            sendUnobserved(context, envelopes.error(env, 409, "auth", "already-authenticated"));
            return;
        }
        JsonObject payload = env.getJsonObject("payload", new JsonObject());
        String resumeToken = payload.getValue("resume_token") instanceof String value ? value : null;
        if (resumeToken != null) {
            AuthSession resumed = tokenStore.get(resumeToken);
            if (resumed != null && resumed.expiresAt().isAfter(Instant.now())) {
                bind(context, resumed);
                return;
            }
            sendUnobserved(context, envelopes.error(env, 401, "auth", "resume-token-invalid"));
            return;
        }
        context.challengeNonce = UUID.randomUUID().toString();
        context.challengeIssuedAt = Instant.now();
        context.helloCapabilities = payload.getValue("capabilities") instanceof JsonObject capabilities
                ? capabilities : new JsonObject();
        JsonObject challenge = envelopes.replyTo(env, "auth.challenge");
        challenge.put("payload", new JsonObject()
                .put("nonce", context.challengeNonce)
                .put("accepted_methods", List.of("bearer"))
                .put("server_capabilities", new JsonObject()
                        .put("audio_codecs", List.of("opus", "g.711-mu"))
                        .put("transport_features", List.of("websocket-signaling", "webrtc-media"))));
        sendUnobserved(context, challenge);
    }

    private void onAuthResponse(ConnectionContext context, JsonObject env) {
        if (context.challengeNonce == null || context.principal != null
                || context.challengeIssuedAt.plusSeconds(60).isBefore(Instant.now())) {
            sendUnobserved(context, envelopes.error(env, 401, "auth", "challenge-required"));
            return;
        }
        JsonObject payload = env.getJsonObject("payload", new JsonObject());
        if (!"bearer".equals(payload.getValue("method"))) {
            sendUnobserved(context, envelopes.error(env, 400, "auth", "unsupported-auth-method"));
            return;
        }
        String credential = payload.getValue("credential") instanceof String value ? value : null;
        ContactCenterProperties.TokenSpec spec =
                credential == null ? null : properties.getTokens().get(credential);
        if (spec == null) {
            context.authFailures++;
            sendUnobserved(context, envelopes.error(env, 401, "auth", "invalid-credential"));
            if (context.authFailures >= 5) {
                context.session.close().subscribe();
            }
            return;
        }
        if (spec.getIdentityId() == null || spec.getIdentityId().isBlank()) {
            sendUnobserved(context, envelopes.error(env, 503, "auth", "identity-not-configured"));
            return;
        }
        Instant now = Instant.now();
        tokenStore.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
        if (tokenStore.size() >= 8192) {
            sendUnobserved(context, envelopes.error(env, 503, "auth", "session-capacity-exhausted"));
            return;
        }
        String token = UctpIds.sessionToken();
        AuthPrincipal principal = new AuthPrincipal(spec.getIdentityId(),
                spec.getDisplayName() != null ? spec.getDisplayName() : spec.getIdentityId(),
                spec.getKind(), spec.getRole(), context.helloCapabilities, "identified", token);
        AuthSession issued = new AuthSession(principal, now.plus(properties.getSessionTokenTtlMinutes(), ChronoUnit.MINUTES));
        tokenStore.put(token, issued);
        context.challengeNonce = null;
        bind(context, issued);
    }

    public boolean isSessionTokenValid(String token) {
        AuthSession issued = token == null ? null : tokenStore.get(token);
        return issued != null && issued.expiresAt().isAfter(Instant.now());
    }

    private void bind(ConnectionContext context, AuthSession issued) {
        AuthPrincipal principal = issued.principal();
        context.principal = principal;
        context.expiresAt = issued.expiresAt();
        router.bindIdentity(context.session.getId(), principal.identityId());
        JsonObject authSession = envelopes.serverEnvelope("auth.session", null, null, null);
        authSession.put("in_reply_to", null);
        authSession.put("payload", new JsonObject()
                .put("identity_id", principal.identityId())
                .put("participant_id", (String) null)
                .put("session_token", principal.sessionToken())
                .put("expires_at", issued.expiresAt().toString())
                .put("assurance", principal.assurance())
                .put("reachability", List.of(new JsonObject()
                        .put("transport", "websocket")
                        .put("address", properties.getPath())
                        .put("priority", 100)
                        .put("device_id", "dev_" + principal.identityId()))));
        sendUnobserved(context, authSession);
        log.info("uctp authenticated identity={} ws={}", principal.identityId(), context.session.getId());
    }

    // ------------------------------------------------------------------
    // 连接清理
    // ------------------------------------------------------------------

    private void onClose(ConnectionContext context) {
        log.info("uctp ws closed id={}", context.session.getId());
        context.clear();
        router.unregisterLocalSession(context.session.getId());
        router.unbind(context.session.getId());
        // 注意：WS 断开不等于媒体断开（P2P WebRTC 存活）；会话由 session.end / 宽限期回收
    }

    private void send(ConnectionContext context, JsonObject envelope) {
        router.deliver(UctpEnvelope.toWsSession(envelope, context.session.getId()));
    }

    private void sendUnobserved(ConnectionContext context, JsonObject envelope) {
        router.replayToLocal(context.session.getId(), envelope);
    }

    private record Admission(boolean execute, boolean conflict, String reason, List<byte[]> replies) {
        static Admission reject(String reason) { return new Admission(false, false, reason, List.of()); }
    }

    private enum ReplayState { PENDING, COMPLETE, EXPIRED, UNAVAILABLE }

    private static final class RequestEntry {
        final byte[] hash;
        final long createdAt;
        final List<byte[]> replies = new ArrayList<>();
        ReplayState state = ReplayState.PENDING;
        int bytes;

        RequestEntry(byte[] hash, long createdAt) {
            this.hash = hash;
            this.createdAt = createdAt;
        }
    }

    /** 每条 WS 连接的会话级状态；过期响应释放，但保留有界墓碑，绝不因淘汰而重执行。 */
    private static final class ConnectionContext {
        final WebSocketSession session;
        final LongSupplier clock;
        String challengeNonce;
        Instant challengeIssuedAt = Instant.EPOCH;
        Instant expiresAt = Instant.EPOCH;
        JsonObject helloCapabilities;
        AuthPrincipal principal;
        int authFailures;
        final Map<String, RequestEntry> requests = new LinkedHashMap<>();
        int replyBytes;
        boolean closed;

        ConnectionContext(WebSocketSession session, LongSupplier clock) {
            this.session = session;
            this.clock = clock;
        }

        synchronized Admission admit(JsonObject env) {
            expire();
            if (closed) return Admission.reject("idempotency-connection-closed");
            String id = env.getString("id");
            byte[] hash = requestHash(env);
            RequestEntry existing = requests.get(id);
            if (existing != null) {
                if (!Arrays.equals(hash, existing.hash)) {
                    return new Admission(false, true, "idempotency-conflict", List.of());
                }
                return switch (existing.state) {
                    case PENDING -> new Admission(false, false, null, List.of());
                    case COMPLETE -> new Admission(false, false, null, List.copyOf(existing.replies));
                    case EXPIRED -> Admission.reject("idempotency-replay-expired");
                    case UNAVAILABLE -> Admission.reject("idempotency-replay-unavailable");
                };
            }
            // At the response high-water mark, reject new commands rather than evicting old results.
            // Already-pending requests may still overflow on completion; those retain a fail-closed tombstone.
            if (requests.size() >= MAX_IDEMPOTENT_ENTRIES
                    || replyBytes > MAX_CONNECTION_REPLY_BYTES - MAX_REPLY_BYTES) {
                return Admission.reject("idempotency-capacity-exhausted");
            }
            requests.put(id, new RequestEntry(hash, clock.getAsLong()));
            return new Admission(true, false, null, List.of());
        }

        synchronized void captureReply(JsonObject envelope) {
            if (closed) return;
            expire();
            RequestEntry entry = requests.get(envelope.getString("in_reply_to"));
            if (entry == null || entry.state != ReplayState.PENDING
                    || envelope.getString("type", "").startsWith("auth.")) return;
            // Never retain credential-bearing replies, including credentials embedded in history.
            if (containsSecret(envelope)) {
                discard(entry, ReplayState.UNAVAILABLE);
                return;
            }
            byte[] wire = envelope.encode().getBytes(StandardCharsets.UTF_8);
            // Charge a fixed overhead per frame as well as the serialized bytes; frame count is bounded too.
            int charge = wire.length + 128;
            if (charge > MAX_REPLY_BYTES - entry.bytes
                    || charge > MAX_CONNECTION_REPLY_BYTES - replyBytes
                    || entry.replies.size() >= MAX_REPLY_FRAMES) {
                discard(entry, ReplayState.UNAVAILABLE);
                return;
            }
            entry.replies.add(wire);
            entry.bytes += charge;
            replyBytes += charge;
            if (List.of("ack", "error", "workforce.queued", "workforce.roster")
                    .contains(envelope.getString("type"))) entry.state = ReplayState.COMPLETE;
        }

        private void expire() {
            long now = clock.getAsLong();
            for (RequestEntry entry : requests.values()) {
                if (now - entry.createdAt >= REPLAY_TTL_MILLIS) discard(entry, ReplayState.EXPIRED);
            }
        }

        private void discard(RequestEntry entry, ReplayState state) {
            replyBytes -= entry.bytes;
            entry.bytes = 0;
            entry.replies.clear();
            entry.state = state;
        }

        synchronized void clear() {
            closed = true;
            requests.clear();
            replyBytes = 0;
        }
    }

    private static byte[] requestHash(JsonObject envelope) {
        JsonObject semantic = envelope.copy();
        semantic.remove("id");
        semantic.remove("ts"); // A retry may refresh its transport timestamp, not its command content.
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(Json.encode(canonical(semantic)).getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static Object canonical(Object value) {
        if (value instanceof JsonObject object) return canonical(object.getMap());
        if (value instanceof JsonArray array) return canonical(array.getList());
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((key, item) -> sorted.put(key.toString(), canonical(item)));
            return sorted;
        }
        if (value instanceof List<?> list) return list.stream().map(ContactCenterWebSocketHandler::canonical).toList();
        return value;
    }

    private static boolean containsSecret(Object value) {
        if (value instanceof JsonObject object) return containsSecret(object.getMap());
        if (value instanceof JsonArray array) return containsSecret(array.getList());
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> field : map.entrySet()) {
                String key = field.getKey().toString().toLowerCase(Locale.ROOT);
                if (key.contains("token") || key.contains("secret") || key.contains("password")
                        || key.contains("credential") || key.equals("authorization")
                        || containsSecret(field.getValue())) return true;
            }
        }
        if (value instanceof List<?> list) return list.stream().anyMatch(ContactCenterWebSocketHandler::containsSecret);
        return false;
    }
}
