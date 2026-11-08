package com.workoss.boot.vertx.contactcenter;

import com.workoss.boot.vertx.contactcenter.core.ConversationService;
import com.workoss.boot.vertx.contactcenter.core.UctpEventRouter;
import com.workoss.boot.vertx.contactcenter.routing.WorkforceService;
import com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.Disposable;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ContactCenterWebSocketHandlerTest {
    private UctpEventRouter router;
    private ConversationService conversations;
    private WorkforceService workforce;
    private ContactCenterProperties properties;
    private ContactCenterWebSocketHandler handler;
    private List<JsonObject> replies;
    private UctpEnvelope envelopes;
    private final Map<String, Consumer<JsonObject>> observers = new HashMap<>();
    private final AtomicLong replayTime = new AtomicLong(System.currentTimeMillis());

    @BeforeEach
    void setup() {
        router = mock(UctpEventRouter.class);
        conversations = mock(ConversationService.class);
        workforce = mock(WorkforceService.class);
        properties = new ContactCenterProperties();
        ContactCenterProperties.TokenSpec customer = new ContactCenterProperties.TokenSpec();
        customer.setIdentityId("customer-fixture");
        customer.setRole("customer");
        properties.getTokens().put("fixture-customer", customer);
        long now = System.currentTimeMillis();
        envelopes = new UctpEnvelope(new UctpIds(new SnowflakeGenerator(
                new SnowflakeGenerator.Window(1, "fixture", now, now + 60_000))));
        handler = new ContactCenterWebSocketHandler(mock(Vertx.class), router, conversations, workforce, properties,
                envelopes, replayTime::get);
        replies = new ArrayList<>();
        doAnswer(call -> {
            observers.put(call.getArgument(0), call.getArgument(1));
            return null;
        }).when(router).registerReplyObserver(anyString(), any());
        doAnswer(call -> {
            observers.remove(call.<String>getArgument(0));
            return null;
        }).when(router).unregisterLocalSession(anyString());
        doAnswer(call -> {
            UctpEnvelope.Outbound outbound = call.getArgument(0);
            replies.add(outbound.envelope().copy());
            if (outbound.route() instanceof UctpEnvelope.Route.WsSessionTarget target) {
                observe(target.wsSessionId(), outbound.envelope());
            } else {
                List.copyOf(observers.keySet()).forEach(ws -> observe(ws, outbound.envelope()));
            }
            return null;
        }).when(router).deliver(any(UctpEnvelope.Outbound.class));
        doAnswer(call -> {
            replies.add(call.<JsonObject>getArgument(1).copy());
            return null;
        }).when(router).replayToLocal(anyString(), any());
    }

    @Test
    void onlySameOriginIsAllowedForBrowserConnections() {
        URI request = URI.create("ws://localhost:18080/uctp");
        assertTrue(ContactCenterWebSocketHandler.originAllowed(request, "http://localhost:18080"));
        assertTrue(ContactCenterWebSocketHandler.originAllowed(request, null));
        assertFalse(ContactCenterWebSocketHandler.originAllowed(request, "http://localhost:8080"));
        assertFalse(ContactCenterWebSocketHandler.originAllowed(request, "http://untrusted.invalid:18080"));
        assertFalse(ContactCenterWebSocketHandler.originAllowed(request, "null"));
        assertFalse(ContactCenterWebSocketHandler.originAllowed(request, "{malformed"));
        assertTrue(ContactCenterWebSocketHandler.originAllowed(URI.create("wss://localhost/uctp"), "https://localhost"));
        assertFalse(ContactCenterWebSocketHandler.originAllowed(URI.create("wss://localhost/uctp"), "http://localhost"));
    }

    @Test
    void unauthenticatedAndUnchallengedRequestsNeverDispatch() {
        receive(envelope("message.send"), response());
        assertEquals(List.of(401, 401), codes());
        verifyNoInteractions(conversations, workforce);
        assertFalse(handler.isSessionTokenValid(null));
        assertFalse(handler.isSessionTokenValid("unknown-fixture"));
    }

    @Test
    void authenticatesOnceAndBlocksCustomerAgentOperations() {
        receive(envelope("auth.hello"), response(), envelope("workforce.status"), envelope("message.send"),
                envelope("auth.hello"));
        assertEquals(List.of(403, 409), codes());
        JsonObject issued = replies.stream().filter(reply -> "auth.session".equals(reply.getString("type")))
                .findFirst().orElseThrow();
        assertEquals("customer-fixture", issued.getJsonObject("payload").getString("identity_id"));
        assertTrue(handler.isSessionTokenValid(issued.getJsonObject("payload").getString("session_token")));
        verify(workforce, never()).handle(any(), any());
        verify(conversations).dispatch(any(), any(), eq("fixture-ws"));
        verify(router).bindIdentity("fixture-ws", "customer-fixture");
    }

    @Test
    void expiredSessionAndResumeAreRejected() {
        properties.setSessionTokenTtlMinutes(-1);
        receive(envelope("auth.hello"), response(), envelope("auth.keepalive"));
        String issued = replies.stream().filter(reply -> "auth.session".equals(reply.getString("type")))
                .findFirst().orElseThrow().getJsonObject("payload").getString("session_token");
        assertFalse(handler.isSessionTokenValid(issued));
        receive(envelope("auth.hello").put("payload", new JsonObject().put("resume_token", issued)));
        assertEquals(List.of(401, 401), codes());
    }

    @Test
    void malformedAndOversizedFramesDoNotPreventNextValidChallenge() {
        receiveRaw("{broken", envelope("message.send").put("payload", new JsonObject().put("text", "x".repeat(65536))).encode(),
                envelope("auth.hello").encode());
        assertEquals(List.of(400, 413), codes());
        assertTrue(replies.stream().anyMatch(reply -> "auth.challenge".equals(reply.getString("type"))));
        verifyNoInteractions(conversations, workforce);
    }

    @Test
    void missingConfiguredIdentityCannotLeakCredential() {
        properties.getTokens().get("fixture-customer").setIdentityId(null);
        receive(envelope("auth.hello"), response());
        assertEquals(List.of(503), codes());
        assertTrue(replies.stream().noneMatch(reply -> "auth.session".equals(reply.getString("type"))));
    }

    @Test
    void asynchronousPendingRetryExecutesOnceThenReplaysOriginalAck() {
        JsonObject request = envelope("message.send").put("cid", "c");
        try (LiveConnection ws = open("fixture-ws")) {
            ws.send(request);
            ws.send(request.copy());
            verify(conversations, times(1)).dispatch(any(), any(), eq("fixture-ws"));
            assertTrue(related(request).isEmpty());
            JsonObject ack = envelopes.ack(request);
            router.deliver(UctpEnvelope.toIdentity(ack, "customer-fixture"));
            ws.send(request.copy().put("ts", Instant.now().plusSeconds(1).toString()));
            assertEquals(List.of(ack, ack), related(request));
            verify(conversations, times(1)).dispatch(any(), any(), eq("fixture-ws"));
        }
    }

    @Test
    void queuedAndRosterAreTerminalRepliesNotPendingForever() {
        properties.getTokens().get("fixture-customer").setRole("agent");
        try (LiveConnection ws = open("fixture-ws")) {
            for (String type : List.of("workforce.enqueue", "workforce.list")) {
                JsonObject request = envelope(type);
                ws.send(request);
                ws.send(request.copy());
                assertTrue(related(request).isEmpty());
                String replyType = type.equals("workforce.enqueue") ? "workforce.queued" : "workforce.roster";
                router.deliver(UctpEnvelope.toIdentity(envelopes.replyTo(request, replyType), "customer-fixture"));
                ws.send(request.copy());
                List<JsonObject> results = related(request);
                assertEquals(2, results.size());
                assertEquals(results.get(0), results.get(1));
            }
            verify(workforce, times(2)).handle(any(), any());
        }
    }

    @Test
    void sameIdWithChangedPayloadTypeOrConversationIs409AndCannotPoisonPendingAck() {
        JsonObject request = envelope("message.send").put("cid", "c")
                .put("payload", new JsonObject().put("text", "original"));
        try (LiveConnection ws = open("fixture-ws")) {
            ws.send(request);
            ws.send(request.copy().put("payload", new JsonObject().put("text", "different")));
            ws.send(request.copy().put("type", "message.read"));
            ws.send(request.copy().put("cid", "other"));
            assertEquals(List.of(409, 409, 409), codes());
            JsonObject ack = envelopes.ack(request);
            router.deliver(UctpEnvelope.toIdentity(ack, "customer-fixture"));
            ws.send(request.copy());
            ws.send(request.copy().put("payload", new JsonObject().put("text", "changed-after-completion")));
            assertEquals(List.of(409, 409, 409, 409), codes());
            assertEquals(List.of(ack, ack), related(request).stream()
                    .filter(reply -> "ack".equals(reply.getString("type"))).toList());
            verify(conversations, times(1)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void reorderedJsonKeysAreTheSameSemanticRequestAndErrorsReplay() {
        JsonObject request = envelope("message.send").put("payload", new JsonObject()
                .put("a", new JsonObject().put("x", 1).put("y", 2)).put("b", 3));
        doAnswer(call -> {
            router.deliver(UctpEnvelope.toIdentity(envelopes.error(call.getArgument(1), 403, "policy", "denied"),
                    "customer-fixture"));
            return null;
        }).when(conversations).dispatch(any(), any(), anyString());
        try (LiveConnection ws = open("fixture-ws")) {
            ws.send(request);
            ws.send(request.copy().put("payload", new JsonObject().put("b", 3)
                    .put("a", new JsonObject().put("y", 2).put("x", 1))));
            assertEquals(List.of(403, 403), codes());
            assertEquals(related(request).get(0), related(request).get(1));
            verify(conversations, times(1)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void historyReplaysAllFramesOnlyToRequestingConnectionAndIgnoresUnrelatedAndAuthFrames() {
        JsonObject request = envelope("history.get").put("cid", "c");
        try (LiveConnection first = open("first"); LiveConnection second = open("second")) {
            first.send(request);
            JsonObject frame1 = envelopes.replyTo(request, "message.send").put("payload", new JsonObject().put("text", "one"));
            JsonObject frame2 = envelopes.replyTo(request, "message.send").put("payload", new JsonObject().put("text", "two"));
            JsonObject ack = envelopes.ack(request);
            router.deliver(UctpEnvelope.toIdentity(frame1, "customer-fixture"));
            first.send(request.copy()); // Partial history must not be replayed or redispatched.
            assertEquals(1, related(request).size());
            router.deliver(UctpEnvelope.toIdentity(envelopes.serverEnvelope("message.typing", "c", null, null), "customer-fixture"));
            router.deliver(UctpEnvelope.toIdentity(envelopes.replyTo(request, "auth.session")
                    .put("payload", new JsonObject().put("session_token", "synthetic-secret")), "customer-fixture"));
            router.deliver(UctpEnvelope.toIdentity(frame2, "customer-fixture"));
            router.deliver(UctpEnvelope.toIdentity(ack, "customer-fixture"));
            clearInvocations(router);
            replies.clear();
            frame1.getJsonObject("payload").put("text", "mutated-after-send");
            first.send(request.copy());
            assertEquals(List.of("one", "two"), replies.stream().filter(e -> "message.send".equals(e.getString("type")))
                    .map(e -> e.getJsonObject("payload").getString("text")).toList());
            assertEquals(3, replies.size());
            verify(router, times(3)).replayToLocal(eq("first"), any());
            verify(router, never()).replayToLocal(eq("second"), any());
            verify(conversations, times(1)).dispatch(any(), any(), eq("first"));
            // Same identity on another WS had no pending request, so received history was not cached there.
            second.send(request.copy());
            verify(conversations).dispatch(any(), any(), eq("second"));
        }
    }

    @Test
    void full512EntryCacheNeverClearsOrReexecutesExistingRequests() {
        doAnswer(call -> {
            router.deliver(UctpEnvelope.toIdentity(envelopes.ack(call.getArgument(1)), "customer-fixture"));
            return null;
        }).when(conversations).dispatch(any(), any(), anyString());
        try (LiveConnection ws = open("fixture-ws")) {
            JsonObject first = envelope("message.send");
            ws.send(first);
            for (int i = 1; i < 512; i++) ws.send(envelope("message.send"));
            ws.send(envelope("message.send"));
            ws.send(first.copy());
            assertEquals(List.of(503), codes());
            assertEquals("idempotency-capacity-exhausted", replies.get(replies.size() - 2).getJsonObject("payload").getString("reason"));
            assertEquals(2, related(first).size());
            verify(conversations, times(512)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void fullPendingCacheStillCapturesTheFirstCompletionAndNeverExecutesThe513thCommand() {
        try (LiveConnection ws = open("fixture-ws")) {
            JsonObject first = envelope("message.send");
            ws.send(first);
            for (int i = 1; i < 512; i++) ws.send(envelope("message.send"));
            ws.send(envelope("message.send"));
            ws.send(first.copy());
            assertEquals(List.of(503), codes());
            assertTrue(related(first).isEmpty());
            JsonObject ack = envelopes.ack(first);
            router.deliver(UctpEnvelope.toIdentity(ack, "customer-fixture"));
            ws.send(first.copy());
            assertEquals(List.of(ack, ack), related(first));
            verify(conversations, times(512)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void combinedHistoryFramesOverThePerRequestBudgetNeverReplayAPartialHistory() {
        try (LiveConnection ws = open("fixture-ws")) {
            JsonObject request = envelope("history.get");
            ws.send(request);
            for (int i = 0; i < 2; i++) router.deliver(UctpEnvelope.toIdentity(envelopes.replyTo(request, "message.send")
                    .put("payload", new JsonObject().put("text", "x".repeat(130 * 1024))), "customer-fixture"));
            router.deliver(UctpEnvelope.toIdentity(envelopes.ack(request), "customer-fixture"));
            replies.clear();
            ws.send(request.copy());
            assertEquals(1, replies.size());
            assertEquals(List.of(503), codes());
            assertEquals("idempotency-replay-unavailable", replies.get(0).getJsonObject("payload").getString("reason"));
            verify(conversations, times(1)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void expiredCompletedAndPendingRequestsFailClosedWithoutReexecution() {
        try (LiveConnection ws = open("fixture-ws")) {
            JsonObject completed = envelope("message.send"), pending = envelope("history.get");
            ws.send(completed);
            router.deliver(UctpEnvelope.toIdentity(envelopes.ack(completed), "customer-fixture"));
            ws.send(pending);
            replayTime.addAndGet(300_001);
            ws.send(completed.copy());
            ws.send(pending.copy());
            assertEquals(List.of(503, 503), codes());
            assertTrue(replies.stream().filter(e -> "error".equals(e.getString("type")))
                    .allMatch(e -> "idempotency-replay-expired".equals(e.getJsonObject("payload").getString("reason"))));
            router.deliver(UctpEnvelope.toIdentity(envelopes.ack(pending), "customer-fixture"));
            ws.send(pending.copy());
            assertEquals(503, codes().get(2));
            verify(conversations, times(2)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void oversizedHistoryAndSecretRepliesCannotBeReplayedOrReexecuted() {
        try (LiveConnection ws = open("fixture-ws")) {
            for (JsonObject payload : List.of(new JsonObject().put("text", "x".repeat(256 * 1024)),
                    new JsonObject().put("nested", new JsonObject().put("session_token", "synthetic-secret")))) {
                JsonObject request = envelope("history.get");
                ws.send(request);
                router.deliver(UctpEnvelope.toIdentity(envelopes.replyTo(request, "message.send").put("payload", payload), "customer-fixture"));
                router.deliver(UctpEnvelope.toIdentity(envelopes.ack(request), "customer-fixture"));
                replies.clear();
                ws.send(request.copy());
                assertEquals(List.of(503), codes());
                assertEquals("idempotency-replay-unavailable", replies.get(0).getJsonObject("payload").getString("reason"));
            }
            verify(conversations, times(2)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void aggregateReplyBudgetAndFrameCountAreBoundedWithoutEvictingOtherResults() {
        try (LiveConnection ws = open("fixture-ws")) {
            List<JsonObject> pending = new ArrayList<>();
            for (int i = 0; i < 17; i++) {
                JsonObject request = envelope("history.get");
                pending.add(request);
                ws.send(request);
            }
            JsonObject retained = pending.get(0);
            for (int i = 0; i < pending.size(); i++) {
                JsonObject request = pending.get(i);
                router.deliver(UctpEnvelope.toIdentity(envelopes.replyTo(request, "message.send")
                        .put("payload", new JsonObject().put("text", "x".repeat(250 * 1024))), "customer-fixture"));
                router.deliver(UctpEnvelope.toIdentity(envelopes.ack(request), "customer-fixture"));
                if (i == 16) {
                    replies.clear();
                    ws.send(request.copy());
                    assertEquals(List.of(503), codes());
                }
            }
            replies.clear();
            ws.send(envelope("message.send"));
            assertEquals(List.of(503), codes());
            assertEquals("idempotency-capacity-exhausted", replies.get(0).getJsonObject("payload").getString("reason"));
            verify(conversations, times(17)).dispatch(any(), any(), anyString());
            replies.clear();
            ws.send(retained.copy());
            assertEquals(2, replies.size());
            assertTrue(codes().isEmpty());
            // Release large frames by TTL before exercising the independent frame-count bound.
            replayTime.addAndGet(300_001);
            JsonObject manyFrames = envelope("history.get");
            ws.send(manyFrames);
            for (int i = 0; i < 257; i++) router.deliver(UctpEnvelope.toIdentity(
                    envelopes.replyTo(manyFrames, "message.send"), "customer-fixture"));
            replies.clear();
            ws.send(manyFrames.copy());
            assertEquals(List.of(503), codes());
            verify(conversations, times(18)).dispatch(any(), any(), anyString());
        }
    }

    @Test
    void disconnectReleasesObserverAndCacheEvenIfAnOutboundCallbackWasAlreadyCaptured() {
        JsonObject request = envelope("message.send");
        LiveConnection ws = open("fixture-ws");
        ws.send(request);
        Consumer<JsonObject> callback = observers.get("fixture-ws");
        ws.close();
        assertTrue(observers.isEmpty());
        verify(router).unregisterLocalSession("fixture-ws");
        verify(router).unbind("fixture-ws");
        callback.accept(envelopes.ack(request)); // Raced callback after close is harmless.
        try (LiveConnection next = open("fixture-ws")) {
            next.send(request.copy());
            verify(conversations, times(2)).dispatch(any(), any(), eq("fixture-ws"));
        }
    }

    private void observe(String ws, JsonObject envelope) {
        Consumer<JsonObject> observer = observers.get(ws);
        if (observer != null) observer.accept(envelope.copy());
    }

    private List<JsonObject> related(JsonObject request) {
        return replies.stream().filter(reply -> request.getString("id").equals(reply.getString("in_reply_to"))).toList();
    }

    private LiveConnection open(String id) {
        LiveConnection connection = new LiveConnection(id);
        connection.send(envelope("auth.hello"));
        connection.send(response());
        return connection;
    }

    private final class LiveConnection implements AutoCloseable {
        private final Sinks.Many<WebSocketMessage> input = Sinks.many().unicast().onBackpressureBuffer();
        private final Disposable subscription;

        LiveConnection(String id) {
            WebSocketSession session = mock(WebSocketSession.class);
            when(session.getId()).thenReturn(id);
            when(session.getHandshakeInfo()).thenReturn(new HandshakeInfo(URI.create("ws://localhost:18080/uctp"),
                    new HttpHeaders(), Mono.empty(), null));
            when(session.receive()).thenReturn(input.asFlux());
            when(session.close()).thenReturn(Mono.empty());
            subscription = handler.handle(session).subscribe();
        }

        void send(JsonObject envelope) {
            input.emitNext(new WebSocketMessage(WebSocketMessage.Type.TEXT,
                    DefaultDataBufferFactory.sharedInstance.wrap(envelope.encode().getBytes(java.nio.charset.StandardCharsets.UTF_8))),
                    Sinks.EmitFailureHandler.FAIL_FAST);
        }

        @Override public void close() { subscription.dispose(); }
    }

    private List<Integer> codes() {
        return replies.stream().filter(reply -> "error".equals(reply.getString("type")))
                .map(reply -> reply.getJsonObject("payload").getInteger("code")).toList();
    }

    private void receive(JsonObject... envelopes) {
        receiveRaw(java.util.Arrays.stream(envelopes).map(JsonObject::encode).toArray(String[]::new));
    }

    private void receiveRaw(String... frames) {
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.getId()).thenReturn("fixture-ws");
        when(session.getHandshakeInfo()).thenReturn(new HandshakeInfo(URI.create("ws://localhost:18080/uctp"),
                new HttpHeaders(), Mono.empty(), null));
        when(session.close()).thenReturn(Mono.empty());
        when(session.receive()).thenReturn(Flux.fromArray(frames).map(frame -> new WebSocketMessage(
                WebSocketMessage.Type.TEXT, DefaultDataBufferFactory.sharedInstance.wrap(frame.getBytes(java.nio.charset.StandardCharsets.UTF_8)))));
        handler.handle(session).block();
    }

    private static JsonObject response() {
        return envelope("auth.response").put("payload", new JsonObject().put("method", "bearer").put("credential", "fixture-customer"));
    }

    private static JsonObject envelope(String type) {
        return new JsonObject().put("v", 1).put("type", type).put("id", "fixture-" + java.util.UUID.randomUUID())
                .put("ts", Instant.now().toString()).put("payload", new JsonObject());
    }
}
