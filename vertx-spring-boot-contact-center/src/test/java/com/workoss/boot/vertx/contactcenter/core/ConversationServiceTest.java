package com.workoss.boot.vertx.contactcenter.core;

import com.workoss.boot.vertx.contactcenter.ContactCenterProperties;
import com.workoss.boot.vertx.contactcenter.journal.JournalRepository;
import com.workoss.boot.vertx.contactcenter.journal.SqlConversationRepository;
import com.workoss.boot.vertx.contactcenter.journal.TimelineEntry;
import com.workoss.boot.vertx.contactcenter.uctp.AuthPrincipal;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Outbound;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Route.IdentityTarget;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConversationServiceTest {
    private Vertx vertx;
    private UctpEventRouter router;
    private UctpIds ids;
    private ConversationService service;
    private final List<Outbound> delivered = new CopyOnWriteArrayList<>();
    private final Map<String, CompletableFuture<JsonObject>> replies = new ConcurrentHashMap<>();
    private final AtomicInteger sequence = new AtomicInteger();
    private final Set<String> createRequests = ConcurrentHashMap.newKeySet();
    private final Set<Context> deliveryContexts = ConcurrentHashMap.newKeySet();

    @BeforeEach void setUp() {
        vertx = Vertx.vertx();
        router = mock(UctpEventRouter.class);
        ids = mock(UctpIds.class);
        when(ids.prefixed(anyString())).thenAnswer(i -> i.getArgument(0) + "_" + sequence.incrementAndGet());
        when(router.identityOf(anyString())).thenAnswer(i -> i.getArgument(0));
        doAnswer(i -> { capture(i.getArgument(0)); return null; }).when(router).deliver(any(Outbound.class));
        doAnswer(i -> { List<Outbound> out = i.getArgument(0); out.forEach(this::capture); return null; }).when(router).deliver(anyList());
        service = new ConversationService(vertx, router, new ContactCenterProperties(), ids, new UctpEnvelope(ids));
    }
    @AfterEach void close() throws Exception { vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS); }
    private void capture(Outbound out) {
        delivered.add(new Outbound(out.envelope().copy(), out.route()));
        if (Vertx.currentContext() != null) deliveryContexts.add(Vertx.currentContext());
        String replyTo = out.envelope().getString("in_reply_to");
        String type = out.envelope().getString("type");
        if (replyTo != null && (Set.of("ack", "error").contains(type) || "conversation.opened".equals(type) && createRequests.contains(replyTo))) {
            CompletableFuture<JsonObject> reply = replies.get(replyTo);
            if (reply != null) reply.complete(out.envelope().copy());
        }
    }
    private AuthPrincipal principal(String identity) {
        return new AuthPrincipal(identity, identity, "human", "customer", new JsonObject(), "identified", null);
    }
    private JsonObject env(String type, String cid, String sid, String conn, JsonObject payload) {
        JsonObject e = new JsonObject().put("v", 1).put("type", type).put("id", "request_" + sequence.incrementAndGet())
                .put("ts", Instant.now().toString()).put("payload", payload);
        if (cid != null) e.put("cid", cid); if (sid != null) e.put("sid", sid); if (conn != null) e.put("connid", conn); return e;
    }
    private CompletableFuture<JsonObject> send(ConversationService target, String who, JsonObject e) {
        CompletableFuture<JsonObject> result = new CompletableFuture<>(); replies.put(e.getString("id"), result);
        if ("conversation.create".equals(e.getString("type"))) createRequests.add(e.getString("id"));
        target.dispatch(principal(who), e, who); return result;
    }
    private JsonObject request(String who, String type, String cid, String sid, String conn, JsonObject payload) throws Exception {
        return send(service, who, env(type, cid, sid, conn, payload)).get(5, TimeUnit.SECONDS);
    }
    private void ack(String who, String type, String cid, String sid, String conn, JsonObject payload) throws Exception {
        assertEquals("ack", request(who, type, cid, sid, conn, payload).getString("type"));
    }
    private void error(int code, String who, String type, String cid, String sid, String conn, JsonObject payload) throws Exception {
        JsonObject result = request(who, type, cid, sid, conn, payload);
        assertEquals("error", result.getString("type")); assertEquals(code, result.getJsonObject("payload").getInteger("code"));
    }
    private void members(ConversationService target, String cid) throws Exception {
        target.createConversation(cid, "default", "persistent", 60, new JsonObject());
        for (String who : List.of("customer", "agent", "other")) target.broadcastConvOp(new JsonObject().put("op", "add-participant")
                .put("cid", cid).put("participant_id", "p-" + who).put("identity_id", who).put("role", who.equals("customer") ? "customer" : "agent"));
        assertEquals("ack", send(target, "customer", env("message.history", cid, null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
    }
    private void members(String cid) throws Exception { members(service, cid); }
    private void invite(String cid, String sid, String medium) throws Exception {
        ack("customer", "session.invite", cid, sid, null, new JsonObject().put("medium", medium).put("to", new JsonArray().add("agent")));
    }
    private void negotiate(String cid, String sid, String conn) throws Exception {
        invite(cid, sid, "voice"); ack("agent", "session.accept", cid, sid, null, new JsonObject());
        ack("customer", "connection.offer", cid, sid, conn, new JsonObject().put("to", "agent").put("sdp", "offer"));
        ack("agent", "connection.answer", cid, sid, conn, new JsonObject().put("sdp", "answer"));
    }
    private long count(String type, String cid) { return delivered.stream().filter(o -> type.equals(o.envelope().getString("type")) && cid.equals(o.envelope().getString("cid"))).count(); }
    private JsonObject message(String msgId, Object to, Object body) { return new JsonObject().put("msg_id", msgId).put("to", to).put("body", body).put("content_type", "text/plain"); }
    private JsonObject storedMessage(String msgId, long at, Object to) {
        return env("message.send", "c", null, null, message(msgId, to, "body-" + msgId).put("from", "p-customer"))
                .put("ts", Instant.ofEpochMilli(at).toString());
    }
    private JsonObject storedSession(String sid, long at, String... participants) {
        return new JsonObject().put("cid", "c").put("sid", sid).put("medium", "voice").put("status", "ENDED")
                .put("participant_ids", new JsonArray(List.of(participants))).put("invited_at", at);
    }
    private List<JsonObject> timelineItems(JsonObject request) {
        return delivered.stream().map(Outbound::envelope).filter(e -> request.getString("id").equals(e.getString("in_reply_to"))
                && "conversation.timeline.item".equals(e.getString("type"))).toList();
    }
    private String timelineKey(JsonObject item) {
        JsonObject payload = item.getJsonObject("payload");
        return payload.getString("kind") + "/" + payload.getString("source_id");
    }

    @Test void unauthorizedCannotSubscribeInviteOfferOrEndAnotherSession() throws Exception {
        members("c"); invite("c", "s", "voice");
        error(403, "stranger", "session.invite", "c", "evil", null, new JsonObject().put("to", new JsonArray().add("agent")));
        error(403, "stranger", "connection.offer", "c", "s", "conn", new JsonObject().put("to", "agent"));
        error(403, "other", "session.end", "c", "s", null, new JsonObject());
        error(403, "other", "session.accept", "c", "s", null, new JsonObject());
        error(403, "customer", "connection.offer", "c", "s", "conn", new JsonObject().put("to", "agent"));
        assertNull(service.conversation("c").participantByIdentity("stranger"));
        assertTrue(service.conversation("c").sessions().get("s").connections().isEmpty());
        verify(router, never()).joinGroup(anyString(), anyString());
        assertTrue(delivered.stream().allMatch(o -> o.route() instanceof IdentityTarget));
    }
    @Test void videoInviteRequiresExplicitAcceptanceAndAllRejectEndsSession() throws Exception {
        members("c"); invite("c", "video", "video");
        assertEquals("video", service.conversation("c").sessions().get("video").medium());
        assertEquals(Set.of("p-customer"), service.conversation("c").sessions().get("video").acceptedParticipants());
        ack("agent", "session.reject", "c", "video", null, new JsonObject());
        assertEquals(SessionState.Status.ENDED, service.conversation("c").sessions().get("video").status());
        assertTrue(count("session.ended", "c") > 0); assertFalse(service.conversation("c").closed());
    }
    @Test void answerNegotiatesButBothDistinctPeersMustConfirmMedia() throws Exception {
        members("c"); negotiate("c", "s", "conn");
        assertEquals(2, count("connection.negotiated", "c")); assertEquals(0, count("connection.ready", "c"));
        assertEquals(SessionState.Status.INVITING, service.conversation("c").sessions().get("s").status());
        error(403, "other", "connection.answer", "c", "s", "conn", new JsonObject());
        error(403, "customer", "connection.answer", "c", "s", "conn", new JsonObject());
        ack("customer", "connection.confirm", "c", "s", "conn", new JsonObject().put("state", "connected"));
        ack("customer", "connection.confirm", "c", "s", "conn", new JsonObject().put("state", "connected"));
        assertEquals(0, count("connection.ready", "c"));
        ack("agent", "connection.confirm", "c", "s", "conn", new JsonObject().put("state", "connected"));
        assertEquals(2, count("connection.ready", "c")); assertEquals(2, count("session.started", "c"));
        assertEquals(SessionState.Status.ACTIVE, service.conversation("c").sessions().get("s").status());
        ack("agent", "connection.confirm", "c", "s", "conn", new JsonObject().put("state", "connected")); assertEquals(2, count("connection.ready", "c"));
    }
    @Test void endedConnectionsAndSessionsCannotBeResurrected() throws Exception {
        members("c"); negotiate("c", "s", "conn"); ack("customer", "connection.end", "c", "s", "conn", new JsonObject());
        for (String type : List.of("connection.answer", "connection.confirm", "connection.ice-candidate", "connection.update"))
            error(409, "agent", type, "c", "s", "conn", new JsonObject().put("state", "connected"));
        error(409, "customer", "connection.offer", "c", "s", "conn", new JsonObject().put("to", "agent"));
        ack("customer", "session.end", "c", "s", null, new JsonObject());
        error(409, "customer", "connection.offer", "c", "s", "new", new JsonObject().put("to", "agent"));
        error(409, "agent", "session.accept", "c", "s", null, new JsonObject()); assertFalse(service.conversation("c").closed());
    }
    @Test void iceAndMediaStateAreConfinedToTheActualLegs() throws Exception {
        members("c"); negotiate("c", "s", "conn"); delivered.clear();
        ack("customer", "connection.ice-candidate", "c", "s", "conn", new JsonObject().put("candidate", "trickle"));
        List<Outbound> ice = delivered.stream().filter(o -> "connection.ice-candidate".equals(o.envelope().getString("type"))).toList();
        assertEquals(1, ice.size()); assertEquals(new IdentityTarget("agent"), ice.getFirst().route());
        ack("agent", "connection.update", "c", "s", "conn", new JsonObject().put("mute", true).put("camera", false).put("hold", true).put("screen-share", true));
        assertTrue(service.conversation("c").sessions().get("s").connections().get("conn").mediaByParticipant.get("p-agent").getBoolean("mute"));
        error(403, "other", "connection.update", "c", "s", "conn", new JsonObject().put("mute", true));
    }
    @Test void sqlFailureHasNoAckNoDeliveryAndNoMemoryMutation() throws Exception {
        members("c"); JournalRepository journal = mock(JournalRepository.class);
        when(journal.append(anyString(), any())).thenReturn(Future.failedFuture("SQL unavailable")); service.journalRepository(journal);
        delivered.clear(); error(503, "customer", "message.send", "c", null, null, message("m", "all", "hello"));
        assertEquals(0, count("ack", "c")); assertEquals(0, count("message.send", "c")); assertEquals(0, count("message.delivered", "c"));
        assertTrue(service.conversation("c").journalSnapshot().isEmpty());
    }
    @Test void duplicateMessageOnlyAcksAndConflictingContentFails() throws Exception {
        members("c"); MemoryMessages store = new MemoryMessages(); service.messageStore(store);
        ack("customer", "message.send", "c", null, null, message("m", new JsonArray().add("agent"), "one"));
        assertEquals(1, count("message.send", "c")); assertEquals(0, count("message.delivered", "c"));
        ack("customer", "message.send", "c", null, null, message("m", new JsonArray().add("agent"), "one")); assertEquals(1, count("message.send", "c"));
        error(409, "customer", "message.send", "c", null, null, message("m", new JsonArray().add("agent"), "two")); assertEquals(1, count("message.send", "c"));
        // A false from SQL must never create a new delivery even without a memory cache entry.
        service.messageStore(new ConversationService.MessageStore() {
            public Future<Boolean> append(String cid, JsonObject e) { return Future.succeededFuture(false); }
            public Future<List<JsonObject>> history(String cid, int limit) { return Future.succeededFuture(List.of()); }
        });
        ack("customer", "message.send", "c", null, null, message("old", "all", "persisted")); assertEquals(1, count("message.send", "c"));
    }
    @Test void directedHistoryAndReceiptsValidateAudienceAndMessageOwnership() throws Exception {
        members("c"); service.messageStore(new MemoryMessages());
        ack("customer", "message.send", "c", null, null, message("private", new JsonArray().add("agent"), "secret")); delivered.clear();
        ack("other", "message.history", "c", null, null, new JsonObject()); assertEquals(0, count("message.send", "c"));
        error(403, "other", "message.received", "c", null, null, new JsonObject().put("msg_id", "private"));
        error(403, "customer", "message.received", "c", null, null, new JsonObject().put("msg_id", "private"));
        error(404, "agent", "message.read", "c", null, null, new JsonObject().put("msg_id", "unknown"));
        ack("agent", "message.received", "c", null, null, new JsonObject().put("msg_id", "private"));
        ack("agent", "message.read", "c", null, null, new JsonObject().put("msg_id", "private"));
        assertEquals(1, count("message.delivered", "c")); assertEquals(1, count("message.read", "c"));
        delivered.stream().filter(o -> Set.of("message.delivered", "message.read").contains(o.envelope().getString("type")))
                .forEach(o -> assertEquals(new IdentityTarget("customer"), o.route()));
        delivered.clear(); ack("agent", "message.history", "c", null, null, new JsonObject().put("limit", 999)); assertEquals(1, count("message.send", "c"));
        error(403, "stranger", "message.history", "c", null, null, new JsonObject());
    }
    @Test void batchReadFetchesOnceAndNotifiesEachSenderWithSingleIdsAfterHistoryCompletes() throws Exception {
        members("c"); MemoryMessages store = spy(new MemoryMessages()); service.messageStore(store);
        ack("customer", "message.send", "c", null, null, message("first", new JsonArray().add("agent"), "one"));
        ack("other", "message.send", "c", null, null, message("second", new JsonArray().add("agent"), "two"));
        Promise<List<JsonObject>> gate = Promise.promise(); CountDownLatch entered = new CountDownLatch(1);
        Set<Context> storeContexts = ConcurrentHashMap.newKeySet();
        doAnswer(i -> { storeContexts.add(Vertx.currentContext()); entered.countDown(); return gate.future(); }).when(store).history("c", 200);
        delivered.clear(); deliveryContexts.clear(); clearInvocations(store);
        JsonObject payload = new JsonObject().put("msg_ids", new JsonArray().add("first").add("second").add("first"));
        CompletableFuture<JsonObject> reply = send(service, "agent", env("message.read", "c", null, null, payload));
        assertTrue(entered.await(5, TimeUnit.SECONDS)); assertFalse(reply.isDone()); assertTrue(delivered.isEmpty());
        CompletableFuture.runAsync(() -> gate.complete(store.entries.stream().map(JsonObject::copy).toList())).get(5, TimeUnit.SECONDS);
        assertEquals("ack", reply.get(5, TimeUnit.SECONDS).getString("type"));
        verify(store, times(1)).history("c", 200); assertEquals(1, count("ack", "c")); assertEquals(2, count("message.read", "c"));
        for (Outbound out : delivered.stream().filter(o -> "message.read".equals(o.envelope().getString("type"))).toList()) {
            JsonObject receipt = out.envelope().getJsonObject("payload");
            assertEquals(new IdentityTarget("first".equals(receipt.getString("msg_id")) ? "customer" : "other"), out.route());
            assertFalse(receipt.containsKey("msg_ids")); assertEquals("p-agent", receipt.getString("by_participant"));
            assertEquals("p-agent", receipt.getString("to_participant")); assertNotNull(receipt.getString("read_at"));
        }
        assertEquals(storeContexts, deliveryContexts); assertEquals(1, storeContexts.size());
        assertTrue(storeContexts.iterator().next().isEventLoopContext());
        doCallRealMethod().when(store).history("c", 200); delivered.clear(); clearInvocations(store);
        ack("agent", "message.received", "c", null, null, payload);
        verify(store, times(1)).history("c", 200); assertEquals(1, count("ack", "c")); assertEquals(2, count("message.delivered", "c"));
        delivered.stream().filter(o -> "message.delivered".equals(o.envelope().getString("type")))
                .forEach(o -> assertNotNull(o.envelope().getJsonObject("payload").getString("delivered_at")));
    }

    @Test void batchReceiptsRejectAllTargetsIfAnyIsUnauthorizedMissingOrSelfSent() throws Exception {
        members("c"); MemoryMessages store = spy(new MemoryMessages()); service.messageStore(store);
        ack("customer", "message.send", "c", null, null, message("valid", new JsonArray().add("agent"), "one"));
        ack("customer", "message.send", "c", null, null, message("private", new JsonArray().add("other"), "secret"));
        ack("agent", "message.send", "c", null, null, message("self", "all", "mine"));
        for (String type : List.of("message.read", "message.received")) {
            for (String invalid : List.of("private", "missing", "self")) {
                delivered.clear(); clearInvocations(store);
                error("missing".equals(invalid) ? 404 : 403, "agent", type, "c", null, null,
                        new JsonObject().put("msg_ids", new JsonArray().add("valid").add(invalid)));
                verify(store, times(1)).history("c", 200);
                assertEquals(0, count("message.read", "c")); assertEquals(0, count("message.delivered", "c")); assertEquals(0, count("ack", "c"));
            }
        }
        ack("customer", "message.send", "c", null, null, message("before-join", "all", "old"));
        service.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c").put("identity_id", "late").put("role", "agent"))
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ack("customer", "message.send", "c", null, null, message("late-valid", new JsonArray().add("late"), "new")); delivered.clear();
        error(403, "late", "message.read", "c", null, null, new JsonObject().put("msg_ids", new JsonArray().add("late-valid").add("before-join")));
        assertEquals(0, count("message.read", "c")); assertEquals(0, count("ack", "c"));
    }

    @Test void receiptIdsRejectEmptyNonStringAmbiguousAndOversizedPayloadsBeforeHistory() throws Exception {
        members("c"); MemoryMessages store = spy(new MemoryMessages()); service.messageStore(store);
        JsonArray oversized = new JsonArray(); for (int i = 0; i < 201; i++) oversized.add("repeated");
        List<JsonObject> invalid = List.of(new JsonObject(), new JsonObject().put("msg_ids", new JsonArray()),
                new JsonObject().put("msg_ids", null), new JsonObject().put("msg_ids", "not-an-array"),
                new JsonObject().put("msg_ids", new JsonArray().add("")), new JsonObject().put("msg_ids", new JsonArray().add("  ")),
                new JsonObject().put("msg_ids", new JsonArray().add(42)), new JsonObject().put("msg_ids", new JsonArray().add((Object) null)),
                new JsonObject().put("msg_ids", new JsonArray().add("valid").add(false)), new JsonObject().put("msg_ids", oversized),
                new JsonObject().put("msg_id", "single").put("msg_ids", new JsonArray().add("batch")),
                new JsonObject().put("msg_id", null), new JsonObject().put("msg_id", ""), new JsonObject().put("msg_id", " \t"),
                new JsonObject().put("msg_id", 42));
        for (String type : List.of("message.read", "message.received")) {
            for (JsonObject payload : invalid) {
                delivered.clear(); error(400, "agent", type, "c", null, null, payload);
                assertEquals(0, count("ack", "c")); assertEquals(0, count("message.read", "c")); assertEquals(0, count("message.delivered", "c"));
            }
        }
        verify(store, never()).history(anyString(), anyInt());
    }

    @Test void batchReceiptsRespectVisibilityRecordedAudienceAndExistingSender() throws Exception {
        members("c"); MemoryMessages store = new MemoryMessages(); service.messageStore(store);
        ack("customer", "message.send", "c", null, null, message("valid", new JsonArray().add("agent"), "one"));
        JsonObject valid = store.entries.getFirst().copy();
        JsonObject hidden = valid.copy(); hidden.getJsonObject("payload").put("msg_id", "hidden").put("to", new JsonArray().add("p-other"));
        store.entries.add(hidden);
        JsonObject noSender = valid.copy(); noSender.getJsonObject("payload").put("msg_id", "no-sender").put("from", "p-absent"); store.entries.add(noSender);
        JsonObject foreign = valid.copy().put("cid", "another"); foreign.getJsonObject("payload").put("msg_id", "foreign"); store.entries.add(foreign);
        for (String invalid : List.of("hidden", "no-sender", "foreign")) {
            delivered.clear(); error("hidden".equals(invalid) ? 403 : 404, "agent", "message.read", "c", null, null,
                    new JsonObject().put("msg_ids", new JsonArray().add("valid").add(invalid)));
            assertEquals(0, count("message.read", "c")); assertEquals(0, count("ack", "c"));
        }
        JsonObject legacy = valid.copy(); legacy.remove("_recipients");
        legacy.getJsonObject("payload").put("msg_id", "legacy").put("to", new JsonArray().add("agent")); store.entries.add(legacy);
        delivered.clear(); ack("agent", "message.read", "c", null, null, new JsonObject().put("msg_id", "legacy"));
        assertEquals(1, count("ack", "c")); assertEquals(1, count("message.read", "c"));
    }

    @Test void batchReadAccepts200ButDoesNotPretendToReadOlderHistory() throws Exception {
        members("c"); MemoryMessages store = spy(new MemoryMessages()); service.messageStore(store);
        JsonArray batch = new JsonArray();
        for (int i = 0; i <= 200; i++) {
            String msgId = "m-" + i;
            store.entries.add(new JsonObject().put("type", "message.send").put("cid", "c").put("_recipients", new JsonArray().add("p-agent"))
                    .put("payload", new JsonObject().put("msg_id", msgId).put("from", "p-customer").put("to", new JsonArray().add("p-agent"))));
            if (i > 0) batch.add(msgId);
        }
        delivered.clear(); ack("agent", "message.read", "c", null, null, new JsonObject().put("msg_ids", batch));
        verify(store, times(1)).history("c", 200); assertEquals(200, count("message.read", "c")); assertEquals(1, count("ack", "c"));
        delivered.clear(); error(404, "agent", "message.read", "c", null, null,
                new JsonObject().put("msg_ids", new JsonArray().add("m-200").add("m-0")));
        assertEquals(0, count("message.read", "c")); assertEquals(0, count("ack", "c"));
    }

    @Test void richMediaAndTypingRemainV1AndTypingIsNotJournaled() throws Exception {
        members("c"); ack("customer", "message.send", "c", null, null, message("image", new JsonArray().add("agent"), new JsonObject().put("url", "https://example.test/image.png")));
        int entries = service.conversation("c").journalSnapshot().size();
        ack("agent", "message.typing", "c", null, null, new JsonObject().put("active", true));
        assertEquals(entries, service.conversation("c").journalSnapshot().size());
        assertTrue(delivered.stream().allMatch(o -> o.envelope().getInteger("v") == 1));
    }
    @Test void closePersistsThenClearsAllSessionsConnectionsAndNotifiesWorkforceOnce() throws Exception {
        service.registerNodeConsumers();
        MemoryConversations store = new MemoryConversations(); service.conversationStore(store);
        CountDownLatch released = new CountDownLatch(1); AtomicInteger releases = new AtomicInteger();
        service.conversationClosedListener(cid -> { releases.incrementAndGet(); released.countDown(); });
        members("c"); negotiate("c", "s1", "k1"); negotiate("c", "s2", "k2");
        ack("customer", "connection.end", "c", "s1", "k1", new JsonObject());
        assertTrue(service.conversation("c").sessions().get("s1").graceTimerId() >= 0);
        ack("customer", "conversation.close", "c", null, null, new JsonObject()); assertTrue(released.await(5, TimeUnit.SECONDS));
        Conversation c = service.conversation("c"); assertTrue(c.closed()); assertEquals(-1, c.idleTimerId()); assertTrue(store.saved.get("c").getBoolean("closed"));
        c.sessions().values().forEach(s -> { assertEquals(SessionState.Status.ENDED, s.status()); assertEquals(-1, s.graceTimerId()); assertTrue(s.pendingInvitees().isEmpty()); s.connections().values().forEach(k -> assertEquals(SessionState.ConnectionStatus.ENDED, k.status)); });
        assertTrue(count("conversation.closed", "c") > 0); assertTrue(count("session.ended", "c") > 0); assertTrue(count("connection.ended", "c") > 0);
        ack("customer", "conversation.close", "c", null, null, new JsonObject()); assertEquals(1, releases.get());
        error(409, "customer", "message.send", "c", null, null, message("after", "all", "no"));
    }
    @Test void failedCloseDoesNotEndMediaOrAcknowledgeTheMutation() throws Exception {
        members("c"); negotiate("c", "s", "conn"); SqlConversationRepository repository = mock(SqlConversationRepository.class);
        when(repository.save(any(), anyList())).thenReturn(Future.failedFuture("save failed")); service.conversationRepository(repository); delivered.clear();
        error(503, "customer", "conversation.close", "c", null, null, new JsonObject());
        assertFalse(service.conversation("c").closed()); assertEquals(SessionState.ConnectionStatus.ANSWERED, service.conversation("c").sessions().get("s").connections().get("conn").status);
        assertEquals(0, count("session.ended", "c")); assertEquals(0, count("conversation.closed", "c")); assertEquals(0, count("ack", "c"));
    }
    @Test void handoffByIdentityLeavesObserverReceivingChatButUnableToSpeakOrJoinNewCalls() throws Exception {
        members("c"); negotiate("c", "s", "conn");
        service.broadcastConvOp(new JsonObject().put("op", "role-change").put("cid", "c").put("identity_id", "agent").put("new_role", "observer"));
        ack("agent", "message.history", "c", null, null, new JsonObject()); assertEquals("observer", service.conversation("c").participantByIdentity("agent").role());
        error(403, "agent", "message.send", "c", null, null, message("no", "all", "no"));
        error(403, "agent", "session.invite", "c", "new", null, new JsonObject().put("to", new JsonArray().add("customer")));
        error(403, "customer", "session.invite", "c", "new", null, new JsonObject().put("to", new JsonArray().add("agent")));
        delivered.clear(); ack("customer", "message.send", "c", null, null, message("chat", "all", "still here"));
        assertTrue(delivered.stream().anyMatch(o -> "message.send".equals(o.envelope().getString("type")) && o.route().equals(new IdentityTarget("agent"))));
        ack("customer", "session.invite", "c", "fresh", null, new JsonObject().put("medium", "video").put("to", new JsonArray().add("other")));
        ack("other", "session.accept", "c", "fresh", null, new JsonObject());
        error(403, "agent", "session.update", "c", "fresh", null, new JsonObject().put("kind", "role-change"));
    }
    private JsonObject assignment(String identityId, String fromIdentityId) {
        return new JsonObject().put("op", "assign-agent").put("cid", "c").put("identity_id", identityId)
                .put("customer_identity_id", "customer").put("kind", "human").put("role", "agent").put("display_name", "Assigned agent")
                .put("from_identity_id", fromIdentityId).put("sid", "s");
    }

    @Test void assignAgentCommitsNewMemberAndHandoffInOneAwaitedSaveBeforeNotifications() throws Exception {
        members("c"); negotiate("c", "s", "conn");
        ack("customer", "connection.confirm", "c", "s", "conn", new JsonObject().put("state", "connected"));
        invite("c", "pending", "voice");
        Promise<Void> gate = Promise.promise(); CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<JsonObject> saving = new AtomicReference<>(); Set<Context> storeContexts = ConcurrentHashMap.newKeySet();
        SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.save(any(), anyList())).thenAnswer(i -> {
            storeContexts.add(Vertx.currentContext()); saving.set(((JsonObject) i.getArgument(0)).copy());
            entered.countDown(); return gate.future();
        });
        service.conversationRepository(store);
        AtomicInteger ended = new AtomicInteger(); Set<Context> effectContexts = ConcurrentHashMap.newKeySet();
        service.sessionEndedListener((cid, sid) -> { ended.incrementAndGet(); effectContexts.add(Vertx.currentContext()); });
        delivered.clear(); deliveryContexts.clear(); clearInvocations(router);
        CompletableFuture<Void> assigned = service.broadcastConvOp(assignment("new-agent", "agent")).toCompletionStage().toCompletableFuture();
        assertTrue(entered.await(5, TimeUnit.SECONDS)); assertFalse(assigned.isDone());
        Conversation staged = Conversation.fromSnapshot(saving.get());
        assertEquals("agent", staged.participantByIdentity("new-agent").role());
        assertEquals("Assigned agent", staged.participantByIdentity("new-agent").displayName());
        assertEquals("observer", staged.participantByIdentity("agent").role());
        assertNull(service.conversation("c").participantByIdentity("new-agent"));
        assertEquals("agent", service.conversation("c").participantByIdentity("agent").role());
        assertEquals(SessionState.ConnectionStatus.ANSWERED, service.conversation("c").sessions().get("s").connections().get("conn").status);
        assertTrue(delivered.isEmpty()); assertEquals(0, ended.get());
        verify(router, never()).conversationMembers(anyString(), anySet());
        CompletableFuture.runAsync(() -> gate.complete()).get(5, TimeUnit.SECONDS);
        assigned.get(5, TimeUnit.SECONDS); verify(store, times(1)).save(any(), anyList());
        Conversation committed = service.conversation("c");
        assertEquals("agent", committed.participantByIdentity("new-agent").role());
        assertEquals("observer", committed.participantByIdentity("agent").role());
        SessionState session = committed.sessions().get("s");
        assertEquals(SessionState.ConnectionStatus.ENDED, session.connections().get("conn").status);
        assertTrue(session.connections().get("conn").connectedParticipants.isEmpty());
        assertFalse(session.acceptedParticipants().contains("p-agent"));
        assertTrue(committed.sessions().get("pending").pendingInvitees().isEmpty());
        assertEquals(SessionState.Status.ENDED, session.status()); assertEquals(2, ended.get());
        assertEquals(4, count("conversation.opened", "c")); assertEquals(8, count("session.update", "c"));
        delivered.stream().filter(o -> "conversation.opened".equals(o.envelope().getString("type"))).forEach(o -> {
            Conversation roster = Conversation.fromSnapshot(o.envelope().getJsonObject("payload").copy().put("cid", "c"));
            assertEquals("agent", roster.participantByIdentity("new-agent").role());
            assertEquals("observer", roster.participantByIdentity("agent").role());
        });
        var order = inOrder(router);
        order.verify(router).conversationMembers("c", Set.of("customer", "agent", "other", "new-agent"));
        order.verify(router).deliver(anyList());
        assertEquals(storeContexts, deliveryContexts); assertEquals(storeContexts, effectContexts);
        assertEquals(1, storeContexts.size()); assertTrue(storeContexts.iterator().next().isEventLoopContext());
    }

    @Test void assignAgentPromotesExistingObserverWithoutDuplicatingMembership() throws Exception {
        members("c");
        service.broadcastConvOp(new JsonObject().put("op", "role-change").put("cid", "c").put("identity_id", "other").put("new_role", "observer"))
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.save(any())).thenReturn(Future.succeededFuture()); service.conversationRepository(store); delivered.clear();
        service.broadcastConvOp(assignment("other", "agent")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        verify(store, times(1)).save(argThat(snapshot -> {
            Conversation c = Conversation.fromSnapshot(snapshot);
            return c.participants().size() == 3 && "agent".equals(c.participantByIdentity("other").role())
                    && "observer".equals(c.participantByIdentity("agent").role());
        }));
        assertEquals("p-other", service.conversation("c").participantByIdentity("other").participantId());
        assertEquals(3, service.conversation("c").participants().size());
        assertTrue(delivered.stream().anyMatch(o -> "session.update".equals(o.envelope().getString("type"))
                && "other".equals(o.envelope().getJsonObject("payload").getJsonObject("details").getString("identity_id"))
                && "agent".equals(o.envelope().getJsonObject("payload").getJsonObject("details").getString("new_role"))));
        ack("other", "message.send", "c", null, null, message("promoted", "all", "can speak"));
        error(403, "agent", "message.send", "c", null, null, message("demoted", "all", "cannot speak"));
    }

    @Test void failedAssignAgentSaveDiscardsMembershipRolesMediaAndEffects() throws Exception {
        members("c"); negotiate("c", "s", "conn");
        service.broadcastConvOp(new JsonObject().put("op", "role-change").put("cid", "c").put("identity_id", "other").put("new_role", "observer"))
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.save(any(), anyList())).thenReturn(Future.failedFuture("save failed")); service.conversationRepository(store);
        AtomicInteger ended = new AtomicInteger(); service.sessionEndedListener((cid, sid) -> ended.incrementAndGet());
        Conversation before = service.conversation("c"); JsonObject snapshot = before.describe();
        delivered.clear(); clearInvocations(router);
        for (String target : List.of("new-agent", "other")) {
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> service.broadcastConvOp(assignment(target, "agent"))
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("save failed", failure.getCause().getMessage());
            assertSame(before, service.conversation("c")); assertEquals(snapshot, service.conversation("c").describe());
            assertNull(service.conversation("c").participantByIdentity("new-agent"));
            assertEquals("observer", service.conversation("c").participantByIdentity("other").role());
            assertEquals(SessionState.ConnectionStatus.ANSWERED, service.conversation("c").sessions().get("s").connections().get("conn").status);
            assertEquals(Set.of("p-customer", "p-agent"), service.conversation("c").sessions().get("s").acceptedParticipants());
            assertTrue(delivered.isEmpty()); assertEquals(0, ended.get());
        }
        verify(store, times(2)).save(any(), anyList()); verify(router, never()).conversationMembers(anyString(), anySet());
    }

    @Test void assignmentCannotNotifyAnUnrelatedOrNonCustomerIdentity() throws Exception {
        members("c"); SqlConversationRepository store = mock(SqlConversationRepository.class);
        service.conversationRepository(store); delivered.clear();
        for (String customer : List.of("intruder", "agent")) {
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> service.broadcastConvOp(
                    assignment("new-agent", "agent").put("customer_identity_id", customer))
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("assignment-customer-not-member", failure.getCause().getMessage());
            assertNull(service.conversation("c").participantByIdentity("new-agent"));
            assertEquals("agent", service.conversation("c").participantByIdentity("agent").role());
            assertTrue(delivered.isEmpty());
        }
        verify(store, never()).save(any());
    }

    @Test void assignAgentValidatesPreviousMemberBeforeAddingAndCatchesUpQueuedChat() throws Exception {
        members("c"); SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.save(any())).thenReturn(Future.succeededFuture()); service.conversationRepository(store); delivered.clear();
        assertThrows(java.util.concurrent.ExecutionException.class, () -> service.broadcastConvOp(assignment("new-agent", "absent"))
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertNull(service.conversation("c").participantByIdentity("new-agent"));
        verify(store, never()).save(any()); assertTrue(delivered.isEmpty());
        ack("customer", "message.send", "c", null, null, message("queued", "all", "waiting")); delivered.clear();
        service.broadcastConvOp(assignment("new-agent", null).put("sid", null)).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        verify(store, times(1)).save(any());
        assertEquals("agent", service.conversation("c").participantByIdentity("agent").role());
        assertTrue(delivered.stream().anyMatch(o -> o.route().equals(new IdentityTarget("new-agent")) && "message.send".equals(o.envelope().getString("type"))
                && "queued".equals(o.envelope().getJsonObject("payload").getString("msg_id"))));
        assertEquals(0, count("message.delivered", "c"));
    }

    @Test void multipleConsultationsAndSessionsDoNotPolluteEachOther() throws Exception {
        members("a"); members("b"); negotiate("a", "same-sid", "same-conn"); negotiate("b", "same-sid", "same-conn");
        ack("customer", "session.end", "a", "same-sid", null, new JsonObject());
        assertEquals(SessionState.Status.INVITING, service.conversation("b").sessions().get("same-sid").status());
        ack("customer", "connection.confirm", "b", "same-sid", "same-conn", new JsonObject().put("state", "connected"));
        ack("agent", "connection.confirm", "b", "same-sid", "same-conn", new JsonObject().put("state", "connected"));
        assertEquals(SessionState.Status.ACTIVE, service.conversation("b").sessions().get("same-sid").status());
        ack("customer", "conversation.close", "a", null, null, new JsonObject()); assertFalse(service.conversation("b").closed());
    }

    @Test void boundedFifoSpansAsyncWritesWithoutBlockingOtherConsultations() throws Exception {
        members("blocked"); members("free");
        Promise<Boolean> gate = Promise.promise(); CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger(); Set<Context> storeContexts = ConcurrentHashMap.newKeySet();
        service.messageStore(new MemoryMessages() {
            public Future<Boolean> append(String cid, JsonObject e) {
                storeContexts.add(Vertx.currentContext());
                if ("blocked".equals(cid) && writes.incrementAndGet() == 1) {
                    entered.countDown(); return gate.future().compose(v -> super.append(cid, e));
                }
                return super.append(cid, e);
            }
        });
        List<CompletableFuture<JsonObject>> results = new ArrayList<>();
        results.add(send(service, "customer", env("message.send", "blocked", null, null, message("m0", "all", "0"))));
        assertTrue(entered.await(5, TimeUnit.SECONDS)); assertFalse(results.getFirst().isDone());
        for (int i = 1; i < 70; i++) results.add(send(service, "customer", env("message.send", "blocked", null, null, message("m" + i, "all", "" + i))));
        ack("customer", "message.history", "free", null, null, new JsonObject());
        assertEquals(1, writes.get());
        assertEquals(6, results.stream().filter(CompletableFuture::isDone).count());
        assertEquals(0, count("message.send", "blocked"));
        CompletableFuture.runAsync(() -> gate.complete(true)).get(5, TimeUnit.SECONDS);
        int accepted = 0;
        for (CompletableFuture<JsonObject> result : results) {
            JsonObject reply = result.get(5, TimeUnit.SECONDS);
            if ("ack".equals(reply.getString("type"))) accepted++;
            else assertEquals(503, reply.getJsonObject("payload").getInteger("code"));
        }
        assertEquals(64, accepted); assertEquals(64, writes.get());
        assertEquals(1, storeContexts.size()); assertEquals(1, deliveryContexts.size());
        assertTrue(storeContexts.iterator().next().isEventLoopContext());
        List<String> bodies = delivered.stream().filter(o -> "message.send".equals(o.envelope().getString("type")) && o.route().equals(new IdentityTarget("agent")))
                .map(o -> o.envelope().getJsonObject("payload").getString("body")).toList();
        for (int i = 0; i < 64; i++) assertEquals("" + i, bodies.get(i));
    }

    @Test void membershipSaveIsAwaitedBeforeFirstMessageResolvesItsAudience() throws Exception {
        members("c"); Promise<Void> gate = Promise.promise(); CountDownLatch entered = new CountDownLatch(1);
        MemoryConversations store = new MemoryConversations() {
            public Future<Void> save(JsonObject snapshot) {
                if (Conversation.fromSnapshot(snapshot).participantByIdentity("new-agent") != null) {
                    entered.countDown(); return gate.future().compose(v -> super.save(snapshot));
                }
                return super.save(snapshot);
            }
        };
        service.conversationStore(store); delivered.clear();
        service.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c").put("identity_id", "new-agent").put("role", "agent"));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        CompletableFuture<JsonObject> message = send(service, "customer", env("message.send", "c", null, null, message("first", "all", "hello")));
        assertFalse(message.isDone()); assertNull(service.conversation("c").participantByIdentity("new-agent"));
        assertEquals(0, count("conversation.opened", "c"));
        gate.complete(); assertEquals("ack", message.get(5, TimeUnit.SECONDS).getString("type"));
        assertTrue(delivered.stream().anyMatch(o -> o.route().equals(new IdentityTarget("new-agent")) && "message.send".equals(o.envelope().getString("type"))));
    }

    @Test void reconnectListRestoresDurableMembershipAndPrivateHistoryButNotMedia() throws Exception {
        MemoryConversations snapshots = new MemoryConversations(); MemoryMessages journal = new MemoryMessages();
        service.conversationStore(snapshots); service.messageStore(journal); members("c"); negotiate("c", "s", "conn");
        ack("customer", "message.send", "c", null, null, message("private", new JsonArray().add("agent"), "secret"));
        ConversationService restarted = new ConversationService(vertx, router, new ContactCenterProperties(), ids, new UctpEnvelope(ids));
        restarted.conversationStore(snapshots); restarted.messageStore(journal); delivered.clear();
        JsonObject timeline = env("conversation.timeline", "c", null, null, new JsonObject());
        assertEquals("ack", send(restarted, "agent", timeline).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(List.of("session/s", "message/private"), timelineItems(timeline).stream().map(this::timelineKey).toList());
        assertFalse(timelineItems(timeline).getFirst().getJsonObject("payload").getJsonObject("entry").containsKey("connections"));
        delivered.clear();
        assertEquals("ack", send(restarted, "other", env("conversation.list", null, null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(1, count("conversation.opened", "c")); assertEquals(0, count("message.send", "c"));
        assertNotNull(restarted.conversation("c").participantByIdentity("agent")); assertTrue(restarted.conversation("c").sessions().isEmpty());
        delivered.clear(); assertEquals("ack", send(restarted, "agent", env("conversation.list", "c", null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(1, count("message.send", "c"));
        delivered.clear(); assertEquals("ack", send(restarted, "stranger", env("conversation.list", null, null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(0, count("conversation.opened", "c")); assertEquals(0, count("message.send", "c"));
        assertInstanceOf(Number.class, snapshots.saved.get("c").getValue("opened_at"));
        assertFalse(snapshots.saved.get("c").containsKey("sessions"));
    }

    @Test void sessionHistorySurvivesRestartAndRecordsAcceptedRejectedAndCanceledCalls() throws Exception {
        MemoryConversations store = new MemoryConversations(); service.conversationStore(store); members("c");
        negotiate("c", "accepted", "conn");
        ack("customer", "connection.confirm", "c", "accepted", "conn", new JsonObject().put("state", "connected"));
        ack("agent", "connection.confirm", "c", "accepted", "conn", new JsonObject().put("state", "connected"));
        ack("customer", "session.end", "c", "accepted", null, new JsonObject());
        invite("c", "rejected", "video"); ack("agent", "session.reject", "c", "rejected", null, new JsonObject());
        invite("c", "canceled", "voice"); ack("customer", "session.cancel", "c", "canceled", null, new JsonObject());

        List<JsonObject> history = service.sessionHistory("c", 200).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(3, history.size());
        Map<String, JsonObject> bySid = history.stream().collect(java.util.stream.Collectors.toMap(s -> s.getString("sid"), s -> s));
        JsonObject accepted = bySid.get("accepted");
        assertEquals("ENDED", accepted.getString("status")); assertEquals("normal-clearing", accepted.getString("end_reason"));
        assertNotNull(accepted.getLong("accepted_at")); assertNotNull(accepted.getLong("started_at")); assertNotNull(accepted.getLong("ended_at"));
        assertTrue(accepted.getLong("duration_ms") >= 0);
        assertEquals(new JsonArray(List.of("p-agent", "p-customer")), accepted.getJsonArray("participant_ids"));
        assertFalse(accepted.containsKey("connections")); assertFalse(accepted.containsKey("streams"));
        assertEquals("all-invitees-declined", bySid.get("rejected").getString("end_reason"));
        assertFalse(bySid.get("rejected").containsKey("started_at"));
        assertEquals("canceled", bySid.get("canceled").getString("end_reason"));
        assertFalse(store.saved.get("c").containsKey("sessions"));

        ConversationService restarted = new ConversationService(vertx, router, new ContactCenterProperties(), ids, new UctpEnvelope(ids));
        restarted.conversationStore(store);
        assertEquals(history, restarted.sessionHistory("c", 200).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertNull(restarted.conversation("c"));
    }

    @Test void sessionHistoryWriteIsAwaitedBeforeAckAndDelivery() throws Exception {
        members("c"); SqlConversationRepository store = mock(SqlConversationRepository.class);
        Promise<Void> gate = Promise.promise(); CountDownLatch entered = new CountDownLatch(1);
        when(store.save(any(), anyList())).thenAnswer(invocation -> { entered.countDown(); return gate.future(); });
        service.conversationRepository(store); delivered.clear();
        CompletableFuture<JsonObject> invited = send(service, "customer", env("session.invite", "c", "s", null,
                new JsonObject().put("medium", "voice").put("to", new JsonArray().add("agent"))));
        assertTrue(entered.await(5, TimeUnit.SECONDS)); assertFalse(invited.isDone());
        assertTrue(delivered.isEmpty()); assertNull(service.conversation("c").sessions().get("s"));
        gate.complete(); assertEquals("ack", invited.get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(SessionState.Status.INVITING, service.conversation("c").sessions().get("s").status());
    }

    @Test void failedSessionHistoryWriteDiscardsInviteAndNotifications() throws Exception {
        members("c"); SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.save(any(), anyList())).thenReturn(Future.failedFuture("SQL unavailable")); service.conversationRepository(store); delivered.clear();
        error(503, "customer", "session.invite", "c", "s", null,
                new JsonObject().put("medium", "voice").put("to", new JsonArray().add("agent")));
        assertNull(service.conversation("c").sessions().get("s"));
        assertEquals(0, count("ack", "c")); assertEquals(0, count("session.invite", "c"));
    }

    @Test void publishedCreateIsClaimedOnceAndOwnershipSurvivesMembershipChange() throws Exception {
        AtomicReference<List<String>> topology = new AtomicReference<>(List.of("a", "b"));
        NodeService a = new NodeService("a", topology); NodeService b = new NodeService("b", topology);
        CountDownLatch created = new CountDownLatch(1); AtomicInteger creates = new AtomicInteger();
        MemoryConversations snapshots = new MemoryConversations() {
            public Future<Void> save(JsonObject snapshot) {
                if (snapshot.getJsonArray("participants").isEmpty()) { creates.incrementAndGet(); created.countDown(); }
                return super.save(snapshot);
            }
        };
        a.conversationStore(snapshots); b.conversationStore(snapshots);
        a.registerNodeConsumers(); b.registerNodeConsumers(); service = a;
        assertEquals("ack", send(a, "customer", env("conversation.list", null, null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        vertx.eventBus().publish(ConversationService.CONVOP_ADDRESS, new JsonObject().put("op", "create").put("cid", "c"));
        assertTrue(created.await(5, TimeUnit.SECONDS));
        for (String who : List.of("customer", "agent")) a.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c")
                .put("participant_id", "p-" + who).put("identity_id", who).put("role", who.equals("customer") ? "customer" : "agent"));
        ack("customer", "message.history", "c", null, null, new JsonObject());
        assertNull(a.conversation("c")); assertNotNull(b.conversation("c")); assertEquals("b", a.ownerNodeOf("c")); assertEquals(1, creates.get());
        topology.set(List.of("a", "b", "z"));
        NodeService z = new NodeService("z", topology); z.registerNodeConsumers();
        assertEquals("ack", send(z, "customer", env("message.send", "c", null, null, message("routed", "all", "stable"))).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals("b", z.ownerNodeOf("c")); assertNull(z.conversation("c"));
        delivered.clear(); assertEquals("ack", send(z, "agent", env("conversation.list", null, null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(1, count("conversation.opened", "c")); assertEquals(1, count("message.send", "c"));
        assertEquals("error", send(z, "stranger", env("connection.offer", "c", "s", "evil", new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        verify(router, never()).joinGroup(anyString(), anyString());
    }

    @Test void createAwaitsSnapshotAndCannotGrantRolesFromClientPayload() throws Exception {
        Promise<Void> gate = Promise.promise(); CountDownLatch entered = new CountDownLatch(1);
        SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.load(anyString())).thenReturn(Future.succeededFuture(null));
        when(store.save(any())).thenAnswer(i -> { entered.countDown(); return gate.future(); });
        service.conversationRepository(store);
        CompletableFuture<JsonObject> created = send(service, "customer", env("conversation.create", null, null, null,
                new JsonObject().put("initial_participants", new JsonArray().add(new JsonObject().put("identity_id", "agent").put("role", "supervisor")))));
        assertTrue(entered.await(5, TimeUnit.SECONDS)); assertFalse(created.isDone());
        assertTrue(delivered.isEmpty()); gate.complete();
        JsonObject opened = created.get(5, TimeUnit.SECONDS); assertEquals("conversation.opened", opened.getString("type"));
        assertEquals("observer", service.conversation(opened.getString("cid")).participantByIdentity("agent").role());
        assertEquals("customer", service.conversation(opened.getString("cid")).participantByIdentity("customer").role());
        assertEquals(1, deliveryContexts.size());
    }

    @Test void failedCreateDoesNotExposeMembershipOrSendOpened() throws Exception {
        SqlConversationRepository store = mock(SqlConversationRepository.class);
        when(store.load(anyString())).thenReturn(Future.succeededFuture(null));
        when(store.save(any())).thenReturn(Future.failedFuture("SQL failed")); service.conversationRepository(store);
        JsonObject reply = request("customer", "conversation.create", null, null, null, new JsonObject());
        assertEquals("error", reply.getString("type")); assertEquals(503, reply.getJsonObject("payload").getInteger("code"));
        assertNull(service.conversation(reply.getString("cid"))); assertEquals(0, count("conversation.opened", reply.getString("cid")));
    }

    @Test void allAudienceRetryIsStableAndLaterMembersCannotForgeDeliveryReceipts() throws Exception {
        members("c"); MemoryMessages store = new MemoryMessages(); service.messageStore(store);
        ack("customer", "message.send", "c", null, null, message("all", "all", "hello"));
        long originalDeliveries = count("message.send", "c");
        service.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c").put("identity_id", "late").put("role", "agent"));
        ack("late", "message.history", "c", null, null, new JsonObject());
        long withHistory = count("message.send", "c"); assertEquals(originalDeliveries + 2, withHistory);
        ack("customer", "message.send", "c", null, null, message("all", "all", "hello")); assertEquals(withHistory, count("message.send", "c"));
        error(403, "late", "message.received", "c", null, null, new JsonObject().put("msg_id", "all"));
        ack("agent", "message.received", "c", null, null, new JsonObject().put("msg_id", "all"));
        assertEquals(1, count("message.delivered", "c"));
    }

    @Test void timelineMergesSourcesAndPaginatesNewestSlicesWithoutGapsOrDuplicates() throws Exception {
        MemoryMessages messages = new MemoryMessages(); MemoryConversations sessions = new MemoryConversations();
        service.messageStore(messages); service.conversationStore(sessions); members("c");
        messages.entries.add(storedMessage("m1", 100, "all"));
        sessions.sessions.put("c/s1", storedSession("s1", 200, "p-customer", "p-agent"));
        messages.entries.add(storedMessage("m2", 300, "all"));
        sessions.sessions.put("c/s2", storedSession("s2", 300, "p-customer", "p-agent"));
        messages.entries.add(storedMessage("m3", 400, "all"));

        List<String> actual = new ArrayList<>(); String cursor = null; int pages = 0;
        do {
            JsonObject payload = new JsonObject().put("limit", 2);
            if (cursor != null) payload.put("cursor", cursor);
            JsonObject request = env("conversation.timeline", "c", null, null, payload);
            JsonObject ack = send(service, "customer", request).get(5, TimeUnit.SECONDS);
            assertEquals("ack", ack.getString("type"));
            List<JsonObject> items = timelineItems(request);
            items.forEach(item -> actual.add(timelineKey(item)));
            items.forEach(item -> assertEquals("customer", ((IdentityTarget) delivered.stream()
                    .filter(out -> out.envelope() == item).findFirst().orElseThrow().route()).identityId()));
            cursor = ack.getJsonObject("payload").getString("next_cursor");
            pages++;
        } while (cursor != null);
        assertEquals(List.of("session/s2", "message/m3", "session/s1", "message/m2", "message/m1"), actual);
        assertEquals(3, pages);
    }

    @Test void timelineCursorDistinguishesKindsWhenTimestampAndSourceIdCollide() throws Exception {
        MemoryMessages messages = new MemoryMessages(); MemoryConversations sessions = new MemoryConversations();
        service.messageStore(messages); service.conversationStore(sessions); members("c");
        messages.entries.add(storedMessage("shared", 100, "all"));
        sessions.sessions.put("c/shared", storedSession("shared", 100, "p-customer", "p-agent"));

        JsonObject first = env("conversation.timeline", "c", null, null, new JsonObject().put("limit", 1));
        JsonObject firstAck = send(service, "customer", first).get(5, TimeUnit.SECONDS);
        assertEquals(List.of("session/shared"), timelineItems(first).stream().map(this::timelineKey).toList());
        String cursor = firstAck.getJsonObject("payload").getString("next_cursor");
        String decoded = new String(java.util.Base64.getUrlDecoder().decode(cursor), java.nio.charset.StandardCharsets.UTF_8);
        assertEquals("session", new JsonObject(decoded).getString("kind"));
        JsonObject second = env("conversation.timeline", "c", null, null, new JsonObject().put("limit", 1).put("cursor", cursor));
        JsonObject secondAck = send(service, "customer", second).get(5, TimeUnit.SECONDS);
        assertEquals(List.of("message/shared"), timelineItems(second).stream().map(this::timelineKey).toList());
        assertFalse(secondAck.getJsonObject("payload").containsKey("next_cursor"));
    }

    @Test void timelineFiltersDirectedMessagesAndSessionParticipantsBeforePagination() throws Exception {
        MemoryMessages messages = new MemoryMessages(); MemoryConversations sessions = new MemoryConversations();
        service.messageStore(messages); service.conversationStore(sessions); members("c");
        messages.entries.add(storedMessage("public-old", 100, "all"));
        messages.entries.add(storedMessage("private", 200, new JsonArray().add("agent")));
        sessions.sessions.put("c/hidden-call", storedSession("hidden-call", 300, "p-customer", "p-agent"));
        messages.entries.add(storedMessage("public-new", 400, "all"));

        JsonObject latest = env("conversation.timeline", "c", null, null, new JsonObject().put("limit", 1));
        JsonObject latestAck = send(service, "other", latest).get(5, TimeUnit.SECONDS);
        assertEquals(List.of("message/public-new"), timelineItems(latest).stream().map(this::timelineKey).toList());
        JsonObject older = env("conversation.timeline", "c", null, null, new JsonObject().put("limit", 1)
                .put("cursor", latestAck.getJsonObject("payload").getString("next_cursor")));
        assertEquals("ack", send(service, "other", older).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(List.of("message/public-old"), timelineItems(older).stream().map(this::timelineKey).toList());

        JsonObject agent = env("conversation.timeline", "c", null, null, new JsonObject());
        assertEquals("ack", send(service, "agent", agent).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(List.of("message/public-old", "message/private", "session/hidden-call", "message/public-new"),
                timelineItems(agent).stream().map(this::timelineKey).toList());
        error(403, "stranger", "conversation.timeline", "c", null, null, new JsonObject());
    }

    @Test void timelineUsesBoundedLimitsRejectsBadCursorsAndFailsWithoutAckOnSqlError() throws Exception {
        members("c"); JournalRepository journal = mock(JournalRepository.class); SqlConversationRepository conversations = mock(SqlConversationRepository.class);
        when(journal.timeline(anyString(), nullable(Long.class), nullable(String.class), nullable(String.class), anyInt()))
                .thenReturn(Future.succeededFuture(List.of()));
        when(conversations.sessionTimeline(anyString(), nullable(Long.class), nullable(String.class), nullable(String.class), anyInt()))
                .thenReturn(Future.succeededFuture(List.of()));
        service.journalRepository(journal); service.conversationRepository(conversations);
        JsonObject empty = env("conversation.timeline", "c", null, null, new JsonObject());
        JsonObject emptyAck = send(service, "customer", empty).get(5, TimeUnit.SECONDS);
        assertTrue(timelineItems(empty).isEmpty()); assertFalse(emptyAck.getJsonObject("payload").containsKey("next_cursor"));
        verify(journal).timeline("c", null, null, null, 101);
        verify(conversations).sessionTimeline("c", null, null, null, 101);
        ack("customer", "conversation.timeline", "c", null, null, new JsonObject().put("limit", 10000));
        verify(journal).timeline("c", null, null, null, 201);
        verify(conversations).sessionTimeline("c", null, null, null, 201);

        String missingKind = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                new JsonObject().put("v", 1).put("at", 100).put("source_id", "m1").encode().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (String cursor : List.of("not-base64", missingKind)) {
            error(400, "customer", "conversation.timeline", "c", null, null, new JsonObject().put("cursor", cursor));
        }
        error(400, "customer", "conversation.timeline", "c", null, null, new JsonObject().put("limit", 0));
        error(400, "customer", "conversation.timeline", "c", null, null, new JsonObject().put("limit", "many"));
        when(journal.timeline(anyString(), nullable(Long.class), nullable(String.class), nullable(String.class), anyInt()))
                .thenReturn(Future.failedFuture("IO failed"));
        delivered.clear(); error(503, "customer", "conversation.timeline", "c", null, null, new JsonObject());
        assertEquals(0, count("ack", "c")); assertEquals(0, count("conversation.timeline.item", "c"));
    }

    @Test void historyUsesDefault100AndClampsAt200AndFailsWithoutAckOnSqlError() throws Exception {
        members("c"); JournalRepository store = mock(JournalRepository.class);
        when(store.history(anyString(), anyInt())).thenReturn(Future.succeededFuture(List.of())); service.journalRepository(store);
        ack("customer", "message.history", "c", null, null, new JsonObject()); verify(store).history("c", 100);
        ack("customer", "message.history", "c", null, null, new JsonObject().put("limit", 10000)); verify(store).history("c", 200);
        when(store.history(anyString(), anyInt())).thenReturn(Future.failedFuture("IO failed")); delivered.clear();
        error(503, "customer", "message.history", "c", null, null, new JsonObject()); assertEquals(0, count("ack", "c"));
    }

    @Test void persistedClosedConversationCannotBeOverwrittenByDuplicateCreate() throws Exception {
        MemoryConversations store = new MemoryConversations(); service.conversationStore(store); members("c");
        ack("customer", "conversation.close", "c", null, null, new JsonObject()); JsonObject before = store.saved.get("c").copy();
        ConversationService restarted = new ConversationService(vertx, router, new ContactCenterProperties(), ids, new UctpEnvelope(ids));
        restarted.conversationStore(store); restarted.createConversation("c", "wrong-tenant", "ephemeral", 5, new JsonObject());
        assertEquals("ack", send(restarted, "customer", env("message.history", "c", null, null, new JsonObject())).get(5, TimeUnit.SECONDS).getString("type"));
        assertTrue(restarted.conversation("c").closed()); assertEquals(before, store.saved.get("c"));
        assertNotNull(restarted.conversation("c").participantByIdentity("agent"));
    }

    @Test void durableOwnerIsUsedWhenTheVolatileDirectoryIsLost() throws Exception {
        AtomicReference<List<String>> topology = new AtomicReference<>(List.of("a", "b"));
        MemoryConversations snapshots = new MemoryConversations();
        NodeService a = new NodeService("a", topology); NodeService b = new NodeService("b", topology);
        a.conversationStore(snapshots); b.conversationStore(snapshots); a.registerNodeConsumers(); b.registerNodeConsumers();
        members(b, "c"); assertEquals("b", snapshots.saved.get("c").getJsonObject("metadata").getString("_owner_node_id"));
        vertx.sharedData().<String, String>getAsyncMap("uctp.conversation.owners.v1").compose(map -> map.remove("c"))
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        topology.set(List.of("a", "b", "z")); NodeService z = new NodeService("z", topology);
        z.conversationStore(snapshots); z.registerNodeConsumers();
        assertEquals("ack", send(z, "customer", env("message.send", "c", null, null, message("persisted-owner", "all", "stable"))).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals("b", z.ownerNodeOf("c")); assertNull(z.conversation("c")); assertNotNull(b.conversation("c"));
    }

    @Test void externalProviderSubmissionIsNotDeliveryAndSqlFailureDoesNotCallProvider() throws Exception {
        members("c");
        var connector = mock(com.workoss.boot.vertx.contactcenter.channel.ChannelConnector.class);
        var registry = mock(com.workoss.boot.vertx.contactcenter.channel.ChannelRegistry.class);
        when(registry.get("sms")).thenReturn(java.util.Optional.of(connector)); service.channelRegistry(registry);
        service.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c").put("identity_id", "external")
                .put("kind", "external").put("role", "customer").put("channel", "sms").put("address", "+10000000000"));
        ack("customer", "message.history", "c", null, null, new JsonObject());
        JournalRepository journal = mock(JournalRepository.class);
        when(journal.append(anyString(), any())).thenReturn(Future.failedFuture("IO failed")); service.journalRepository(journal);
        error(503, "customer", "message.send", "c", null, null, message("external-fail", new JsonArray().add("external"), "no send"));
        verify(connector, never()).sendMessage(any());
        when(journal.append(anyString(), any())).thenReturn(Future.succeededFuture(true));
        ack("customer", "message.send", "c", null, null, message("external-ok", new JsonArray().add("external"), "queued"));
        verify(connector).sendMessage(any()); assertEquals(0, count("message.delivered", "c"));
    }

    @Test void explicitCloseCancelsAnActuallyArmedEphemeralIdleTimer() throws Exception {
        service.createConversation("c", "default", "ephemeral", 60, new JsonObject()); members("c"); negotiate("c", "s", "conn");
        ack("customer", "session.end", "c", "s", null, new JsonObject());
        long idleTimer = service.conversation("c").idleTimerId(); assertTrue(idleTimer >= 0);
        ack("customer", "conversation.close", "c", null, null, new JsonObject());
        assertEquals(-1, service.conversation("c").idleTimerId()); assertFalse(vertx.cancelTimer(idleTimer));
    }

    @Test void realGraceTimerEndsTheSessionButNotTheConsultation() throws Exception {
        ContactCenterProperties properties = new ContactCenterProperties(); properties.setReconnectGraceSeconds(1);
        service = new ConversationService(vertx, router, properties, ids, new UctpEnvelope(ids));
        CountDownLatch ended = new CountDownLatch(1); service.sessionEndedListener((cid, sid) -> ended.countDown());
        members("c"); negotiate("c", "s", "conn");
        ack("customer", "connection.end", "c", "s", "conn", new JsonObject());
        assertTrue(ended.await(5, TimeUnit.SECONDS));
        assertEquals(SessionState.Status.ENDED, service.conversation("c").sessions().get("s").status());
        assertFalse(service.conversation("c").closed());
        error(409, "customer", "connection.offer", "c", "s", "new-conn", new JsonObject().put("to", "agent"));
    }

    @Test void noSqlDedupeRejectsGlobalMessageIdReuseAcrossConsultations() throws Exception {
        members("a"); members("b");
        ack("customer", "message.send", "a", null, null, message("global-id", "all", "first"));
        error(409, "customer", "message.send", "b", null, null, message("global-id", "all", "second"));
        ack("customer", "message.send", "a", null, null, message("global-id", "all", "first"));
        assertEquals(2, count("message.send", "a")); assertEquals(0, count("message.send", "b"));
    }

    @Test void queuedFirstMessageIsCaughtUpAfterAgentMembershipIsSaved() throws Exception {
        service.createConversation("c", "default", "persistent", 60, new JsonObject());
        service.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c").put("identity_id", "customer").put("role", "customer"));
        ack("customer", "message.send", "c", null, null, message("queued-first", "all", "first question"));
        assertEquals(0, count("message.send", "c"));
        service.broadcastConvOp(new JsonObject().put("op", "add-participant").put("cid", "c").put("identity_id", "agent").put("role", "agent"));
        ack("customer", "message.history", "c", null, null, new JsonObject());
        assertTrue(delivered.stream().anyMatch(o -> o.route().equals(new IdentityTarget("agent")) && "message.send".equals(o.envelope().getString("type"))
                && "queued-first".equals(o.envelope().getJsonObject("payload").getString("msg_id"))));
        assertEquals(0, count("message.delivered", "c"));
    }

    @Test void unavailableStableOwnerRestoresReadOnlySnapshotWithoutTakingOwnership() throws Exception {
        AtomicReference<List<String>> topology = new AtomicReference<>(List.of("a", "b"));
        MemoryConversations snapshots = new MemoryConversations(); MemoryMessages journal = new MemoryMessages();
        NodeService a = new NodeService("a", topology); NodeService b = new NodeService("b", topology);
        a.conversationStore(snapshots); b.conversationStore(snapshots); a.messageStore(journal); b.messageStore(journal);
        members(b, "c");
        assertEquals("ack", send(b, "customer", env("session.invite", "c", "offline-call", null,
                new JsonObject().put("medium", "voice").put("to", new JsonArray().add("agent")))).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals("ack", send(b, "customer", env("message.send", "c", null, null,
                message("before-restart", "all", "preserved history"))).get(5, TimeUnit.SECONDS).getString("type"));
        JsonObject before = snapshots.saved.get("c").copy(); topology.set(List.of("a"));

        JsonObject list = env("conversation.list", null, null, null, new JsonObject());
        assertEquals("ack", send(a, "customer", list).get(5, TimeUnit.SECONDS).getString("type"));
        List<JsonObject> recovered = delivered.stream().map(Outbound::envelope)
                .filter(e -> list.getString("id").equals(e.getString("in_reply_to"))).toList();
        assertTrue(recovered.stream().anyMatch(e -> "conversation.opened".equals(e.getString("type")) && "c".equals(e.getString("cid"))));
        assertTrue(recovered.stream().anyMatch(e -> "message.send".equals(e.getString("type")) && "before-restart".equals(e.getJsonObject("payload").getString("msg_id"))));
        JsonObject history = env("message.history", "c", null, null, new JsonObject());
        assertEquals("ack", send(a, "customer", history).get(5, TimeUnit.SECONDS).getString("type"));
        assertTrue(delivered.stream().map(Outbound::envelope).anyMatch(e -> history.getString("id").equals(e.getString("in_reply_to"))
                && "message.send".equals(e.getString("type")) && "before-restart".equals(e.getJsonObject("payload").getString("msg_id"))));
        JsonObject timeline = env("conversation.timeline", "c", null, null, new JsonObject());
        assertEquals("ack", send(a, "customer", timeline).get(5, TimeUnit.SECONDS).getString("type"));
        assertEquals(Set.of("session/offline-call", "message/before-restart"),
                timelineItems(timeline).stream().map(this::timelineKey).collect(java.util.stream.Collectors.toSet()));

        error(503, "customer", "message.send", "c", null, null, message("after-restart", "all", "must not write"));
        assertEquals("b", a.ownerNodeOf("c")); assertNull(a.conversation("c")); assertEquals(before, snapshots.saved.get("c"));
    }

    private final class NodeService extends ConversationService {
        private final String node;
        private final AtomicReference<List<String>> topology;
        NodeService(String node, AtomicReference<List<String>> topology) {
            super(vertx, router, new ContactCenterProperties(), ids, new UctpEnvelope(ids)); this.node = node; this.topology = topology;
        }
        protected String selfNodeId() { return node; }
        protected List<String> clusterNodeIds() { return topology.get(); }
    }

    static class MemoryMessages implements ConversationService.MessageStore {
        final List<JsonObject> entries = new CopyOnWriteArrayList<>();
        public Future<Boolean> append(String cid, JsonObject e) {
            for (JsonObject old : entries) if (old.getJsonObject("payload").getString("msg_id").equals(e.getJsonObject("payload").getString("msg_id")))
                return old.getString("cid").equals(cid) && old.getJsonObject("payload").equals(e.getJsonObject("payload")) ? Future.succeededFuture(false) : Future.failedFuture("conflicting msg_id");
            entries.add(e.copy()); return Future.succeededFuture(true);
        }
        public Future<List<JsonObject>> history(String cid, int limit) {
            List<JsonObject> list = entries.stream().filter(e -> cid.equals(e.getString("cid"))).toList();
            return Future.succeededFuture(list.subList(Math.max(0, list.size() - limit), list.size()));
        }
        public Future<List<TimelineEntry>> timeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
            List<TimelineEntry> list = entries.stream().filter(e -> cid.equals(e.getString("cid")))
                    .map(e -> new TimelineEntry("message", Instant.parse(e.getString("ts")).toEpochMilli(),
                            e.getJsonObject("payload").getString("msg_id"), e.copy()))
                    .filter(e -> beforeAt == null || e.occurredAt() < beforeAt
                            || e.occurredAt() == beforeAt && (e.sourceId().compareTo(beforeId) < 0
                            || e.sourceId().equals(beforeId) && e.kind().compareTo(beforeKind) < 0))
                    .sorted(java.util.Comparator.comparingLong(TimelineEntry::occurredAt)
                            .thenComparing(TimelineEntry::sourceId).thenComparing(TimelineEntry::kind)).toList();
            return Future.succeededFuture(list.subList(Math.max(0, list.size() - limit), list.size()));
        }
    }
    static class MemoryConversations implements ConversationService.ConversationStore {
        final Map<String, JsonObject> saved = new ConcurrentHashMap<>();
        final Map<String, JsonObject> sessions = new ConcurrentHashMap<>();
        public Future<Void> save(JsonObject snapshot) { saved.put(snapshot.getString("cid"), snapshot.copy()); return Future.succeededFuture(); }
        public Future<Void> save(JsonObject snapshot, List<JsonObject> history) {
            saved.put(snapshot.getString("cid"), snapshot.copy());
            history.forEach(session -> sessions.put(session.getString("cid") + "/" + session.getString("sid"), session.copy()));
            return Future.succeededFuture();
        }
        public Future<JsonObject> load(String cid) { JsonObject s = saved.get(cid); return Future.succeededFuture(s == null ? null : s.copy()); }
        public Future<List<JsonObject>> findByIdentity(String who, int limit) {
            return Future.succeededFuture(saved.values().stream().filter(s -> Conversation.fromSnapshot(s).participantByIdentity(who) != null).limit(limit).map(JsonObject::copy).toList());
        }
        public Future<List<JsonObject>> sessionHistory(String cid, int limit) {
            List<JsonObject> history = sessions.values().stream().filter(session -> cid.equals(session.getString("cid")))
                    .sorted(java.util.Comparator.comparingLong((JsonObject session) -> session.getLong("invited_at"))
                            .thenComparing(session -> session.getString("sid"))).map(JsonObject::copy).toList();
            return Future.succeededFuture(history.subList(Math.max(0, history.size() - limit), history.size()));
        }
        public Future<List<TimelineEntry>> sessionTimeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
            List<TimelineEntry> list = sessions.values().stream().filter(session -> cid.equals(session.getString("cid")))
                    .map(session -> new TimelineEntry("session", session.getLong("invited_at"), session.getString("sid"), session.copy()))
                    .filter(e -> beforeAt == null || e.occurredAt() < beforeAt
                            || e.occurredAt() == beforeAt && (e.sourceId().compareTo(beforeId) < 0
                            || e.sourceId().equals(beforeId) && e.kind().compareTo(beforeKind) < 0))
                    .sorted(java.util.Comparator.comparingLong(TimelineEntry::occurredAt)
                            .thenComparing(TimelineEntry::sourceId).thenComparing(TimelineEntry::kind)).toList();
            return Future.succeededFuture(list.subList(Math.max(0, list.size() - limit), list.size()));
        }
    }
}
