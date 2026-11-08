package com.workoss.boot.vertx.contactcenter.core;

import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class UctpEventRouterTest {
    private Vertx vertx;
    private UctpEventRouter router;
    private final List<String> writes = new CopyOnWriteArrayList<>();
    private final Map<String, CompletableFuture<Void>> markers = new ConcurrentHashMap<>();
    private final Map<String, JsonObject> publicEvents = new ConcurrentHashMap<>();
    @BeforeEach void setup() throws Exception {
        vertx = Vertx.vertx(); ClusterWebSocketBroadcaster broadcaster = mock(ClusterWebSocketBroadcaster.class);
        when(broadcaster.sendToLocal(anyString(), any())).thenAnswer(i -> {
            String ws = i.getArgument(0); String wire = i.getArgument(1); String id = new JsonObject(wire).getString("id");
            publicEvents.put(id, new JsonObject(wire));
            writes.add(ws + ":" + id); CompletableFuture<Void> marker = markers.get(id); if (marker != null) marker.complete(null); return true;
        });
        router = new UctpEventRouter(vertx, broadcaster);
        CompletableFuture<Void> ready = new CompletableFuture<>();
        vertx.runOnContext(v -> { router.registerListener(); ready.complete(null); }); ready.get(5, TimeUnit.SECONDS);
        router.bindIdentity("member-ws", "member"); router.bindIdentity("intruder-ws", "intruder");
    }
    @AfterEach void close() throws Exception { vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS); }
    private JsonObject event(String id) { return new JsonObject().put("v", 1).put("type", "message.typing").put("id", id).put("payload", new JsonObject()); }
    private void barrier(String id) throws Exception {
        CompletableFuture<Void> marker = new CompletableFuture<>(); markers.put(id, marker);
        router.deliver(UctpEnvelope.toIdentity(event(id), "member")); marker.get(5, TimeUnit.SECONDS);
    }
    @Test void authenticatedIdentityAloneDoesNotAuthorizeGroupSubscription() throws Exception {
        router.joinGroup("intruder-ws", "c"); router.joinGroup("member-ws", "c");
        router.deliver(UctpEnvelope.toConversation(event("before-membership"), "c")); barrier("first-barrier");
        assertEquals(List.of("member-ws:first-barrier"), writes);
        router.conversationMembers("c", Set.of("member"));
        router.joinGroup("intruder-ws", "c"); router.joinGroup("member-ws", "c");
        router.deliver(UctpEnvelope.toConversation(event("authorized"), "c")); barrier("second-barrier");
        assertTrue(writes.contains("member-ws:authorized")); assertFalse(writes.contains("intruder-ws:authorized"));
    }
    @Test void rebindingCannotRetainThePreviousIdentityGroupMembership() throws Exception {
        router.conversationMembers("c", Set.of("member")); router.joinGroup("member-ws", "c");
        router.bindIdentity("member-ws", "intruder"); router.bindIdentity("marker-ws", "member");
        router.deliver(UctpEnvelope.toConversation(event("secret"), "c")); barrier("barrier");
        assertFalse(writes.stream().anyMatch(w -> w.endsWith(":secret"))); assertEquals("intruder", router.identityOf("member-ws"));
    }
    @Test void assignmentWaitsForBothDurableMembersBeforeClientCanInvite() throws Exception {
        JsonObject assigned = event("assignment").put("type", "workforce.assigned").put("cid", "c")
                .put("payload", new JsonObject().put("agent_identity_id", "peer"));
        router.deliver(UctpEnvelope.toIdentity(assigned, "member")); barrier("before-save");
        assertFalse(writes.contains("member-ws:assignment"));
        router.conversationMembers("c", Set.of("member")); barrier("customer-only");
        assertFalse(writes.contains("member-ws:assignment"));
        CompletableFuture<Void> assignedWrite = new CompletableFuture<>(); markers.put("assignment", assignedWrite);
        router.conversationMembers("c", Set.of("member", "peer")); assignedWrite.get(5, TimeUnit.SECONDS);
        assertTrue(writes.contains("member-ws:assignment"));
    }

    @Test void internalReceiptAudienceNeverAppearsOnTheV1Wire() throws Exception {
        JsonObject message = event("private-audience").put("type", "message.send")
                .put("_recipients", new io.vertx.core.json.JsonArray().add("member"));
        router.deliver(UctpEnvelope.toIdentity(message, "member")); barrier("barrier");
        assertEquals(1, publicEvents.get("private-audience").getInteger("v"));
        assertFalse(publicEvents.get("private-audience").containsKey("_recipients"));
        assertTrue(message.containsKey("_recipients"));
    }

    @Test void allLocalRouteModesObserveOnlyTheirRecipientsWithAnIndependentJsonCopy() throws Exception {
        Map<String, List<String>> observed = new ConcurrentHashMap<>();
        for (String ws : List.of("member-ws", "intruder-ws")) {
            router.registerReplyObserver(ws, reply -> {
                observed.computeIfAbsent(ws, ignored -> new CopyOnWriteArrayList<>()).add(reply.getString("id"));
                reply.getJsonObject("payload").put("observer-mutation", true);
            });
        }
        router.conversationMembers("c", Set.of("member"));
        router.joinGroup("member-ws", "c");
        router.joinGroup("intruder-ws", "c");
        router.deliver(UctpEnvelope.toConversation(event("group-reply").put("in_reply_to", "request"), "c"));
        router.deliver(UctpEnvelope.toIdentity(event("identity-reply").put("in_reply_to", "request"), "member"));
        router.deliver(UctpEnvelope.toWsSession(event("ws-reply").put("in_reply_to", "request"), "intruder-ws"));
        barrier("barrier");
        assertEquals(List.of("group-reply", "identity-reply"), observed.get("member-ws"));
        assertEquals(List.of("ws-reply"), observed.get("intruder-ws"));
        for (String id : List.of("group-reply", "identity-reply", "ws-reply")) {
            assertFalse(publicEvents.get(id).getJsonObject("payload").containsKey("observer-mutation"));
        }
    }

    @Test void clusterDeliveredRepliesUseTheSameLocalObserverWithoutChangingIdentityRouting() throws Exception {
        List<JsonObject> observed = new CopyOnWriteArrayList<>();
        router.registerReplyObserver("member-ws", observed::add);
        CompletableFuture<Void> marker = new CompletableFuture<>(); markers.put("remote-reply", marker);
        JsonObject remote = event("remote-reply").put("type", "ack").put("in_reply_to", "pending-request");
        vertx.eventBus().publish(UctpEventRouter.OUT_ADDRESS, new JsonObject().put("wire", remote.encode())
                .put("target", new JsonObject().put("mode", "identity").put("identity_id", "member")));
        marker.get(5, TimeUnit.SECONDS);
        assertEquals(List.of(remote), observed);
        assertTrue(writes.contains("member-ws:remote-reply"));
        assertFalse(writes.contains("intruder-ws:remote-reply"));
    }

    @Test void authenticationBroadcastsAndReplaysNeverReachReplyObserversAndUnregisterRemovesThem() throws Exception {
        List<JsonObject> observed = new CopyOnWriteArrayList<>();
        router.registerReplyObserver("member-ws", observed::add);
        router.deliver(UctpEnvelope.toIdentity(event("broadcast"), "member"));
        router.deliver(UctpEnvelope.toIdentity(event("auth-secret").put("type", "auth.session").put("in_reply_to", "request")
                .put("payload", new JsonObject().put("session_token", "synthetic-secret")), "member"));
        barrier("barrier");
        assertTrue(observed.isEmpty());
        JsonObject reply = event("replay").put("type", "ack").put("in_reply_to", "request");
        router.replayToLocal("member-ws", reply);
        assertTrue(writes.contains("member-ws:replay"));
        assertFalse(writes.contains("intruder-ws:replay"));
        assertTrue(observed.isEmpty());
        router.unregisterLocalSession("member-ws");
        router.deliver(UctpEnvelope.toIdentity(event("after-unregister").put("in_reply_to", "request"), "member"));
        barrier("after-barrier");
        assertTrue(observed.isEmpty());
        router.registerReplyObserver("member-ws", observed::add);
        router.unbind("member-ws");
        router.bindIdentity("member-ws", "member");
        router.deliver(UctpEnvelope.toIdentity(event("after-unbind").put("in_reply_to", "request"), "member"));
        barrier("last-barrier");
        assertTrue(observed.isEmpty());
    }

    @Test void closedOrRevokedMembershipClearsSubscriptions() throws Exception {
        router.conversationMembers("c", Set.of("member")); router.joinGroup("member-ws", "c");
        router.conversationMembers("c", Set.of());
        router.deliver(UctpEnvelope.toConversation(event("closed"), "c")); barrier("barrier");
        assertFalse(writes.stream().anyMatch(w -> w.endsWith(":closed")));
    }
}
