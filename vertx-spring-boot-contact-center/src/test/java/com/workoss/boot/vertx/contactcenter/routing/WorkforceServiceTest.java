package com.workoss.boot.vertx.contactcenter.routing;

import com.workoss.boot.vertx.contactcenter.core.Conversation;
import com.workoss.boot.vertx.contactcenter.core.ConversationService;
import com.workoss.boot.vertx.contactcenter.core.UctpEventRouter;
import com.workoss.boot.vertx.contactcenter.uctp.AuthPrincipal;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Outbound;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope.Route.IdentityTarget;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkforceServiceTest {

    private Vertx vertx;
    private UctpEventRouter router;
    private ConversationService conversations;
    private WorkforceService workforce;
    private final List<Outbound> delivered = new CopyOnWriteArrayList<>();
    private final List<JsonObject> conversationOps = new CopyOnWriteArrayList<>();
    private final Map<String, Boolean> online = new ConcurrentHashMap<>();
    private final AtomicReference<Consumer<String>> closed = new AtomicReference<>();
    private final AtomicReference<BiConsumer<String, String>> sessionEnded = new AtomicReference<>();
    private final AtomicInteger requestIds = new AtomicInteger();

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        router = mock(UctpEventRouter.class);
        conversations = mock(ConversationService.class);
        UctpIds ids = mock(UctpIds.class);
        Map<String, AtomicInteger> sequences = new ConcurrentHashMap<>();
        when(ids.prefixed(anyString())).thenAnswer(invocation -> {
            String prefix = invocation.getArgument(0);
            return prefix + "_" + sequences.computeIfAbsent(prefix, ignored -> new AtomicInteger()).incrementAndGet();
        });
        when(router.identityOnline(anyString())).thenAnswer(invocation ->
                online.getOrDefault(invocation.getArgument(0), true));
        doAnswer(invocation -> {
            Outbound outbound = invocation.getArgument(0);
            delivered.add(new Outbound(outbound.envelope().copy(), outbound.route()));
            return null;
        }).when(router).deliver(any(Outbound.class));
        doAnswer(invocation -> {
            JsonObject op = invocation.getArgument(0);
            conversationOps.add(op.copy());
            return Future.succeededFuture();
        }).when(conversations).broadcastConvOp(any(JsonObject.class));
        doAnswer(invocation -> {
            closed.set(invocation.getArgument(0));
            return null;
        }).when(conversations).conversationClosedListener(any());
        doAnswer(invocation -> {
            sessionEnded.set(invocation.getArgument(0));
            return null;
        }).when(conversations).sessionEndedListener(any());
        workforce = new WorkforceService(vertx, router, conversations, ids, new UctpEnvelope(ids));
    }

    @AfterEach
    void tearDown() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void oneAgentAcceptsTwoConversationsWithoutBecomingBusy() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");

        assertEquals("agent-a", assignedAgent(first));
        assertEquals("agent-a", assignedAgent(second));
        JsonObject agent = agent("agent-a");
        assertAssignments(agent, first, second);
        assertEquals("available", agent.getString("state"));
        assertEquals(2, agent.getInteger("handled"));
        assertFalse(agent.containsKey("busy_cid"));
    }

    @Test
    void closingOneConversationLeavesOtherAssignmentAndBreakIntact() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        status("agent-a", "break");

        onLoop(() -> {
            closed.get().accept(first);
            closed.get().accept(first);
            return null;
        });

        JsonObject agent = agent("agent-a");
        assertAssignments(agent, second);
        assertEquals("break", agent.getString("state"));
        assertEquals(2, agent.getInteger("handled"));
    }

    @Test
    void endingSessionsKeepsChatConversationAssigned() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");

        onLoop(() -> {
            sessionEnded.get().accept(first, "sid-1");
            sessionEnded.get().accept(first, "sid-2");
            return null;
        });

        assertAssignments(agent("agent-a"), first, second);
        assertEquals("agent-a", assignedAgent(enqueue("customer-c")));
    }

    @Test
    void busyBlocksOnlyNewAssignmentsAndAvailableDrainsQueue() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        int opsBefore = conversationOps.size();
        status("agent-a", "busy");
        assertEquals(opsBefore, conversationOps.size());
        String third = enqueue("customer-c");
        String fourth = enqueue("customer-d");

        onLoop(() -> {
            sessionEnded.get().accept(first, "sid-1");
            return null;
        });
        assertAssignments(agent("agent-a"), first, second);
        assertFalse(hasAssignment(third));
        assertFalse(hasAssignment(fourth));
        assertEquals("busy", agent("agent-a").getString("state"));
        assertEquals(2, agent("agent-a").getInteger("handled"));

        status("agent-a", "available");

        assertEquals("agent-a", assignedAgent(third));
        assertEquals("agent-a", assignedAgent(fourth));
        assertAssignments(agent("agent-a"), first, second, third, fourth);
        assertEquals("available", agent("agent-a").getString("state"));
    }

    @Test
    void breakBlocksOnlyNewAssignmentsAndAvailableDrainsQueue() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        int opsBefore = conversationOps.size();
        status("agent-a", "break");
        assertEquals(opsBefore, conversationOps.size());
        String third = enqueue("customer-c");

        assertAssignments(agent("agent-a"), first, second);
        assertFalse(hasAssignment(third));
        assertEquals("break", agent("agent-a").getString("state"));

        status("agent-a", "available");

        assertEquals("agent-a", assignedAgent(third));
        assertAssignments(agent("agent-a"), first, second, third);
        assertEquals("available", agent("agent-a").getString("state"));
    }

    @Test
    void invalidOrMissingStatusReturns400WithoutChangingStateOrAssignments() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        online.put("waiting-customer", false);
        String waiting = enqueue("waiting-customer");
        List<JsonObject> invalidPayloads = List.of(
                new JsonObject(),
                new JsonObject().put("state", (Object) null),
                new JsonObject().put("state", "unknown"),
                new JsonObject().put("state", ""),
                new JsonObject().put("state", "AVAILABLE"),
                new JsonObject().put("state", "BUSY"),
                new JsonObject().put("state", "BREAK"),
                new JsonObject().put("state", " busy "),
                new JsonObject().put("state", 1),
                new JsonObject().put("state", true),
                new JsonObject().put("state", new JsonArray().add("available")),
                new JsonObject().put("state", new JsonObject()));

        for (String previousState : List.of("available", "busy", "break")) {
            status("agent-a", previousState);
            JsonObject before = agent("agent-a");
            online.put("waiting-customer", true);
            List<JsonObject> invalidRequests = new ArrayList<>();
            invalidPayloads.forEach(payload -> invalidRequests.add(envelope("workforce.status", payload)));
            invalidRequests.add(envelope("workforce.status", null));
            invalidRequests.add(envelope("workforce.status", new JsonObject()).put("payload", "busy"));
            JsonObject missingPayload = envelope("workforce.status", new JsonObject());
            missingPayload.remove("payload");
            invalidRequests.add(missingPayload);

            for (JsonObject request : invalidRequests) {
                int deliveredBefore = delivered.size();
                int opsBefore = conversationOps.size();
                onLoop(() -> {
                    workforce.handle(principal("agent-a"), request);
                    return null;
                });

                assertEquals(deliveredBefore + 1, delivered.size());
                assertError(request.getString("id"), "agent-a", 400, "invalid-agent-state");
                assertEquals(opsBefore, conversationOps.size());
                assertEquals(before, agent("agent-a"));
                assertAssignments(agent("agent-a"), first, second);
                assertFalse(hasAssignment(waiting));
            }
            online.put("waiting-customer", false);
        }

        online.put("waiting-customer", true);
        status("agent-a", "available");
        assertEquals("agent-a", assignedAgent(waiting));
        assertAssignments(agent("agent-a"), first, second, waiting);
    }

    @Test
    void reloginAndRenamePreserveBusyAssignmentsHandledAndLastAssignedAt() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        status("agent-a", "busy");
        String third = enqueue("customer-c", "sales");
        Instant before = onLoop(() -> record("agent-a").lastAssignedAt);

        command("agent-a", "workforce.login", new JsonObject()
                .put("display_name", "Renamed")
                .put("skills", new JsonArray().add("sales")));
        assertEquals("busy", agent("agent-a").getString("state"));
        assertFalse(hasAssignment(third));
        login("agent-a", "sales");

        JsonObject agent = agent("agent-a");
        assertAssignments(agent, first, second);
        assertEquals("busy", agent.getString("state"));
        assertTrue(agent.getBoolean("logged_in"));
        assertEquals("Renamed", agent.getString("display_name"));
        assertEquals(2, agent.getInteger("handled"));
        assertEquals(new JsonArray().add("sales"), agent.getJsonArray("skills"));
        assertEquals(before, onLoop(() -> record("agent-a").lastAssignedAt));
        assertFalse(hasAssignment(third));

        status("agent-a", "available");
        assertAssignments(agent("agent-a"), first, second, third);
    }

    @Test
    void reloginAndRenamePreserveBreakAndExistingConversation() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        status("agent-a", "break");
        String second = enqueue("customer-b");

        command("agent-a", "workforce.login", new JsonObject().put("display_name", "Renamed"));
        login("agent-a");

        JsonObject agent = agent("agent-a");
        assertAssignments(agent, first);
        assertEquals("break", agent.getString("state"));
        assertEquals("Renamed", agent.getString("display_name"));
        assertFalse(hasAssignment(second));
    }

    @Test
    void logoutPreservesTrackingButRequiresLoginBeforeTakingNewWork() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        command("agent-a", "workforce.logout", new JsonObject());
        String third = enqueue("customer-c");
        status("agent-a", "available");

        JsonObject agent = agent("agent-a");
        assertAssignments(agent, first, second);
        assertEquals("break", agent.getString("state"));
        assertFalse(agent.getBoolean("logged_in"));
        assertFalse(hasAssignment(third));
        assertTrue(delivered.stream().anyMatch(outbound -> "error".equals(UctpEnvelope.type(outbound.envelope()))
                && "agent-not-logged-in".equals(outbound.envelope().getJsonObject("payload").getString("reason"))));

        onLoop(() -> {
            closed.get().accept(first);
            return null;
        });
        assertAssignments(agent("agent-a"), second);
        login("agent-a");
        assertAssignments(agent("agent-a"), second, third);
        assertTrue(agent("agent-a").getBoolean("logged_in"));
        assertEquals("available", agent("agent-a").getString("state"));
    }

    @Test
    void unauthorizedHandoffReturns403WithoutChangingAgentsOrQueuedTransfer() throws Exception {
        login("agent-a", "sales");
        String first = enqueue("customer-a", "sales");
        String second = enqueue("customer-b", "sales");
        login("agent-b", "support");
        String other = enqueue("customer-c", "support");
        command("agent-a", "workforce.handoff", new JsonObject()
                .put("cid", first).put("customer_identity_id", "customer-a")
                .put("skills", new JsonArray().add("sales")).put("sid", "sid-1"));
        command("agent-a", "workforce.logout", new JsonObject());
        JsonObject sourceBefore = agent("agent-a");
        JsonObject otherBefore = agent("agent-b");

        for (String caller : List.of("unregistered", "agent-b", "agent-a")) {
            for (String cid : List.of(first, "cid-not-assigned")) {
                int deliveredBefore = delivered.size();
                int opsBefore = conversationOps.size();
                String requestId = command(caller, "workforce.handoff", new JsonObject()
                        .put("cid", cid).put("customer_identity_id", "customer-a")
                        .put("skills", new JsonArray().add("support")).put("sid", "sid-forged"));

                assertEquals(deliveredBefore + 1, delivered.size());
                assertError(requestId, caller, 403, "handoff-not-authorized");
                assertEquals(opsBefore, conversationOps.size());
                assertEquals(sourceBefore, agent("agent-a"));
                assertEquals(otherBefore, agent("agent-b"));
                assertEquals(2, assignmentCount(first));
                assertFalse(hasAssignment("cid-not-assigned"));
            }
        }

        login("agent-c", "sales");
        assertEquals("agent-c", assignedAgent(first));
        assertAssignments(agent("agent-a"), second);
        assertAssignments(agent("agent-b"), other);
        assertAssignments(agent("agent-c"), first);
        List<JsonObject> roleChanges = conversationOps.stream()
                .filter(op -> "assign-agent".equals(op.getString("op")) && op.getString("from_identity_id") != null).toList();
        assertEquals(1, roleChanges.size());
        assertEquals(first, roleChanges.getFirst().getString("cid"));
        assertEquals("agent-a", roleChanges.getFirst().getString("from_identity_id"));
        assertEquals("agent-c", roleChanges.getFirst().getString("identity_id"));
        assertEquals("sid-1", roleChanges.getFirst().getString("sid"));
    }

    @Test
    void handoffExcludesSourceAndReleasesOnlyTransferredConversationWithoutLocalCopy() throws Exception {
        login("agent-a");
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        status("agent-a", "busy");
        handoff("agent-a", first, "customer-a");

        assertAssignments(agent("agent-a"), first, second);
        assertEquals(2, assignmentCount(first));
        login("agent-b");

        assertEquals("agent-b", assignedAgent(first));
        assertAssignments(agent("agent-a"), second);
        assertEquals("busy", agent("agent-a").getString("state"));
        assertAssignments(agent("agent-b"), first);
        assertEquals(2, agent("agent-a").getInteger("handled"));
        assertEquals(1, agent("agent-b").getInteger("handled"));
        assertTrue(conversationOps.stream().anyMatch(op -> "assign-agent".equals(op.getString("op"))
                && first.equals(op.getString("cid"))
                && "agent-a".equals(op.getString("from_identity_id"))
                && "agent-b".equals(op.getString("identity_id"))));
        verify(conversations, never()).conversation(anyString());

        onLoop(() -> {
            closed.get().accept(first);
            return null;
        });
        assertAssignments(agent("agent-a"), second);
        assertAssignments(agent("agent-b"));
    }

    @Test
    void handoffDoesNotSelectAnAgentAlreadyAssignedSameConversation() throws Exception {
        login("agent-a");
        String cid = enqueue("customer-a");
        login("agent-b");
        onLoop(() -> {
            // 模拟两名坐席已共同接待同一 cid，确保转接源有权提交。
            record("agent-b").assignedConversations.add(cid);
            return null;
        });
        handoff("agent-b", cid, "customer-a");

        assertEquals(2, assignmentCount(cid));
        assertAssignments(agent("agent-a"), cid);
        assertAssignments(agent("agent-b"), cid);
        assertEquals(0, conversationOps.stream().filter(op -> "role-change".equals(op.getString("op"))).count());

        login("agent-c");
        assertEquals("agent-c", assignedAgent(cid));
        assertAssignments(agent("agent-a"), cid);
        assertAssignments(agent("agent-b"));
        assertAssignments(agent("agent-c"), cid);
    }

    @Test
    void assignmentUsesOnlineSkillsLoadThenOldestTimeAndStableIdentity() throws Exception {
        login("agent-b", "sales");
        login("agent-a", "sales");
        login("agent-0", "support");
        online.put("agent-0", false);
        online.put("agent-a", false);
        String first = enqueue("customer-a", "sales");
        assertEquals("agent-b", assignedAgent(first));

        online.put("agent-a", true);
        String second = enqueue("customer-b", "sales");
        assertEquals("agent-a", assignedAgent(second));
        onLoop(() -> {
            record("agent-b").lastAssignedAt = Instant.EPOCH;
            record("agent-a").lastAssignedAt = Instant.EPOCH.plusSeconds(1);
            return null;
        });
        assertEquals("agent-b", assignedAgent(enqueue("customer-c", "sales")));

        onLoop(() -> {
            record("agent-b").lastAssignedAt = Instant.EPOCH;
            return null;
        });
        assertEquals("agent-a", assignedAgent(enqueue("customer-d", "sales")));
        onLoop(() -> {
            record("agent-a").lastAssignedAt = Instant.EPOCH;
            record("agent-b").lastAssignedAt = Instant.EPOCH;
            return null;
        });
        assertEquals("agent-a", assignedAgent(enqueue("customer-e", "sales")));
        assertFalse(hasAssignment(enqueue("customer-f", "support")));
    }

    @Test
    void externalAndWebSocketConversationsShareSameLoadAccounting() throws Exception {
        login("agent-a");
        online.put("external-customer", false);
        when(conversations.conversation(anyString())).thenReturn(mock(Conversation.class));
        String external = onLoop(() -> workforce.enqueueExternal("external-customer", "external", "SMS",
                Set.of(), 0, new JsonObject().put("channel", "sms")));
        String webSocket = enqueue("customer-a");

        assertEquals("agent-a", assignedAgent(external));
        assertEquals("agent-a", assignedAgent(webSocket));
        assertAssignments(agent("agent-a"), external, webSocket);
        login("agent-b");
        assertEquals("agent-b", assignedAgent(enqueue("customer-b")));
    }

    @Test
    void externalCustomerHandoffDoesNotRequireAWebSocketConnection() throws Exception {
        login("agent-a");
        online.put("external-customer", false);
        String cid = onLoop(() -> workforce.enqueueExternal("external-customer", "external", "SMS",
                Set.of(), 0, new JsonObject().put("channel", "sms")));
        login("agent-b");
        handoff("agent-a", cid, "external-customer");
        assertAssignments(agent("agent-a"));
        assertAssignments(agent("agent-b"), cid);
        assertTrue(conversationOps.stream().anyMatch(op -> "assign-agent".equals(op.getString("op"))
                && "agent-b".equals(op.getString("identity_id"))
                && "external-customer".equals(op.getString("customer_identity_id"))));
    }

    @Test
    void handoffCannotInventExternalStatusForAnOfflineWebSocketCustomer() throws Exception {
        login("agent-a");
        String cid = enqueue("customer-a");
        login("agent-b");
        online.put("customer-a", false);
        command("agent-a", "workforce.handoff", new JsonObject().put("cid", cid)
                .put("customer_identity_id", "customer-a").put("external", true));
        assertAssignments(agent("agent-a"), cid);
        assertAssignments(agent("agent-b"));
    }

    @Test
    void closingQueuedConversationPreventsLaterAssignment() throws Exception {
        String cid = enqueue("customer-a");
        onLoop(() -> {
            closed.get().accept(cid);
            return null;
        });
        login("agent-a");

        assertFalse(hasAssignment(cid));
        assertAssignments(agent("agent-a"));
    }

    @Test
    void registeredEventBusConsumerExecutesOnVertxEventLoop() throws Exception {
        CompletableFuture<Void> reply = new CompletableFuture<>();
        doAnswer(invocation -> {
            Outbound outbound = invocation.getArgument(0);
            delivered.add(new Outbound(outbound.envelope().copy(), outbound.route()));
            if ("workforce.roster".equals(UctpEnvelope.type(outbound.envelope()))) {
                assertTrue(Vertx.currentContext().isEventLoopContext());
                reply.complete(null);
            }
            return null;
        }).when(router).deliver(any(Outbound.class));
        onLoop(() -> {
            workforce.registerListener();
            vertx.eventBus().publish(WorkforceService.ADDRESS, new JsonObject()
                    .put("principal", principal("agent-a").toJson())
                    .put("env", envelope("workforce.login", new JsonObject())));
            return null;
        });
        reply.get(5, TimeUnit.SECONDS);
        assertEquals("available", agent("agent-a").getString("state"));
    }

    @Test
    void enqueueWaitsForCustomerSaveAndFailureDoesNotQueueOrAssign() throws Exception {
        login("agent-a");
        Promise<Void> saved = Promise.promise();
        doAnswer(invocation -> {
            JsonObject op = invocation.getArgument(0);
            conversationOps.add(op.copy());
            return "add-participant".equals(op.getString("op")) ? saved.future() : Future.succeededFuture();
        }).when(conversations).broadcastConvOp(any(JsonObject.class));
        String request = command("customer-a", "workforce.enqueue", new JsonObject());
        assertFalse(delivered.stream().anyMatch(outbound -> request.equals(outbound.envelope().getString("in_reply_to"))));
        assertAssignments(agent("agent-a"));

        onLoop(() -> { saved.fail("save-unavailable"); return null; });

        assertError(request, "customer-a", 503, "enqueue-save-failed");
        status("agent-a", "available");
        assertAssignments(agent("agent-a"));
        assertFalse(hasAssignment("cid_1"));
    }

    @Test
    void successfulCustomerSavePrecedesQueuedAndAssignment() throws Exception {
        login("agent-a");
        Promise<Void> saved = Promise.promise();
        doAnswer(invocation -> {
            JsonObject op = invocation.getArgument(0);
            conversationOps.add(op.copy());
            return "add-participant".equals(op.getString("op")) ? saved.future() : Future.succeededFuture();
        }).when(conversations).broadcastConvOp(any(JsonObject.class));
        String request = command("customer-a", "workforce.enqueue", new JsonObject());
        assertFalse(hasAssignment("cid_1"));
        onLoop(() -> { saved.complete(); return null; });
        assertEquals(1, delivered.stream().filter(outbound -> request.equals(outbound.envelope().getString("in_reply_to"))
                && "workforce.queued".equals(outbound.envelope().getString("type"))).count());
        assertEquals("agent-a", assignedAgent("cid_1"));
    }

    @Test
    void pendingAssignmentsReserveLoadWithoutPrematureNotifications() throws Exception {
        login("agent-a");
        login("agent-b");
        Map<String, Promise<Void>> saves = new ConcurrentHashMap<>();
        doAnswer(invocation -> {
            JsonObject op = invocation.getArgument(0);
            conversationOps.add(op.copy());
            if (!"assign-agent".equals(op.getString("op"))) return Future.succeededFuture();
            Promise<Void> save = Promise.promise();
            saves.put(op.getString("cid"), save);
            return save.future();
        }).when(conversations).broadcastConvOp(any(JsonObject.class));
        String first = enqueue("customer-a");
        String second = enqueue("customer-b");
        assertFalse(hasAssignment(first));
        assertFalse(hasAssignment(second));
        assertAssignments(agent("agent-a"));
        assertAssignments(agent("agent-b"));
        assertEquals(List.of("agent-a", "agent-b"), conversationOps.stream()
                .filter(op -> "assign-agent".equals(op.getString("op")))
                .map(op -> op.getString("identity_id")).toList());
        onLoop(() -> { saves.get(first).complete(); saves.get(second).complete(); return null; });
        assertEquals("agent-a", assignedAgent(first));
        assertEquals("agent-b", assignedAgent(second));
    }

    @Test
    void failedAssignmentRollsBackReservationAndRetainsQueueForExplicitRetry() throws Exception {
        login("agent-a");
        Promise<Void> saved = Promise.promise();
        AtomicInteger attempts = new AtomicInteger();
        doAnswer(invocation -> {
            JsonObject op = invocation.getArgument(0);
            conversationOps.add(op.copy());
            return "assign-agent".equals(op.getString("op")) && attempts.incrementAndGet() == 1
                    ? saved.future() : Future.succeededFuture();
        }).when(conversations).broadcastConvOp(any(JsonObject.class));
        String cid = enqueue("customer-a");
        onLoop(() -> { saved.fail("save-unavailable"); return null; });
        assertFalse(hasAssignment(cid));
        assertAssignments(agent("agent-a"));
        assertEquals(0, agent("agent-a").getInteger("handled"));
        assertTrue(delivered.stream().anyMatch(outbound -> "error".equals(outbound.envelope().getString("type"))
                && "assignment-save-failed".equals(outbound.envelope().getJsonObject("payload").getString("reason"))));
        status("agent-a", "available");
        assertEquals("agent-a", assignedAgent(cid));
        assertEquals(1, agent("agent-a").getInteger("handled"));
    }

    @Test
    void failedTransferKeepsSourceAndDoesNotPublishNewAssignment() throws Exception {
        login("agent-a");
        String cid = enqueue("customer-a");
        login("agent-b");
        Promise<Void> saved = Promise.promise();
        doAnswer(invocation -> saved.future()).when(conversations).broadcastConvOp(any(JsonObject.class));
        handoff("agent-a", cid, "customer-a");
        assertAssignments(agent("agent-a"), cid);
        assertAssignments(agent("agent-b"));
        onLoop(() -> { saved.fail("save-unavailable"); return null; });
        assertAssignments(agent("agent-a"), cid);
        assertAssignments(agent("agent-b"));
        assertEquals(2, assignmentCount(cid));
        assertEquals(0, agent("agent-b").getInteger("handled"));
    }

    @Test
    void closeWhileAssignmentIsSavingDoesNotResurrectReservationOrQueue() throws Exception {
        login("agent-a");
        Promise<Void> saved = Promise.promise();
        doAnswer(invocation -> {
            JsonObject op = invocation.getArgument(0);
            return "assign-agent".equals(op.getString("op")) ? saved.future() : Future.succeededFuture();
        }).when(conversations).broadcastConvOp(any(JsonObject.class));
        String cid = enqueue("customer-a");
        onLoop(() -> { closed.get().accept(cid); saved.complete(); return null; });
        status("agent-a", "available");
        assertFalse(hasAssignment(cid));
        assertAssignments(agent("agent-a"));
        assertEquals(0, agent("agent-a").getInteger("handled"));
    }

    private void login(String identityId, String... skills) throws Exception {
        command(identityId, "workforce.login", new JsonObject().put("skills", new JsonArray(List.of(skills))));
    }

    private void status(String identityId, String state) throws Exception {
        command(identityId, "workforce.status", new JsonObject().put("state", state));
    }

    private String enqueue(String customer, String... skills) throws Exception {
        String requestId = command(customer, "workforce.enqueue",
                new JsonObject().put("skills", new JsonArray(List.of(skills))));
        return delivered.stream()
                .map(Outbound::envelope)
                .filter(env -> "workforce.queued".equals(UctpEnvelope.type(env)))
                .filter(env -> requestId.equals(env.getString("in_reply_to")))
                .findFirst().orElseThrow().getJsonObject("payload").getString("cid");
    }

    private void handoff(String source, String cid, String customer) throws Exception {
        command(source, "workforce.handoff", new JsonObject()
                .put("cid", cid).put("customer_identity_id", customer).put("sid", "sid-1"));
    }

    private String command(String identityId, String type, JsonObject payload) throws Exception {
        JsonObject env = envelope(type, payload);
        onLoop(() -> {
            workforce.handle(principal(identityId), env);
            return null;
        });
        return env.getString("id");
    }

    private JsonObject envelope(String type, JsonObject payload) {
        return new JsonObject().put("v", 1).put("id", "request-" + requestIds.incrementAndGet())
                .put("ts", Instant.now().toString()).put("type", type).put("payload", payload);
    }

    private static AuthPrincipal principal(String identityId) {
        return new AuthPrincipal(identityId, identityId, "human", "agent", new JsonObject(), "identified", null);
    }

    private JsonObject agent(String identityId) throws Exception {
        String requestId = command(identityId, "workforce.agents", new JsonObject());
        JsonArray roster = delivered.stream()
                .map(Outbound::envelope)
                .filter(env -> "workforce.roster".equals(UctpEnvelope.type(env)))
                .filter(env -> requestId.equals(env.getString("in_reply_to")))
                .findFirst().orElseThrow().getJsonObject("payload").getJsonArray("agents");
        return roster.stream().map(JsonObject.class::cast)
                .filter(record -> identityId.equals(record.getString("identity_id")))
                .findFirst().orElseThrow();
    }

    private static void assertAssignments(JsonObject agent, String... cids) {
        assertEquals(cids.length, agent.getInteger("active_conversations"));
        assertEquals(Set.of(cids), Set.copyOf(agent.getJsonArray("assigned_cids").getList()));
    }

    private void assertError(String requestId, String identityId, int code, String reason) {
        List<Outbound> replies = delivered.stream()
                .filter(outbound -> requestId.equals(outbound.envelope().getString("in_reply_to")))
                .toList();
        assertEquals(1, replies.size());
        Outbound reply = replies.getFirst();
        assertEquals(new IdentityTarget(identityId), reply.route());
        assertEquals("error", UctpEnvelope.type(reply.envelope()));
        assertEquals(code, reply.envelope().getJsonObject("payload").getInteger("code"));
        assertEquals(reason, reply.envelope().getJsonObject("payload").getString("reason"));
    }

    private boolean hasAssignment(String cid) {
        return delivered.stream().anyMatch(outbound -> isAssignment(outbound, cid));
    }

    private long assignmentCount(String cid) {
        return delivered.stream().filter(outbound -> isAssignment(outbound, cid)).count();
    }

    private String assignedAgent(String cid) {
        List<Outbound> assignments = delivered.stream().filter(outbound -> isAssignment(outbound, cid))
                .filter(outbound -> outbound.envelope().getJsonObject("payload").containsKey("agent_identity_id"))
                .toList();
        return assignments.getLast().envelope().getJsonObject("payload").getString("agent_identity_id");
    }

    private static boolean isAssignment(Outbound outbound, String cid) {
        return outbound.route() instanceof IdentityTarget
                && "workforce.assigned".equals(UctpEnvelope.type(outbound.envelope()))
                && cid.equals(outbound.envelope().getString("cid"));
    }

    @SuppressWarnings("unchecked")
    private WorkforceService.AgentRecord record(String identityId) {
        try {
            Field field = WorkforceService.class.getDeclaredField("agents");
            field.setAccessible(true);
            return ((Map<String, WorkforceService.AgentRecord>) field.get(workforce)).get(identityId);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    private <T> T onLoop(Supplier<T> action) throws Exception {
        CompletableFuture<T> result = new CompletableFuture<>();
        vertx.runOnContext(ignored -> {
            try {
                assertTrue(Vertx.currentContext().isEventLoopContext());
                result.complete(action.get());
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }
}
