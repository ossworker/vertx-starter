package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.DatabaseException;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.workoss.boot.vertx.contactcenter.journal.SqlPoolTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Opt-in real PG tests: -Ductp.test.pg.enabled=true with optional .host/.port/.database/.user properties.
 * Uses a disposable, random schema in an explicitly designated local test database. No application
 * credentials/configuration are read; the test role must use local trust authentication and create schemas.
 * Without opt-in, JUnit records SKIP (not PASS); with opt-in, connection/SQL failures fail the test.
 */
@Timeout(60)
class PostgresDataLayerIntegrationTest {
    private Vertx vertx;
    private Pool admin;
    private Pool pool;
    private String schema;
    private SqlJournalRepository journal;
    private SqlConversationRepository conversations;

    @BeforeEach
    void connectToExplicitTestDatabase() throws Exception {
        assumeTrue(Boolean.getBoolean("uctp.test.pg.enabled"),
                "SKIP: real PostgreSQL not explicitly enabled; set -Ductp.test.pg.enabled=true for a local test database");
        vertx = Vertx.vertx();
        PgConnectOptions options = new PgConnectOptions()
                .setHost(System.getProperty("uctp.test.pg.host", "localhost"))
                .setPort(Integer.getInteger("uctp.test.pg.port", 5432))
                .setDatabase(System.getProperty("uctp.test.pg.database", "uctp_test"))
                .setUser(System.getProperty("uctp.test.pg.user", "uctp_test"))
                .setPassword("");
        admin = Pool.pool(vertx, options, new PoolOptions().setMaxSize(1));
        String candidate = "uctp_test_" + UUID.randomUUID().toString().replace("-", "");
        await(admin.query("CREATE SCHEMA " + candidate).execute());
        schema = candidate; // Cleanup only a schema this test actually created.
        pool = Pool.pool(vertx, new PgConnectOptions(options).addProperty("search_path", schema),
                new PoolOptions().setMaxSize(8));
        journal = new SqlJournalRepository(pool, true);
        conversations = new SqlConversationRepository(pool, true);
        await(journal.initialize());
        await(conversations.initialize());
        await(journal.initialize());
        await(conversations.initialize());
    }

    @AfterEach
    void deleteOnlyOwnedTestSchema() throws Exception {
        try {
            if (pool != null) {
                await(pool.close());
            }
            if (schema != null) {
                await(admin.query("DROP SCHEMA " + schema + " CASCADE").execute());
            }
        } finally {
            try {
                if (admin != null) {
                    await(admin.close());
                }
            } finally {
                if (vertx != null) {
                    await(vertx.close());
                }
            }
        }
    }

    @Test
    void journalConcurrentRetriesCollisionAndStableLatestHistory() throws Exception {
        List<Future<Boolean>> retries = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            retries.add(journal.append("cid", message("m1", "same")));
        }
        int inserted = 0;
        for (Future<Boolean> retry : retries) {
            if (await(retry)) {
                inserted++;
            }
        }
        assertEquals(1, inserted);
        assertFalse(await(journal.append("cid", message("m1", "same").put("id", "transport-retry"))));
        assertInstanceOf(IllegalStateException.class, failure(journal.append("other-cid", message("m1", "same"))));
        assertInstanceOf(IllegalStateException.class, failure(journal.append("cid", message("m1", "different"))));
        await(journal.append("cid", message("m3", "three")));
        await(journal.append("cid", message("m2", "two")));
        await(pool.preparedQuery("UPDATE uctp_journal SET created_at=$1 WHERE cid=$2").execute(Tuple.of(10L, "cid")));
        assertEquals(List.of("m2", "m3"), await(journal.history("cid", 2)).stream()
                .map(envelope -> envelope.getJsonObject("payload").getString("msg_id")).toList());
        assertTrue(await(journal.history("absent", 200)).isEmpty());
        assertInstanceOf(DatabaseException.class, failure(pool.query("SELECT * FROM absent_table").execute()));
        // Repository failures, too, must not turn into successful writes or empty history.
        await(pool.query("ALTER TABLE uctp_journal RENAME TO unavailable_journal").execute());
        assertInstanceOf(DatabaseException.class, failure(journal.append("cid", message("m4", "four"))));
        assertInstanceOf(DatabaseException.class, failure(journal.history("cid", 1)));
    }

    @Test
    void conversationRoundTripMemberReplacementClosedLookupAndActualRollback() throws Exception {
        JsonObject initial = snapshot("cid", "alice", "alice", "bob");
        await(conversations.save(initial));
        assertEquals(initial, await(conversations.load("cid")));
        assertEquals(1, await(conversations.findByIdentity("alice", 20)).size());
        JsonObject replacement = snapshot("cid", "bob").put("closed", true).put("closed_at", 200L);
        await(conversations.save(replacement));
        assertTrue(await(conversations.findByIdentity("alice", 20)).isEmpty());
        assertEquals(replacement, await(conversations.findByIdentity("bob", 20)).getFirst());
        await(pool.query("ALTER TABLE uctp_conversation_members ADD CONSTRAINT reject_test_member "
                + "CHECK (identity_id <> 'rejected')").execute());
        JsonObject rejected = snapshot("cid", "rejected").put("metadata", new JsonObject().put("should", "rollback"));
        assertInstanceOf(DatabaseException.class, failure(conversations.save(rejected)));
        assertEquals(replacement, await(conversations.load("cid")));
        assertEquals(1, await(conversations.findByIdentity("bob", 20)).size());
        assertTrue(await(conversations.findByIdentity("rejected", 20)).isEmpty());
        await(conversations.save(snapshot("cid")));
        assertTrue(await(conversations.findByIdentity("bob", 20)).isEmpty());
        assertNull(await(conversations.load("missing")));
    }

    @Test
    void sessionHistoryUpsertsInSnapshotTransactionAndSurvivesIndependentLookup() throws Exception {
        JsonObject conversation = snapshot("cid", "alice");
        JsonObject invited = session("cid", "s1", 100);
        await(conversations.save(conversation, List.of(invited)));
        JsonObject ended = invited.copy().put("status", "ENDED").put("accepted_at", 110L)
                .put("started_at", 120L).put("ended_at", 160L).put("end_reason", "normal-clearing").put("duration_ms", 40L);
        await(conversations.save(conversation, List.of(ended, session("cid", "s2", 200))));
        List<JsonObject> history = await(conversations.sessionHistory("cid", 200));
        assertEquals(2, history.size()); assertEquals(ended, history.get(0));
        assertEquals("s2", await(conversations.sessionHistory("cid", 1)).getFirst().getString("sid"));
        assertFalse(history.get(0).containsKey("connections"));

        await(pool.query("ALTER TABLE uctp_sessions ADD CONSTRAINT reject_test_session CHECK (sid <> 'rejected')").execute());
        JsonObject replacement = conversation.copy().put("metadata", new JsonObject().put("should", "rollback"));
        assertInstanceOf(DatabaseException.class, failure(conversations.save(replacement,
                List.of(session("cid", "rejected", 300)))));
        assertEquals(conversation, await(conversations.load("cid")));
        assertEquals(history, await(conversations.sessionHistory("cid", 200)));
    }

    @Test
    void messageAndSessionTimelineShareStableCursorSemantics() throws Exception {
        for (String msgId : List.of("m1", "shared", "m3")) {
            await(journal.append("cid", message(msgId, msgId)));
        }
        await(pool.preparedQuery("UPDATE uctp_journal SET created_at=$1 WHERE msg_id=$2").execute(Tuple.of(100L, "m1")));
        await(pool.preparedQuery("UPDATE uctp_journal SET created_at=$1 WHERE msg_id=$2").execute(Tuple.of(200L, "shared")));
        await(pool.preparedQuery("UPDATE uctp_journal SET created_at=$1 WHERE msg_id=$2").execute(Tuple.of(300L, "m3")));
        await(conversations.save(snapshot("cid", "alice"), List.of(
                session("cid", "s1", 150), session("cid", "shared", 200), session("cid", "s3", 250))));

        assertEquals(List.of("shared", "m3"), await(journal.timeline("cid", null, null, null, 2)).stream()
                .map(TimelineEntry::sourceId).toList());
        assertEquals(List.of("shared", "s3"), await(conversations.sessionTimeline("cid", null, null, null, 2)).stream()
                .map(TimelineEntry::sourceId).toList());
        assertEquals(List.of("m1", "shared"), await(journal.timeline("cid", 200L, "shared", "session", 201)).stream()
                .map(TimelineEntry::sourceId).toList());
        assertEquals(List.of("m1"), await(journal.timeline("cid", 200L, "shared", "message", 201)).stream()
                .map(TimelineEntry::sourceId).toList());
        assertEquals(List.of("s1"), await(conversations.sessionTimeline("cid", 200L, "shared", "session", 201)).stream()
                .map(TimelineEntry::sourceId).toList());
    }

    private static JsonObject session(String cid, String sid, long invitedAt) {
        return new JsonObject().put("cid", cid).put("sid", sid).put("medium", "voice")
                .put("status", "INVITING").put("initiator_participant_id", "p0")
                .put("participant_ids", new JsonArray().add("p0")).put("invited_at", invitedAt);
    }

    private static JsonObject message(String msgId, String body) {
        return new JsonObject().put("type", "message.send").put("id", "transport-" + msgId)
                .put("payload", new JsonObject().put("msg_id", msgId).put("from", "participant")
                        .put("content_type", "text/plain").put("body", body));
    }

    private static JsonObject snapshot(String cid, String... identities) {
        JsonArray participants = new JsonArray();
        for (String identity : identities) {
            participants.add(new JsonObject().put("identity_id", identity).put("participant_id", "p-" + participants.size())
                    .put("role", "customer").put("kind", "human"));
        }
        return new JsonObject().put("cid", cid).put("tenant_id", "tenant").put("policy", "persistent")
                .put("idle_close_secs", 30).put("opened_at", 100L).put("closed", false).putNull("closed_at")
                .put("metadata", new JsonObject().put("title", "test")).put("participants", participants);
    }
}
