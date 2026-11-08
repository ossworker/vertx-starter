package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static com.workoss.boot.vertx.contactcenter.journal.SqlPoolTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SqlConversationRepositoryTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void snapshotAndDeduplicatedMembershipCommitInOneTransaction(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class, CALLS_REAL_METHODS);
        SqlConnection connection = mock(SqlConnection.class);
        Transaction transaction = mock(Transaction.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.getConnection()).thenReturn(Future.succeededFuture(connection));
        when(connection.begin()).thenReturn(Future.succeededFuture(transaction));
        when(connection.close()).thenReturn(Future.succeededFuture());
        when(transaction.commit()).thenReturn(Future.succeededFuture());
        when(transaction.rollback()).thenReturn(Future.succeededFuture());
        when(connection.preparedQuery(anyString())).thenReturn(query);
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(1)));
        when(query.executeBatch(anyList())).thenReturn(Future.succeededFuture(rows(1)));
        JsonObject snapshot = snapshot("alice", "alice", "bob");
        await(new SqlConversationRepository(pool, postgres).save(snapshot));
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(connection, times(3)).preparedQuery(sql.capture());
        assertTrue(sql.getAllValues().get(0).contains(postgres ? "ON CONFLICT (cid) DO UPDATE" : "ON DUPLICATE KEY UPDATE"));
        assertTrue(sql.getAllValues().get(0).contains(postgres ? "$3" : "?, ?, ?"));
        assertTrue(sql.getAllValues().get(1).startsWith("DELETE FROM uctp_conversation_members"));
        ArgumentCaptor<Tuple> tuples = ArgumentCaptor.forClass(Tuple.class);
        verify(query, times(2)).execute(tuples.capture());
        assertEquals(snapshot, new JsonObject(tuples.getAllValues().get(0).getString(1)));
        ArgumentCaptor<List<Tuple>> members = ArgumentCaptor.forClass(List.class);
        verify(query).executeBatch(members.capture());
        assertEquals(List.of("alice", "bob"), members.getValue().stream().map(tuple -> tuple.getString(1)).toList());
        var ordered = inOrder(query, transaction, connection);
        ordered.verify(query).execute(any(Tuple.class));
        ordered.verify(query).execute(any(Tuple.class));
        ordered.verify(query).executeBatch(anyList());
        ordered.verify(transaction).commit();
        ordered.verify(connection).close();
        verify(pool, never()).preparedQuery(anyString());
        verify(transaction, never()).rollback();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void memberFailureRollsBackSnapshotAndPropagates(boolean postgres) {
        Pool pool = mock(Pool.class, CALLS_REAL_METHODS);
        SqlConnection connection = mock(SqlConnection.class);
        Transaction transaction = mock(Transaction.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.getConnection()).thenReturn(Future.succeededFuture(connection));
        when(connection.begin()).thenReturn(Future.succeededFuture(transaction));
        when(connection.close()).thenReturn(Future.succeededFuture());
        when(transaction.rollback()).thenReturn(Future.succeededFuture());
        when(connection.preparedQuery(anyString())).thenReturn(query);
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(1)));
        Throwable unavailable = databaseFailure(postgres, false);
        when(query.executeBatch(anyList())).thenReturn(Future.failedFuture(unavailable));
        assertSame(unavailable, failure(new SqlConversationRepository(pool, postgres).save(snapshot("alice"))));
        verify(transaction).rollback();
        verify(transaction, never()).commit();
        verify(connection).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void loadDistinguishesMissingFromFailureAndIdentityIncludesClosed(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(query);
        JsonObject closed = snapshot("alice").put("closed", true).put("closed_at", 200L);
        Row row = mock(Row.class);
        when(row.getString("snapshot")).thenReturn(closed.encode());
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(0)), Future.succeededFuture(rows(0, row)));
        SqlConversationRepository repository = new SqlConversationRepository(pool, postgres);
        assertNull(await(repository.load("missing")));
        assertEquals(closed, await(repository.findByIdentity("alice' --", 200)).getFirst());
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool, times(2)).preparedQuery(sql.capture());
        assertFalse(sql.getValue().contains("alice'"));
        assertFalse(sql.getValue().contains("closed"));
        assertFalse(sql.getValue().toUpperCase().contains("JSON"));
        assertTrue(sql.getValue().endsWith("ORDER BY c.opened_at DESC, c.cid DESC LIMIT " + (postgres ? "$2" : "?")));
        Throwable unavailable = databaseFailure(postgres, false);
        when(query.execute(any(Tuple.class))).thenReturn(Future.failedFuture(unavailable));
        assertSame(unavailable, failure(repository.load("cid")));
        assertSame(unavailable, failure(repository.findByIdentity("alice", 1)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void sessionHistoryCommitsWithSnapshotAndReadsLatestSlice(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class, CALLS_REAL_METHODS);
        SqlConnection connection = mock(SqlConnection.class);
        Transaction transaction = mock(Transaction.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.getConnection()).thenReturn(Future.succeededFuture(connection));
        when(connection.begin()).thenReturn(Future.succeededFuture(transaction));
        when(connection.close()).thenReturn(Future.succeededFuture());
        when(transaction.commit()).thenReturn(Future.succeededFuture());
        when(connection.preparedQuery(anyString())).thenReturn(query);
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(1)));
        when(query.executeBatch(anyList())).thenReturn(Future.succeededFuture(rows(1)));
        JsonObject session = session("s1", 100).put("status", "INVITING");
        SqlConversationRepository repository = new SqlConversationRepository(pool, postgres);
        await(repository.save(snapshot("alice"), List.of(session)));
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(connection, times(4)).preparedQuery(sql.capture());
        assertTrue(sql.getAllValues().get(3).contains("INSERT INTO uctp_sessions"));
        assertTrue(sql.getAllValues().get(3).contains(postgres ? "ON CONFLICT (cid, sid)" : "ON DUPLICATE KEY UPDATE"));
        ArgumentCaptor<Tuple> tuples = ArgumentCaptor.forClass(Tuple.class);
        verify(query, times(3)).execute(tuples.capture());
        Tuple persisted = tuples.getAllValues().get(2);
        assertEquals("cid", persisted.getString(0)); assertEquals("s1", persisted.getString(1));
        assertEquals(session, new JsonObject(persisted.getString(2)));
        verify(transaction).commit(); verify(connection).close();

        Pool historyPool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> historyQuery = mock(PreparedQuery.class);
        when(historyPool.preparedQuery(anyString())).thenReturn(historyQuery);
        Row row = mock(Row.class); when(row.getString("snapshot")).thenReturn(session.encode());
        when(historyQuery.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(1, row)));
        assertEquals(List.of(session), new SqlConversationRepository(historyPool, postgres).sessionHistory("cid", 1)
                .toCompletionStage().toCompletableFuture().join());
        ArgumentCaptor<String> historySql = ArgumentCaptor.forClass(String.class);
        verify(historyPool).preparedQuery(historySql.capture());
        assertTrue(historySql.getValue().contains("ORDER BY invited_at DESC, sid DESC LIMIT"));
        assertTrue(historySql.getValue().contains("ORDER BY invited_at ASC, sid ASC"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void sessionTimelineUsesStrictCursorAndStableAscendingOrder(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(query);
        JsonObject session = session("s1", 100);
        Row row = mock(Row.class);
        when(row.getLong("invited_at")).thenReturn(100L);
        when(row.getString("sid")).thenReturn("s1");
        when(row.getString("snapshot")).thenReturn(session.encode());
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(1, row)));
        List<TimelineEntry> result = await(new SqlConversationRepository(pool, postgres)
                .sessionTimeline("cid", 200L, "s2", "session", 3));
        assertEquals(List.of(new TimelineEntry("session", 100, "s1", session)), result);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool).preparedQuery(sql.capture());
        assertTrue(sql.getValue().contains("invited_at < "));
        assertTrue(sql.getValue().contains("sid < "));
        assertTrue(sql.getValue().contains(postgres ? "LIMIT $5" : "LIMIT ?"));
        assertTrue(sql.getValue().endsWith("ORDER BY invited_at ASC, sid ASC"));
        ArgumentCaptor<Tuple> args = ArgumentCaptor.forClass(Tuple.class);
        verify(query).execute(args.capture());
        assertEquals(200L, args.getValue().getLong(1));
        assertEquals("s2", args.getValue().getString(3));
        assertEquals(3, args.getValue().getInteger(4));
    }

    @Test
    void invalidSnapshotsAndLimitsNeverIssueSql() {
        Pool pool = mock(Pool.class);
        SqlConversationRepository repository = new SqlConversationRepository(pool, true);
        assertInstanceOf(IllegalArgumentException.class, failure(repository.save(snapshot(" "))));
        assertInstanceOf(IllegalArgumentException.class, failure(repository.load("")));
        assertInstanceOf(IllegalArgumentException.class, failure(repository.save(snapshot("alice"), List.of(session("", 100)))));
        assertInstanceOf(IllegalArgumentException.class, failure(repository.save(snapshot("alice"), List.of(session("s", 100).put("cid", "other")))));
        for (int limit : new int[]{0, 201, Integer.MAX_VALUE}) {
            assertInstanceOf(IllegalArgumentException.class, failure(repository.findByIdentity("alice", limit)));
            assertInstanceOf(IllegalArgumentException.class, failure(repository.sessionHistory("cid", limit)));
        }
        for (int limit : new int[]{0, 202, Integer.MAX_VALUE}) {
            assertInstanceOf(IllegalArgumentException.class, failure(repository.sessionTimeline("cid", null, null, null, limit)));
        }
        assertInstanceOf(IllegalArgumentException.class, failure(repository.sessionTimeline("cid", 100L, "s1", null, 1)));
        assertInstanceOf(IllegalArgumentException.class, failure(repository.sessionTimeline("cid", 100L, "s1", "unknown", 1)));
        verifyNoInteractions(pool);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void initializesRelationalTablesWithoutJsonFunctions(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        Query<RowSet<Row>> query = mock(Query.class);
        when(pool.query(anyString())).thenReturn(query);
        when(query.execute()).thenReturn(Future.succeededFuture(rows(0)));
        await(new SqlConversationRepository(pool, postgres).initialize());
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool, times(postgres ? 5 : 3)).query(sql.capture());
        assertTrue(sql.getAllValues().get(0).contains(postgres ? "snapshot TEXT" : "snapshot MEDIUMTEXT"));
        assertTrue(sql.getAllValues().get(1).contains("PRIMARY KEY (cid, identity_id)"));
        assertTrue(sql.getAllValues().get(postgres ? 3 : 2).contains("PRIMARY KEY (cid, sid)"));
        if (!postgres) {
            assertTrue(sql.getAllValues().get(1).contains("KEY idx_uctp_members_identity"));
            assertTrue(sql.getValue().contains("KEY idx_uctp_sessions_cid"));
        }
    }

    private static JsonObject session(String sid, long invitedAt) {
        return new JsonObject().put("cid", "cid").put("sid", sid).put("medium", "voice")
                .put("status", "INVITING").put("initiator_participant_id", "p-alice")
                .put("participant_ids", new JsonArray().add("p-alice")).put("invited_at", invitedAt);
    }

    private static JsonObject snapshot(String... identities) {
        JsonArray participants = new JsonArray();
        for (String identity : identities) {
            participants.add(new JsonObject().put("identity_id", identity).put("participant_id", "p-" + identity));
        }
        return new JsonObject().put("cid", "cid").put("tenant_id", "tenant").put("policy", "persistent")
                .put("idle_close_secs", 30).put("opened_at", 100L).put("closed", false).putNull("closed_at")
                .put("metadata", new JsonObject().put("nested", new JsonArray().add("value")))
                .put("participants", participants);
    }
}
