package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static com.workoss.boot.vertx.contactcenter.journal.SqlPoolTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SqlJournalRepositoryTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void insertIsParameterizedAndWaitsForCommitAcknowledgment(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> insert = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(insert);
        Promise<RowSet<Row>> committed = Promise.promise();
        when(insert.execute(any(Tuple.class))).thenReturn(committed.future());
        JsonObject envelope = envelope();
        Future<Boolean> result = new SqlJournalRepository(pool, postgres).append("c' OR '1'='1", envelope);
        assertFalse(result.isComplete());
        envelope.getJsonObject("payload").put("body", "caller changed it");
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool).preparedQuery(sql.capture());
        assertFalse(sql.getValue().contains("OR '1'"));
        assertTrue(sql.getValue().contains(postgres ? "$8" : "?, ?, ?, ?, ?, ?, ?, ?"));
        ArgumentCaptor<Tuple> args = ArgumentCaptor.forClass(Tuple.class);
        verify(insert).execute(args.capture());
        assertEquals("original", new JsonObject(args.getValue().getString(6)).getJsonObject("payload").getString("body"));
        assertEquals("c' OR '1'='1", args.getValue().getString(1));
        committed.complete(rows(1));
        assertTrue(await(result));
        verify(pool, never()).getConnection();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void duplicateRequiresTheSameCidAndPayloadButNotTransportEnvelope(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(query);
        Row stored = stored("cid", envelope());
        when(query.execute(any(Tuple.class))).thenReturn(
                Future.failedFuture(databaseFailure(postgres, true)), Future.succeededFuture(rows(0, stored)));
        JsonObject retry = envelope().put("id", "new-transport-id").put("ts", 123L);
        assertFalse(await(new SqlJournalRepository(pool, postgres).append("cid", retry)));

        for (JsonObject conflicting : List.of(envelope().put("payload", envelope().getJsonObject("payload").put("body", "changed")),
                envelope().put("payload", envelope().getJsonObject("payload").put("from", "other")))) {
            when(query.execute(any(Tuple.class))).thenReturn(
                    Future.failedFuture(databaseFailure(postgres, true)), Future.succeededFuture(rows(0, stored)));
            assertInstanceOf(IllegalStateException.class, failure(new SqlJournalRepository(pool, postgres).append("cid", conflicting)));
        }
        when(query.execute(any(Tuple.class))).thenReturn(
                Future.failedFuture(databaseFailure(postgres, true)), Future.succeededFuture(rows(0, stored)));
        assertInstanceOf(IllegalStateException.class, failure(new SqlJournalRepository(pool, postgres).append("other-cid", envelope())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void databaseErrorsAndDuplicateVerificationErrorsPropagate(boolean postgres) {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(query);
        Throwable unavailable = databaseFailure(postgres, false);
        when(query.execute(any(Tuple.class))).thenReturn(Future.failedFuture(unavailable));
        SqlJournalRepository repository = new SqlJournalRepository(pool, postgres);
        assertSame(unavailable, failure(repository.append("cid", envelope())));
        verify(pool, times(1)).preparedQuery(anyString());
        when(query.execute(any(Tuple.class))).thenReturn(Future.failedFuture(databaseFailure(postgres, true)), Future.failedFuture(unavailable));
        assertSame(unavailable, failure(repository.append("cid", envelope())));
        when(query.execute(any(Tuple.class))).thenReturn(Future.failedFuture(databaseFailure(postgres, true)), Future.succeededFuture(rows(0)));
        assertInstanceOf(IllegalStateException.class, failure(repository.append("cid", envelope())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void historyUsesLatestSliceAndStableAscendingOrder(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(query);
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(0, stored("cid", envelope()))));
        assertEquals(List.of(envelope()), await(new SqlJournalRepository(pool, postgres).history("cid", 2)));
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool).preparedQuery(sql.capture());
        assertTrue(sql.getValue().contains("ORDER BY created_at DESC, msg_id DESC LIMIT " + (postgres ? "$2" : "?")));
        assertTrue(sql.getValue().endsWith("ORDER BY created_at ASC, msg_id ASC"));
        ArgumentCaptor<Tuple> args = ArgumentCaptor.forClass(Tuple.class);
        verify(query).execute(args.capture());
        assertEquals(2, args.getValue().getInteger(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void timelineUsesThreePartCursorAndReturnsStoredEnvelope(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
        when(pool.preparedQuery(anyString())).thenReturn(query);
        Row row = mock(Row.class);
        when(row.getLong("created_at")).thenReturn(100L);
        when(row.getString("msg_id")).thenReturn("message-id");
        when(row.getString("envelope")).thenReturn(envelope().encode());
        when(query.execute(any(Tuple.class))).thenReturn(Future.succeededFuture(rows(1, row)));
        List<TimelineEntry> result = await(new SqlJournalRepository(pool, postgres)
                .timeline("cid", 200L, "message-id", "session", 3));
        assertEquals(1, result.size());
        assertEquals(new TimelineEntry("message", 100, "message-id", envelope()), result.getFirst());
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool).preparedQuery(sql.capture());
        assertTrue(sql.getValue().contains("msg_id = "));
        assertTrue(sql.getValue().contains("AND 'message' < "));
        assertTrue(sql.getValue().contains(postgres ? "LIMIT $7" : "LIMIT ?"));
        assertTrue(sql.getValue().endsWith("ORDER BY created_at ASC, msg_id ASC"));
        ArgumentCaptor<Tuple> args = ArgumentCaptor.forClass(Tuple.class);
        verify(query).execute(args.capture());
        assertEquals(200L, args.getValue().getLong(1));
        assertEquals("message-id", args.getValue().getString(4));
        assertEquals("session", args.getValue().getString(5));
        assertEquals(3, args.getValue().getInteger(6));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void initializeUsesCompatibleIndexSyntaxAndReportsFailure(boolean postgres) throws Exception {
        Pool pool = mock(Pool.class);
        Query<RowSet<Row>> query = mock(Query.class);
        when(pool.query(anyString())).thenReturn(query);
        when(query.execute()).thenReturn(Future.succeededFuture(rows(0)));
        SqlJournalRepository repository = new SqlJournalRepository(pool, postgres);
        await(repository.initialize());
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(pool, times(postgres ? 2 : 1)).query(sql.capture());
        assertTrue(sql.getAllValues().getFirst().contains("msg_id VARCHAR(64)"));
        assertTrue(sql.getAllValues().getFirst().contains(postgres ? "envelope TEXT" : "envelope MEDIUMTEXT"));
        if (!postgres) {
            assertTrue(sql.getValue().contains("KEY idx_uctp_journal_cid"));
            assertFalse(sql.getValue().contains("CREATE INDEX IF NOT EXISTS"));
        }
        Throwable failure = databaseFailure(postgres, false);
        when(query.execute()).thenReturn(Future.failedFuture(failure));
        assertSame(failure, failure(repository.initialize()));
    }

    @Test
    void invalidArgumentsDoNotReachThePoolAndDuplicateClassificationIsNarrow() {
        Pool pool = mock(Pool.class);
        SqlJournalRepository repository = new SqlJournalRepository(pool, true);
        assertInstanceOf(IllegalArgumentException.class, failure(repository.append(" ", envelope())));
        assertInstanceOf(IllegalArgumentException.class, failure(repository.append("cid", new JsonObject().put("payload", new JsonObject()))));
        for (int limit : new int[]{0, -1, 201, Integer.MAX_VALUE}) {
            assertInstanceOf(IllegalArgumentException.class, failure(repository.history("cid", limit)));
        }
        for (int limit : new int[]{0, -1, 202, Integer.MAX_VALUE}) {
            assertInstanceOf(IllegalArgumentException.class, failure(repository.timeline("cid", null, null, null, limit)));
        }
        assertInstanceOf(IllegalArgumentException.class, failure(repository.timeline("cid", 100L, "id", null, 1)));
        assertInstanceOf(IllegalArgumentException.class, failure(repository.timeline("cid", 100L, "id", "unknown", 1)));
        verifyNoInteractions(pool);
        assertFalse(new SqlDialect(false).isDuplicate(databaseFailure(true, true)));
        assertFalse(new SqlDialect(true).isDuplicate(databaseFailure(false, true)));
        assertFalse(new SqlDialect(true).isDuplicate(new RuntimeException("23505")));
    }

    private static JsonObject envelope() {
        return new JsonObject().put("id", "transport-id").put("type", "message.send")
                .put("payload", new JsonObject().put("msg_id", "message-id").put("from", "participant")
                        .put("content_type", "text/plain").put("body", "original"));
    }

    private static Row stored(String cid, JsonObject envelope) {
        return mock(Row.class, invocation -> "getString".equals(invocation.getMethod().getName())
                ? switch ((String) invocation.getArgument(0)) {
                    case "cid" -> cid;
                    case "msg_type" -> "message.send";
                    case "envelope" -> envelope.encode();
                    default -> null;
                } : org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation));
    }
}
