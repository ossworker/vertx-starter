package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.core.batch.VertxBatchBuffer;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.mapping.MappedStatement;
import org.junit.jupiter.api.Test;

import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link VertxBatchBuffer}: consecutive same-SQL grouping (MyBatis {@code BatchExecutor}
 * semantics), {@code executeBatch} fan-out, and {@link BatchResult} update-count mapping.
 */
class BatchBufferTest {

    @Test
    void groupsConsecutiveIdenticalSql() {
        SqlClient client = mock(SqlClient.class);
        when(client.preparedQuery(anyString())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            PreparedQuery<RowSet<Row>> pq = mock(PreparedQuery.class);
            when(pq.executeBatch(anyList())).thenAnswer(inv2 -> {
                List<Tuple> tuples = inv2.getArgument(0);
                return Future.succeededFuture(chainOf(tuples.size(), 1));
            });
            return pq;
        });

        MappedStatement ms = mock(MappedStatement.class);
        VertxBatchBuffer buffer = new VertxBatchBuffer();
        buffer.add(ms, "INSERT A", Tuple.of(1));
        buffer.add(ms, "INSERT A", Tuple.of(2));
        buffer.add(ms, "INSERT B", Tuple.of(3));
        buffer.add(ms, "INSERT B", Tuple.of(4));
        buffer.add(ms, "INSERT A", Tuple.of(5)); // non-consecutive -> its own group

        List<BatchResult> results = join(buffer.flush(client));

        assertEquals(3, results.size());
        assertEquals("INSERT A", results.get(0).getSql());
        assertEquals("INSERT B", results.get(1).getSql());
        assertEquals("INSERT A", results.get(2).getSql());
        // Group 0 = the two leading "INSERT A" tuples; group 2 = the trailing single "INSERT A".
        assertArrayEquals(new int[]{1, 1}, results.get(0).getUpdateCounts());
        assertArrayEquals(new int[]{1, 1}, results.get(1).getUpdateCounts());
        assertArrayEquals(new int[]{1}, results.get(2).getUpdateCounts());
        assertTrue(buffer.isEmpty(), "buffer must be cleared after flush");
    }

    @Test
    void updateCountsComeFromRowSetChain() {
        SqlClient client = mock(SqlClient.class);
        @SuppressWarnings("unchecked")
        PreparedQuery<RowSet<Row>> pq = mock(PreparedQuery.class);
        when(client.preparedQuery(anyString())).thenReturn(pq);
        // Build the chain first: calling chainOf() inside thenReturn() would start a nested stubbing
        // while the outer one is still open, which Mockito rejects as "unfinished stubbing".
        RowSet<Row> chain = chainOf(2, 7);
        when(pq.executeBatch(anyList())).thenReturn(Future.succeededFuture(chain));

        VertxBatchBuffer buffer = new VertxBatchBuffer();
        buffer.add(mock(MappedStatement.class), "UPDATE t SET x = ?", Tuple.of(1));
        buffer.add(mock(MappedStatement.class), "UPDATE t SET x = ?", Tuple.of(2));

        List<BatchResult> results = join(buffer.flush(client));
        assertEquals(1, results.size());
        assertArrayEquals(new int[]{7, 7}, results.get(0).getUpdateCounts());
    }

    @Test
    void missingIndividualCountsFallBackToSuccessNoInfo() {
        SqlClient client = mock(SqlClient.class);
        @SuppressWarnings("unchecked")
        PreparedQuery<RowSet<Row>> pq = mock(PreparedQuery.class);
        when(client.preparedQuery(anyString())).thenReturn(pq);
        // Only one RowSet in the chain, but three statements buffered.
        RowSet<Row> chain = chainOf(1, 3);
        when(pq.executeBatch(anyList())).thenReturn(Future.succeededFuture(chain));

        VertxBatchBuffer buffer = new VertxBatchBuffer();
        buffer.add(mock(MappedStatement.class), "SQL", Tuple.of(1));
        buffer.add(mock(MappedStatement.class), "SQL", Tuple.of(2));
        buffer.add(mock(MappedStatement.class), "SQL", Tuple.of(3));

        int[] counts = join(buffer.flush(client)).get(0).getUpdateCounts();
        assertEquals(3, counts.length);
        assertEquals(3, counts[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, counts[1]);
        assertEquals(Statement.SUCCESS_NO_INFO, counts[2]);
    }

    @Test
    void emptyBufferFlushesToEmptyListWithoutTouchingClient() {
        SqlClient client = mock(SqlClient.class);
        VertxBatchBuffer buffer = new VertxBatchBuffer();
        assertTrue(buffer.isEmpty());
        List<BatchResult> results = join(buffer.flush(client));
        assertTrue(results.isEmpty());
        verifyNoInteractions(client);
    }

    // ===== helpers =====

    private static <T> T join(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }

    /** Build a linked {@link RowSet} chain of {@code n} nodes, each reporting {@code rowCount}. */
    @SuppressWarnings("unchecked")
    private static RowSet<Row> chainOf(int n, int rowCount) {
        RowSet<Row> next = null;
        for (int k = n - 1; k >= 0; k--) {
            RowSet<Row> rs = mock(RowSet.class);
            when(rs.rowCount()).thenReturn(rowCount);
            when(rs.next()).thenReturn(next);
            next = rs;
        }
        return next;
    }
}
