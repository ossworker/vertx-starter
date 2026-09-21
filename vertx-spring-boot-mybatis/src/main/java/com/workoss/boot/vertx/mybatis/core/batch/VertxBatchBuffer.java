package com.workoss.boot.vertx.mybatis.core.batch;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.mapping.MappedStatement;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Buffers update statements for MyBatis {@code ExecutorType.BATCH} semantics and flushes them with
 * vertx-sql-client's {@code executeBatch}.
 * <p>
 * An instance is owned by a single session/executor (not shared across threads). Statements are
 * grouped into <em>consecutive</em> runs of identical SQL — exactly like MyBatis's
 * {@code BatchExecutor} — so the resulting {@link BatchResult} order matches the call order.
 */
public final class VertxBatchBuffer {

    private record Entry(MappedStatement ms, String sql, Tuple tuple) {
    }

    private final List<Entry> entries = new ArrayList<>();

    public void add(MappedStatement ms, String sql, Tuple tuple) {
        entries.add(new Entry(ms, sql, tuple));
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public void clear() {
        entries.clear();
    }

    /**
     * Flush buffered statements as batches, then clear the buffer.
     *
     * @param client the (transactional) client to execute against
     * @return a future of one {@link BatchResult} per consecutive same-SQL group
     */
    public Future<List<BatchResult>> flush(SqlClient client) {
        if (entries.isEmpty()) {
            return Future.succeededFuture(Collections.emptyList());
        }
        List<Entry> snapshot = new ArrayList<>(entries);
        entries.clear();

        List<Future<BatchResult>> futures = new ArrayList<>();
        int i = 0;
        while (i < snapshot.size()) {
            Entry start = snapshot.get(i);
            List<Tuple> tuples = new ArrayList<>();
            int j = i;
            while (j < snapshot.size() && snapshot.get(j).sql().equals(start.sql())) {
                tuples.add(snapshot.get(j).tuple());
                j++;
            }
            MappedStatement ms = start.ms();
            String sql = start.sql();
            int count = tuples.size();
            futures.add(client.preparedQuery(sql).executeBatch(tuples)
                    .map(rowSet -> toBatchResult(ms, sql, rowSet, count)));
            i = j;
        }

        return Future.all(futures).map(cf -> {
            List<BatchResult> results = new ArrayList<>(futures.size());
            for (Future<BatchResult> f : futures) {
                results.add(f.result());
            }
            return results;
        });
    }

    private static BatchResult toBatchResult(MappedStatement ms, String sql,
                                             RowSet<Row> rowSet, int expected) {
        int[] counts = new int[expected];
        int idx = 0;
        RowSet<Row> current = rowSet;
        while (current != null && idx < expected) {
            counts[idx++] = current.rowCount();
            current = current.next();
        }
        // Statements whose individual count is unavailable are reported as SUCCESS_NO_INFO.
        while (idx < expected) {
            counts[idx++] = Statement.SUCCESS_NO_INFO;
        }
        BatchResult result = new BatchResult(ms, sql);
        result.setUpdateCounts(counts);
        return result;
    }
}
