package com.workoss.boot.vertx.mybatis.core;

import com.workoss.boot.vertx.mybatis.core.batch.VertxBatchBuffer;
import com.workoss.boot.vertx.mybatis.dialect.SqlDialect;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BatchExecutor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * The single non-blocking execution core for vertx-mybatis. Every SQL execution — sync
 * {@code BaseExecutor} hooks, async {@code Future} mapper methods, and the cursor path — funnels
 * through here, eliminating the previously duplicated sync/async logic.
 * <p>
 * All execution methods return {@link io.vertx.core.Future} and never block. The <em>only</em>
 * blocking boundary is {@link #block(Future)}, used by the synchronous MyBatis API; it fails fast
 * with {@link IllegalStateException} when called on a Vert.x thread instead of silently deadlocking
 * the event loop.
 * <p>
 * Connection selection is delegated to {@link SqlConnectionResolver} (reactive tx → blocking Spring
 * tx → pool), so the same statement participates in the active transaction on either path. Callers
 * that already hold a connection (the manual-session sync path) may pass it explicitly.
 * <p>
 * Thread-safe and stateless per call; a single instance is shared.
 */
public final class VertxExecutionEngine {

    private final Pool pool;
    private final SqlResolver sqlResolver;
    private final VertxRowMapper rowMapper;
    private final SqlConnectionResolver connectionResolver;
    private final SqlDialect dialect;

    public VertxExecutionEngine(Pool pool, SqlResolver sqlResolver, VertxRowMapper rowMapper,
                                SqlConnectionResolver connectionResolver, SqlDialect dialect) {
        this.pool = pool;
        this.sqlResolver = sqlResolver;
        this.rowMapper = rowMapper;
        this.connectionResolver = connectionResolver != null
                ? connectionResolver : new DefaultSqlConnectionResolver();
        this.dialect = dialect;
    }

    public SqlDialect getDialect() {
        return dialect;
    }

    public SqlConnectionResolver getConnectionResolver() {
        return connectionResolver;
    }

    // ===== Query =====

    public <E> Future<List<E>> query(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                     ResultHandler<E> resultHandler) {
        return query(ms, parameter, rowBounds, resultHandler, (SqlClient) null);
    }

    public <E> Future<List<E>> query(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                     ResultHandler<E> resultHandler, SqlClient client) {
        final SqlAndParams sp;
        try {
            sp = sqlResolver.resolve(ms, parameter);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
        final String sql = dialect != null ? dialect.applyPagination(sp.sql(), rowBounds) : sp.sql();
        final ResultMap resultMap = firstResultMap(ms);
        return clientFuture(client)
                .compose(c -> c.preparedQuery(sql).execute(sp.tuple()))
                .map(rowSet -> {
                    if (resultHandler != null) {
                        rowMapper.mapResultsWithHandler(rowSet, resultHandler, resultMap);
                        return Collections.<E>emptyList();
                    }
                    return rowMapper.mapResults(rowSet, resultMap);
                });
    }

    public <T> Future<T> queryOne(MappedStatement ms, Object parameter, RowBounds rowBounds) {
        return this.<T>query(ms, parameter, rowBounds, null).map(list -> {
            if (list.size() > 1) {
                throw new TooManyResultsException(
                        "Expected one result (or null) to be returned by selectOne(), but found: "
                                + list.size());
            }
            return list.isEmpty() ? null : list.get(0);
        });
    }

    public <E> Future<Cursor<E>> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) {
        return queryCursor(ms, parameter, rowBounds, null);
    }

    public <E> Future<Cursor<E>> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                             SqlClient client) {
        return this.<E>query(ms, parameter, rowBounds, null, client).map(VertxCursor::new);
    }

    // ===== Update =====

    public Future<Integer> update(MappedStatement ms, Object parameter) {
        return update(ms, parameter, null, null);
    }

    public Future<Integer> update(MappedStatement ms, Object parameter, SqlClient client) {
        return update(ms, parameter, client, null);
    }

    public Future<Integer> update(MappedStatement ms, Object parameter, VertxBatchBuffer buffer) {
        return update(ms, parameter, null, buffer);
    }

    public Future<Integer> update(MappedStatement ms, Object parameter, SqlClient client,
                                  VertxBatchBuffer buffer) {
        final SqlAndParams sp;
        try {
            sp = sqlResolver.resolve(ms, parameter);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
        if (buffer != null) {
            buffer.add(ms, sp.sql(), sp.tuple());
            return Future.succeededFuture(BatchExecutor.BATCH_UPDATE_RETURN_VALUE);
        }
        return clientFuture(client)
                .compose(c -> c.preparedQuery(sp.sql()).execute(sp.tuple()))
                .map(RowSet::rowCount);
    }

    // ===== Batch flush =====

    public Future<List<BatchResult>> flush(VertxBatchBuffer buffer) {
        return flush(buffer, null);
    }

    public Future<List<BatchResult>> flush(VertxBatchBuffer buffer, SqlClient client) {
        if (buffer == null || buffer.isEmpty()) {
            return Future.succeededFuture(Collections.emptyList());
        }
        return clientFuture(client).compose(buffer::flush);
    }

    // ===== Internals =====

    private Future<SqlClient> clientFuture(SqlClient client) {
        return client != null ? Future.succeededFuture(client) : connectionResolver.resolve(pool);
    }

    private static ResultMap firstResultMap(MappedStatement ms) {
        List<ResultMap> resultMaps = ms.getResultMaps();
        return resultMaps == null || resultMaps.isEmpty() ? null : resultMaps.get(0);
    }

    /**
     * The single blocking boundary for the synchronous MyBatis API.
     *
     * @throws IllegalStateException if called on a Vert.x event-loop/worker thread (would deadlock)
     */
    public <T> T block(Future<T> future) {
        if (Context.isOnVertxThread()) {
            throw new IllegalStateException(
                    "Blocking MyBatis call executed on a Vert.x thread. Use the async (Future) "
                            + "mapper API, or run the blocking SqlSession API on a worker/pool thread.");
        }
        try {
            return future.toCompletionStage().toCompletableFuture().join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(cause);
        }
    }

    /** Unwrap an exception from the blocking boundary into a clean {@link SQLException}. */
    public SQLException unwrapSqlException(Throwable e) {
        Throwable cause = e;
        if (e instanceof CompletionException && e.getCause() != null) {
            cause = e.getCause();
        }
        if (cause instanceof SQLException se) {
            return se;
        }
        return new SQLException("SQL execution failed: "
                + (cause.getMessage() != null ? cause.getMessage() : cause.toString()), cause);
    }
}
