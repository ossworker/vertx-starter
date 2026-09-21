package com.workoss.boot.vertx.mybatis.core;

import com.workoss.boot.vertx.mybatis.core.batch.VertxBatchBuffer;
import com.workoss.boot.vertx.mybatis.spring.VertxDataSourceTransactionManager;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BaseExecutor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * MyBatis {@link Executor} implementation backed by vertx-sql-client.
 * <p>
 * Extends {@link BaseExecutor} to inherit first-level cache, transaction tracking and the standard
 * query/update flow, and so can be wrapped by the MyBatis interceptor chain (PageHelper, etc.).
 * <p>
 * This class is now a <b>thin synchronous adapter</b>: every {@code doXxx} hook delegates to the
 * shared {@link VertxExecutionEngine} and blocks at {@link VertxExecutionEngine#block(Future)} — the
 * single, event-loop-guarded blocking boundary. The engine, not this class, owns SQL resolution,
 * dialect pagination, row mapping and connection/transaction selection.
 * <p>
 * Async ({@code Future}) execution no longer lives here; {@link VertxSqlSession} and the mapper proxy
 * call the engine directly, so the async path never blocks.
 */
public class VertxExecutor extends BaseExecutor {

    private final Pool pool;
    private final VertxTransaction vertxTransaction;
    private final VertxExecutionEngine engine;
    private final ExecutorType executorType;
    private final VertxBatchBuffer batchBuffer = new VertxBatchBuffer();

    public VertxExecutor(Configuration configuration, Transaction transaction, Pool pool,
                         VertxTransaction vertxTransaction, VertxExecutionEngine engine,
                         ExecutorType executorType) {
        super(configuration, transaction);
        this.pool = pool;
        this.vertxTransaction = vertxTransaction;
        this.engine = engine;
        this.executorType = executorType != null ? executorType : ExecutorType.SIMPLE;
    }

    /** The shared non-blocking engine this executor delegates to (used by the async session path). */
    public VertxExecutionEngine getEngine() {
        return engine;
    }

    /** The batch buffer accumulated by this executor ({@link ExecutorType#BATCH} sessions). */
    public VertxBatchBuffer getBatchBuffer() {
        return batchBuffer;
    }

    // ===== Synchronous BaseExecutor hooks (blocking boundary) =====

    @Override
    protected int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
        // Manual (non-Spring) transaction: make sure a tx is begun. Blocking is acceptable here —
        // this is the synchronous API, which must not run on an event loop (guarded by engine.block).
        if (VertxDataSourceTransactionManager.getCurrentConnection(pool) == null) {
            vertxTransaction.ensureTransaction();
        }
        VertxBatchBuffer buffer = executorType == ExecutorType.BATCH ? batchBuffer : null;
        try {
            Integer rows = engine.block(engine.update(ms, parameter, getSqlClient(), buffer));
            return rows != null ? rows : 0;
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {
            throw engine.unwrapSqlException(e);
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                  ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
        // Re-type the raw handler so the generic engine call is not erased to a raw invocation.
        ResultHandler<E> handler = resultHandler;
        try {
            return engine.block(engine.query(ms, parameter, rowBounds, handler, getSqlClient()));
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {
            throw engine.unwrapSqlException(e);
        }
    }

    @Override
    protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                          BoundSql boundSql) throws SQLException {
        try {
            return engine.block(engine.queryCursor(ms, parameter, rowBounds, getSqlClient()));
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {
            throw engine.unwrapSqlException(e);
        }
    }

    @Override
    protected List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
        if (isRollback) {
            batchBuffer.clear();
            return Collections.emptyList();
        }
        try {
            return engine.block(engine.flush(batchBuffer, getSqlClient()));
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {
            throw engine.unwrapSqlException(e);
        }
    }

    /**
     * Resolve the client for the synchronous path: blocking Spring transaction → this session's
     * manual {@link VertxTransaction} connection → pool.
     */
    private SqlClient getSqlClient() {
        SqlConnection springConn = VertxDataSourceTransactionManager.getCurrentConnection(pool);
        if (springConn != null) {
            return springConn;
        }
        SqlConnection conn = vertxTransaction.getSqlConnection();
        return conn != null ? conn : pool;
    }

    // ===== Deprecated async delegators (kept for source compatibility) =====

    /**
     * @deprecated the async path now lives in {@link VertxExecutionEngine}; use
     *             {@link VertxSqlSession#selectListAsync} or the engine directly.
     */
    @Deprecated
    public <E> Future<List<E>> doQueryAsync(MappedStatement ms, Object parameter, RowBounds rowBounds) {
        return engine.query(ms, parameter, rowBounds, null, vertxTransaction.getSqlConnection());
    }

    /**
     * @deprecated the async path now lives in {@link VertxExecutionEngine}; use
     *             {@link VertxSqlSession#updateAsync} or the engine directly.
     */
    @Deprecated
    public Future<Integer> doUpdateAsync(MappedStatement ms, Object parameter) {
        return engine.update(ms, parameter, vertxTransaction.getSqlConnection());
    }
}
