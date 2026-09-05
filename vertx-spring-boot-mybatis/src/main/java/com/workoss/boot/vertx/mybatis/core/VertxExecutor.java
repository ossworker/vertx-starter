package com.workoss.boot.vertx.mybatis.core;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import com.workoss.boot.vertx.mybatis.spring.VertxDataSourceTransactionManager;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.executor.BaseExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MyBatis {@link Executor} implementation backed by vertx-sql-client.
 * <p>
 * Extends {@link BaseExecutor} to inherit:
 * <ul>
 *   <li>First-level cache (LocalCache) management</li>
 *   <li>Transaction isolation level tracking</li>
 *   <li>Standard query/update flow (cache lookup → delegate to doQuery/doUpdate → cache write)</li>
 * </ul>
 * <p>
 * The three abstract methods ({@code doUpdate}, {@code doQuery}, {@code doFlushStatements})
 * delegate SQL execution to vertx-sql-client instead of JDBC.
 * <p>
 * Plugin compatibility: Since BaseExecutor implements {@link Executor}, the
 * {@code InterceptorChain.pluginAll(executor)} wraps this executor via JDK proxy,
 * allowing all Executor-level interceptors (PageHelper, PerformanceInterceptor, etc.)
 * to work transparently.
 */
public class VertxExecutor extends BaseExecutor {

    private static final Logger logger = LoggerFactory.getLogger(VertxExecutor.class);

    private final Pool pool;
    private final VertxTransaction vertxTransaction;
    private final SqlResolver sqlResolver;
    private final VertxRowMapper rowMapper;

    public VertxExecutor(Configuration configuration, Transaction transaction,
                         Pool pool, VertxTransaction vertxTransaction,
                         SqlResolver sqlResolver, VertxRowMapper rowMapper) {
        super(configuration, transaction);
        this.pool = pool;
        this.vertxTransaction = vertxTransaction;
        this.sqlResolver = sqlResolver;
        this.rowMapper = rowMapper;
    }

    // ===== Synchronous methods (called by BaseExecutor through interceptor chain) =====

    @Override
    protected int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
        SqlAndParams sqlAndParams = sqlResolver.resolve(ms, parameter);
        SqlClient client = getSqlClient();

        // Ensure transaction is started for write operations (skip if Spring manages the tx)
        if (VertxDataSourceTransactionManager.getCurrentConnection(pool) == null) {
            vertxTransaction.ensureTransaction();
        }

        try {
            RowSet<Row> result = client.preparedQuery(sqlAndParams.sql())
                    .execute(sqlAndParams.tuple())
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
            return result.rowCount();
        } catch (Exception e) {
            throw unwrapSqlException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E> List<E> doQuery(MappedStatement ms, Object parameter,
                                  RowBounds rowBounds, ResultHandler resultHandler,
                                  BoundSql boundSql) throws SQLException {
        SqlAndParams sqlAndParams = sqlResolver.resolve(ms, parameter);
        String sql = applyRowBounds(sqlAndParams.sql(), rowBounds);
        SqlClient client = getSqlClient();

        try {
            RowSet<Row> rowSet = client.preparedQuery(sql)
                    .execute(sqlAndParams.tuple())
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            List<ResultMap> resultMaps = ms.getResultMaps();
            ResultMap resultMap = resultMaps.isEmpty() ? null : resultMaps.get(0);

            if (resultHandler != null) {
                rowMapper.mapResultsWithHandler(rowSet, resultHandler, resultMap);
                return Collections.emptyList();
            }

            return rowMapper.mapResults(rowSet, resultMap);
        } catch (Exception e) {
            throw unwrapSqlException(e);
        }
    }

    @Override
    protected List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
        // vertx-sql-client batch operations are handled separately via executeBatch
        return Collections.emptyList();
    }

    @Override
    protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter,
                                         RowBounds rowBounds, BoundSql boundSql) throws SQLException {
        SqlAndParams sqlAndParams = sqlResolver.resolve(ms, parameter);
        String sql = applyRowBounds(sqlAndParams.sql(), rowBounds);
        SqlClient client = getSqlClient();

        try {
            RowSet<Row> rowSet = client.preparedQuery(sql)
                    .execute(sqlAndParams.tuple())
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            List<ResultMap> resultMaps = ms.getResultMaps();
            ResultMap resultMap = resultMaps.isEmpty() ? null : resultMaps.get(0);
            List<E> results = rowMapper.mapResults(rowSet, resultMap);
            return new VertxCursor<>(results);
        } catch (Exception e) {
            throw unwrapSqlException(e);
        }
    }

    // ===== Async methods (bypass BaseExecutor cache and interceptor chain) =====

    /**
     * Async query — returns Future, does not use first-level cache.
     * Called directly by VertxSqlSession for Future-returning mapper methods.
     */
    @SuppressWarnings("unchecked")
    public <E> Future<List<E>> doQueryAsync(MappedStatement ms, Object parameter, RowBounds rowBounds) {
        try {
            SqlAndParams sqlAndParams = sqlResolver.resolve(ms, parameter);
            String sql = applyRowBounds(sqlAndParams.sql(), rowBounds);
            SqlClient client = getSqlClient();

            return client.preparedQuery(sql)
                    .execute(sqlAndParams.tuple())
                    .map(rowSet -> {
                        List<ResultMap> resultMaps = ms.getResultMaps();
                        ResultMap resultMap = resultMaps.isEmpty() ? null : resultMaps.get(0);
                        return rowMapper.<E>mapResults(rowSet, resultMap);
                    });
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * Async update — returns Future, bypasses first-level cache.
     */
    public Future<Integer> doUpdateAsync(MappedStatement ms, Object parameter) {
        try {
            SqlAndParams sqlAndParams = sqlResolver.resolve(ms, parameter);
            SqlClient client = getSqlClient();

            // Ensure transaction for write operations (skip if Spring manages the tx)
            if (VertxDataSourceTransactionManager.getCurrentConnection(pool) == null) {
                vertxTransaction.ensureTransaction();
            }

            return client.preparedQuery(sqlAndParams.sql())
                    .execute(sqlAndParams.tuple())
                    .map(RowSet::rowCount);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    // ===== Utility methods =====

    /**
     * Get the appropriate SqlClient: 
     * 1. Spring-managed transactional connection (via @Transactional)
     * 2. VertxTransaction's SqlConnection (manual transaction)
     * 3. Pool (auto-manages connections per query)
     */
    private SqlClient getSqlClient() {
        // Check Spring transaction context first
        SqlConnection springConn = VertxDataSourceTransactionManager.getCurrentConnection(pool);
        if (springConn != null) {
            return springConn;
        }
        // Then check VertxTransaction
        SqlConnection conn = vertxTransaction.getSqlConnection();
        return conn != null ? conn : pool;
    }

    /**
     * Apply RowBounds pagination by appending LIMIT/OFFSET to the SQL.
     * In production, users typically use interceptors (e.g. PageHelper) instead.
     */
    private String applyRowBounds(String sql, RowBounds rowBounds) {
        if (rowBounds == null || rowBounds == RowBounds.DEFAULT) {
            return sql;
        }

        StringBuilder sb = new StringBuilder(sql);
        boolean needsPaging = false;

        if (rowBounds.getOffset() > 0) {
            sb.append(" OFFSET ").append(rowBounds.getOffset());
            needsPaging = true;
        }
        if (rowBounds.getLimit() < RowBounds.NO_ROW_LIMIT) {
            // Insert LIMIT before OFFSET if offset was already appended
            if (needsPaging) {
                int offsetIdx = sb.indexOf(" OFFSET ");
                sb.insert(offsetIdx, " LIMIT " + rowBounds.getLimit());
            } else {
                sb.append(" LIMIT ").append(rowBounds.getLimit());
            }
        }

        return sb.toString();
    }

    /**
     * Unwrap exceptions from CompletableFuture.join() to produce clean SQLExceptions.
     */
    private SQLException unwrapSqlException(Exception e) {
        Throwable cause = e;
        if (e instanceof java.util.concurrent.CompletionException && e.getCause() != null) {
            cause = e.getCause();
        }
        if (cause instanceof SQLException se) {
            return se;
        }
        return new SQLException("SQL execution failed: " + cause.getMessage(), cause);
    }
}
