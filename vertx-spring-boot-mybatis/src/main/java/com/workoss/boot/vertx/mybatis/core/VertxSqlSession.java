package com.workoss.boot.vertx.mybatis.core;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

/**
 * MyBatis {@link SqlSession} implementation backed by vertx-sql-client.
 * <p>
 * Provides both synchronous methods (standard SqlSession API, blocking) and
 * asynchronous methods (returning {@link Future}) for reactive use cases.
 * <p>
 * Sync methods go through the full Executor interceptor chain (including
 * plugins like PageHelper). Async methods bypass the chain and first-level
 * cache for maximum throughput.
 */
public class VertxSqlSession implements SqlSession {

    private static final Logger logger = LoggerFactory.getLogger(VertxSqlSession.class);

    private final Configuration configuration;
    /** Executor after interceptor chain wrapping (for sync methods) */
    private final Executor executor;
    /** Original VertxExecutor instance (owns the batch buffer and the shared engine) */
    private final VertxExecutor vertxExecutor;
    /** Shared non-blocking engine used directly by the async methods */
    private final VertxExecutionEngine engine;
    private final VertxTransaction vertxTransaction;
    private final boolean autoCommit;
    private boolean dirty = false;
    private boolean closed = false;

    public VertxSqlSession(Configuration configuration, Executor executor,
                           VertxExecutor vertxExecutor, VertxTransaction vertxTransaction,
                           boolean autoCommit) {
        this.configuration = configuration;
        this.executor = executor;
        this.vertxExecutor = vertxExecutor;
        this.engine = vertxExecutor != null ? vertxExecutor.getEngine() : null;
        this.vertxTransaction = vertxTransaction;
        this.autoCommit = autoCommit;
    }

    // ===== Synchronous SqlSession methods =====

    @Override
    public <T> T selectOne(String statement) {
        return selectOne(statement, null);
    }

    @Override
    public <T> T selectOne(String statement, Object parameter) {
        List<T> list = selectList(statement, parameter, RowBounds.DEFAULT);
        if (list.size() == 1) {
            return list.get(0);
        } else if (list.size() > 1) {
            throw new TooManyResultsException(
                    "Expected one result (or null) to be returned by selectOne(), but found: " + list.size());
        }
        return null;
    }

    @Override
    public <E> List<E> selectList(String statement) {
        return selectList(statement, null, RowBounds.DEFAULT);
    }

    @Override
    public <E> List<E> selectList(String statement, Object parameter) {
        return selectList(statement, parameter, RowBounds.DEFAULT);
    }

    @Override
    public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
        try {
            MappedStatement ms = configuration.getMappedStatement(statement);
            return executor.query(ms, wrapCollection(parameter), rowBounds, Executor.NO_RESULT_HANDLER);
        } catch (java.sql.SQLException e) {
            throw new PersistenceException("Error querying database. Cause: " + e, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void select(String statement, ResultHandler handler) {
        select(statement, null, RowBounds.DEFAULT, handler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void select(String statement, Object parameter, ResultHandler handler) {
        select(statement, parameter, RowBounds.DEFAULT, handler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler) {
        try {
            MappedStatement ms = configuration.getMappedStatement(statement);
            executor.query(ms, wrapCollection(parameter), rowBounds, handler);
        } catch (java.sql.SQLException e) {
            throw new PersistenceException("Error querying database. Cause: " + e, e);
        }
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
        return selectMap(statement, null, mapKey, RowBounds.DEFAULT);
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
        return selectMap(statement, parameter, mapKey, RowBounds.DEFAULT);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds) {
        List<? extends V> list = selectList(statement, parameter, rowBounds);
        Map<K, V> map = new LinkedHashMap<>();
        for (V value : list) {
            org.apache.ibatis.reflection.MetaObject metaObject = configuration.newMetaObject(value);
            K key = (K) metaObject.getValue(mapKey);
            map.put(key, value);
        }
        return map;
    }

    @Override
    public <T> Cursor<T> selectCursor(String statement) {
        return selectCursor(statement, null, RowBounds.DEFAULT);
    }

    @Override
    public <T> Cursor<T> selectCursor(String statement, Object parameter) {
        return selectCursor(statement, parameter, RowBounds.DEFAULT);
    }

    @Override
    public <T> Cursor<T> selectCursor(String statement, Object parameter, RowBounds rowBounds) {
        try {
            MappedStatement ms = configuration.getMappedStatement(statement);
            return executor.queryCursor(ms, wrapCollection(parameter), rowBounds);
        } catch (java.sql.SQLException e) {
            throw new PersistenceException("Error querying cursor. Cause: " + e, e);
        }
    }

    @Override
    public int insert(String statement) {
        return insert(statement, null);
    }

    @Override
    public int insert(String statement, Object parameter) {
        return update(statement, parameter);
    }

    @Override
    public int update(String statement) {
        return update(statement, null);
    }

    @Override
    public int update(String statement, Object parameter) {
        dirty = true;
        try {
            MappedStatement ms = configuration.getMappedStatement(statement);
            return executor.update(ms, wrapCollection(parameter));
        } catch (java.sql.SQLException e) {
            throw new PersistenceException("Error updating database. Cause: " + e, e);
        }
    }

    @Override
    public int delete(String statement) {
        return update(statement, null);
    }

    @Override
    public int delete(String statement, Object parameter) {
        return update(statement, parameter);
    }

    @Override
    public List<BatchResult> flushStatements() {
        try {
            return executor.flushStatements();
        } catch (java.sql.SQLException e) {
            throw new PersistenceException("Error flushing statements. Cause: " + e, e);
        }
    }

    // ===== Async methods (non-blocking, funnel through the shared engine) =====
    //
    // These bypass the interceptor chain and first-level cache for maximum throughput. Connection
    // selection is delegated to the engine's SqlConnectionResolver (reactive tx -> blocking Spring
    // tx -> pool); when this session was opened manually and already holds a connection, that
    // connection is passed explicitly so the async call participates in the same transaction.

    public <T> Future<T> selectOneAsync(String statement) {
        return selectOneAsync(statement, null);
    }

    public <T> Future<T> selectOneAsync(String statement, Object parameter) {
        return this.<T>selectListAsync(statement, parameter, RowBounds.DEFAULT)
                .map(list -> {
                    if (list.size() > 1) {
                        throw new TooManyResultsException(
                                "Expected one result (or null) to be returned by selectOneAsync(), but found: "
                                        + list.size());
                    }
                    return list.isEmpty() ? null : list.get(0);
                });
    }

    public <E> Future<List<E>> selectListAsync(String statement) {
        return selectListAsync(statement, null, RowBounds.DEFAULT);
    }

    public <E> Future<List<E>> selectListAsync(String statement, Object parameter) {
        return selectListAsync(statement, parameter, RowBounds.DEFAULT);
    }

    public <E> Future<List<E>> selectListAsync(String statement, Object parameter, RowBounds rowBounds) {
        MappedStatement ms = configuration.getMappedStatement(statement);
        return engine.query(ms, wrapCollection(parameter), rowBounds, null, sessionClient());
    }

    public Future<Integer> insertAsync(String statement) {
        return insertAsync(statement, null);
    }

    public Future<Integer> insertAsync(String statement, Object parameter) {
        return updateAsync(statement, parameter);
    }

    public Future<Integer> updateAsync(String statement) {
        return updateAsync(statement, null);
    }

    public Future<Integer> updateAsync(String statement, Object parameter) {
        dirty = true;
        MappedStatement ms = configuration.getMappedStatement(statement);
        return engine.update(ms, wrapCollection(parameter), sessionClient());
    }

    public Future<Integer> deleteAsync(String statement) {
        return updateAsync(statement, null);
    }

    public Future<Integer> deleteAsync(String statement, Object parameter) {
        return updateAsync(statement, parameter);
    }

    /**
     * Flush any statements buffered by this session's {@link org.apache.ibatis.session.ExecutorType#BATCH}
     * executor, asynchronously. Returns an empty list when nothing is buffered.
     */
    public Future<List<BatchResult>> flushAsync() {
        return engine.flush(vertxExecutor.getBatchBuffer(), sessionClient());
    }

    /**
     * The connection this manually-opened session already holds, or {@code null} to let the engine
     * resolve one (reactive transaction, blocking Spring transaction, or the pool).
     */
    private SqlClient sessionClient() {
        return vertxTransaction != null ? vertxTransaction.getSqlConnection() : null;
    }

    // ===== Transaction management =====

    @Override
    public void commit() {
        commit(false);
    }

    @Override
    public void commit(boolean force) {
        try {
            if (force || dirty) {
                executor.commit(true);
                dirty = false;
            }
        } catch (Exception e) {
            throw new ExecutorException("Error committing transaction. Cause: " + e, e);
        }
    }

    @Override
    public void rollback() {
        rollback(false);
    }

    @Override
    public void rollback(boolean force) {
        try {
            if (force || dirty) {
                executor.rollback(true);
                dirty = false;
            }
        } catch (Exception e) {
            throw new ExecutorException("Error rolling back transaction. Cause: " + e, e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            executor.close(isCommitOrRollbackRequired());
        } catch (Exception e) {
            logger.warn("Error closing executor", e);
        }
    }

    @Override
    public void clearCache() {
        executor.clearLocalCache();
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public <T> T getMapper(Class<T> type) {
        return configuration.getMapper(type, this);
    }

    @Override
    public Connection getConnection() {
        // JDBC Connection is not used; return null
        return null;
    }

    private boolean isCommitOrRollbackRequired() {
        return !autoCommit && dirty;
    }

    /**
     * Wrap collection/array parameters in a Map (same as MyBatis DefaultSqlSession).
     */
    private Object wrapCollection(Object object) {
        if (object instanceof Collection<?>) {
            Map<String, Object> map = new HashMap<>(2);
            map.put("collection", object);
            if (object instanceof List<?>) {
                map.put("list", object);
            }
            return map;
        } else if (object != null && object.getClass().isArray()) {
            Map<String, Object> map = new HashMap<>(2);
            map.put("array", object);
            return map;
        }
        return object;
    }
}
