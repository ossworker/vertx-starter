package com.workoss.boot.vertx.mybatis.core;

import io.vertx.core.Future;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.session.*;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Thread-safe {@link SqlSession} manager that maintains a ThreadLocal SqlSession.
 * <p>
 * Similar to MyBatis's SqlSessionManager, but adapted for vertx-sql-client.
 * When a session is already open on the current thread (e.g., from a Spring transaction),
 * it delegates to that session. Otherwise, it auto-opens and auto-closes sessions per call.
 * <p>
 * This class also provides async method variants that return {@link Future}.
 */
public class VertxSqlSessionManager implements SqlSession {

    private final SqlSessionFactory sqlSessionFactory;
    private final ThreadLocal<SqlSession> localSqlSession = new ThreadLocal<>();

    public VertxSqlSessionManager(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    /**
     * Start a managed session (for manual transaction control).
     * The session will be reused for all subsequent calls on this thread
     * until {@link #close()} is called.
     */
    public void startManagedSession() {
        localSqlSession.set(sqlSessionFactory.openSession());
    }

    public void startManagedSession(boolean autoCommit) {
        localSqlSession.set(sqlSessionFactory.openSession(autoCommit));
    }

    /**
     * Get or create a session for the current thread.
     */
    private SqlSession getOrCreateSession() {
        SqlSession session = localSqlSession.get();
        if (session != null) {
            return session;
        }
        return sqlSessionFactory.openSession(true);
    }

    private boolean isManagedSession() {
        return localSqlSession.get() != null;
    }

    private void closeIfNotManaged(SqlSession session) {
        if (!isManagedSession()) {
            session.close();
        }
    }

    // ===== Async methods =====

    public <T> Future<T> selectOneAsync(String statement) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<T> future = vs.selectOneAsync(statement);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        // Fallback for non-VertxSqlSession
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    public <T> Future<T> selectOneAsync(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<T> future = vs.selectOneAsync(statement, parameter);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    public <E> Future<List<E>> selectListAsync(String statement) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<List<E>> future = vs.selectListAsync(statement);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    public <E> Future<List<E>> selectListAsync(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<List<E>> future = vs.selectListAsync(statement, parameter);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    public Future<Integer> insertAsync(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<Integer> future = vs.insertAsync(statement, parameter);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    public Future<Integer> updateAsync(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<Integer> future = vs.updateAsync(statement, parameter);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    public Future<Integer> deleteAsync(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        if (session instanceof VertxSqlSession vs) {
            Future<Integer> future = vs.deleteAsync(statement, parameter);
            if (!isManagedSession()) {
                future = future.onComplete(r -> session.close());
            }
            return future;
        }
        return Future.failedFuture("Async operations require VertxSqlSession");
    }

    // ===== SqlSession delegation =====

    @Override
    public <T> T selectOne(String statement) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectOne(statement);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <T> T selectOne(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectOne(statement, parameter);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <E> List<E> selectList(String statement) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectList(statement);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <E> List<E> selectList(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectList(statement, parameter);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectList(statement, parameter, rowBounds);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public void select(String statement, ResultHandler handler) {
        SqlSession session = getOrCreateSession();
        try {
            session.select(statement, handler);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public void select(String statement, Object parameter, ResultHandler handler) {
        SqlSession session = getOrCreateSession();
        try {
            session.select(statement, parameter, handler);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler) {
        SqlSession session = getOrCreateSession();
        try {
            session.select(statement, parameter, rowBounds, handler);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectMap(statement, mapKey);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectMap(statement, parameter, mapKey);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds) {
        SqlSession session = getOrCreateSession();
        try {
            return session.selectMap(statement, parameter, mapKey, rowBounds);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public <T> Cursor<T> selectCursor(String statement) {
        throw new UnsupportedOperationException("Cursor is not supported");
    }

    @Override
    public <T> Cursor<T> selectCursor(String statement, Object parameter) {
        throw new UnsupportedOperationException("Cursor is not supported");
    }

    @Override
    public <T> Cursor<T> selectCursor(String statement, Object parameter, RowBounds rowBounds) {
        throw new UnsupportedOperationException("Cursor is not supported");
    }

    @Override
    public int insert(String statement) {
        SqlSession session = getOrCreateSession();
        try {
            return session.insert(statement);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public int insert(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        try {
            return session.insert(statement, parameter);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public int update(String statement) {
        SqlSession session = getOrCreateSession();
        try {
            return session.update(statement);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public int update(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        try {
            return session.update(statement, parameter);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public int delete(String statement) {
        SqlSession session = getOrCreateSession();
        try {
            return session.delete(statement);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public int delete(String statement, Object parameter) {
        SqlSession session = getOrCreateSession();
        try {
            return session.delete(statement, parameter);
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public List<BatchResult> flushStatements() {
        SqlSession session = getOrCreateSession();
        try {
            return session.flushStatements();
        } finally {
            closeIfNotManaged(session);
        }
    }

    @Override
    public void commit() {
        SqlSession session = localSqlSession.get();
        if (session != null) {
            session.commit();
        }
    }

    @Override
    public void commit(boolean force) {
        SqlSession session = localSqlSession.get();
        if (session != null) {
            session.commit(force);
        }
    }

    @Override
    public void rollback() {
        SqlSession session = localSqlSession.get();
        if (session != null) {
            session.rollback();
        }
    }

    @Override
    public void rollback(boolean force) {
        SqlSession session = localSqlSession.get();
        if (session != null) {
            session.rollback(force);
        }
    }

    @Override
    public void close() {
        SqlSession session = localSqlSession.get();
        localSqlSession.remove();
        if (session != null) {
            session.close();
        }
    }

    @Override
    public void clearCache() {
        SqlSession session = localSqlSession.get();
        if (session != null) {
            session.clearCache();
        }
    }

    @Override
    public Configuration getConfiguration() {
        return sqlSessionFactory.getConfiguration();
    }

    @Override
    public <T> T getMapper(Class<T> type) {
        return getOrCreateSession().getMapper(type);
    }

    @Override
    public Connection getConnection() {
        return null;
    }
}
