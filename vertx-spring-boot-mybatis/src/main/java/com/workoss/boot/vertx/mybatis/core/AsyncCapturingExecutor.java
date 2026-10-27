package com.workoss.boot.vertx.mybatis.core;

import io.vertx.core.Future;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import java.util.Collections;
import java.util.List;

/**
 * An {@link Executor} that lets MyBatis's synchronous {@link Interceptor} chain participate in the
 * non-blocking {@code Future} mapper path without blocking.
 * <p>
 * It is wrapped by the interceptor chain ({@link Plugin#wrap}) exactly like the synchronous
 * {@link VertxExecutor}. When an interceptor eventually calls {@code Invocation.proceed()}, this
 * executor does <em>not</em> run JDBC and return a result; instead it triggers the shared
 * {@link VertxExecutionEngine}, stashes the resulting {@link Future} in {@link #getCaptured()}, and
 * returns an empty sentinel synchronously. The caller ({@code VertxMapperProxy}) then returns that
 * captured {@code Future} to the application.
 * <p>
 * The value of this indirection: interceptors that <em>rewrite</em> the statement before proceeding —
 * the common SQL-rewriting pattern of swapping {@code args[0]} for a new {@link MappedStatement}, or
 * adjusting {@link RowBounds} — are honoured, because the engine reads the (possibly modified)
 * arguments the chain passes down. The whole {@code proceed()} call stack is synchronous (the engine
 * returns a pending {@code Future} immediately, never blocking), so a single instance safely carries
 * the captured {@code Future} for one mapper invocation; a fresh instance is used per call.
 * <p>
 * <b>Not supported on this path:</b> interceptors that post-process or block on the returned
 * {@code List} (e.g. PageHelper building a {@code Page} from a synchronous count query) see only the
 * empty sentinel. Such result-wrapping/paging behaviour remains available on the synchronous mapper
 * API; for async paging use {@link com.workoss.boot.vertx.mybatis.dialect.SqlDialect#applyPagination}
 * with {@link RowBounds}. If an interceptor short-circuits (returns without {@code proceed()}), its
 * return value is delivered as an already-succeeded {@code Future}.
 */
public final class AsyncCapturingExecutor implements Executor {

    private final VertxExecutionEngine engine;

    /** The engine {@link Future} captured when the chain reached {@code query}/{@code update}. */
    private Future<?> captured;

    public AsyncCapturingExecutor(VertxExecutionEngine engine) {
        this.engine = engine;
    }

    /**
     * Wrap {@code capturing} with the given interceptor chain, mirroring
     * {@code VertxSqlSessionFactory#applyPlugins} for the synchronous executor.
     */
    public static Executor chain(AsyncCapturingExecutor capturing, List<Interceptor> interceptors) {
        Executor result = capturing;
        if (interceptors != null) {
            for (Interceptor interceptor : interceptors) {
                result = (Executor) Plugin.wrap(result, interceptor);
            }
        }
        return result;
    }

    /** The {@link Future} produced by the engine, or {@code null} if the chain never proceeded here. */
    public Future<?> getCaptured() {
        return captured;
    }

    // ===== Capturing execution methods =====

    @Override
    public int update(MappedStatement ms, Object parameter) {
        captured = engine.update(ms, parameter);
        return 0;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds,
                             ResultHandler resultHandler) {
        captured = engine.query(ms, parameter, rowBounds, resultHandler);
        return Collections.emptyList();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds,
                             ResultHandler resultHandler, CacheKey cacheKey, BoundSql boundSql) {
        // The engine re-resolves SQL from the (possibly rewritten) MappedStatement + parameter, so a
        // chain that swapped args[0] is honoured. A BoundSql-only rewrite is not carried across.
        captured = engine.query(ms, parameter, rowBounds, resultHandler);
        return Collections.emptyList();
    }

    @Override
    public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) {
        captured = engine.queryCursor(ms, parameter, rowBounds);
        return null;
    }

    // ===== Session-lifecycle methods: inert on the async path =====

    @Override
    public List<BatchResult> flushStatements() {
        return Collections.emptyList();
    }

    @Override
    public void commit(boolean required) {
    }

    @Override
    public void rollback(boolean required) {
    }

    @Override
    public CacheKey createCacheKey(MappedStatement ms, Object parameter, RowBounds rowBounds,
                                   BoundSql boundSql) {
        return CacheKey.NULL_CACHE_KEY;
    }

    @Override
    public boolean isCached(MappedStatement ms, CacheKey key) {
        return false;
    }

    @Override
    public void clearLocalCache() {
    }

    @Override
    public void deferLoad(MappedStatement ms, MetaObject resultObject, String property,
                          CacheKey key, Class<?> targetType) {
    }

    @Override
    public Transaction getTransaction() {
        return null;
    }

    @Override
    public void close(boolean forceRollback) {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setExecutorWrapper(Executor executor) {
    }
}
