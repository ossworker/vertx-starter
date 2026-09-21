package com.workoss.boot.vertx.mybatis.core;

import com.workoss.boot.vertx.mybatis.dialect.DialectResolver;
import io.vertx.sqlclient.Pool;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * MyBatis {@link SqlSessionFactory} implementation that creates
 * {@link VertxSqlSession} instances backed by vertx-sql-client.
 * <p>
 * Applies the MyBatis interceptor chain to the {@link VertxExecutor},
 * allowing all Executor-level plugins (PageHelper, PerformanceInterceptor, etc.)
 * to wrap the executor transparently.
 * <p>
 * All SQL execution funnels through the shared, thread-safe {@link VertxExecutionEngine};
 * the factory only builds per-session {@link VertxExecutor}/{@link VertxTransaction} objects
 * around it and honours the requested {@link ExecutorType}.
 */
public class VertxSqlSessionFactory implements SqlSessionFactory {

    private static final Logger logger = LoggerFactory.getLogger(VertxSqlSessionFactory.class);

    private final Configuration configuration;
    private final Pool pool;
    private final VertxExecutionEngine engine;

    /**
     * Preferred constructor: the engine (SQL resolution, dialect, row mapping, connection/transaction
     * selection) is built and shared by the auto-configuration.
     */
    public VertxSqlSessionFactory(Configuration configuration, Pool pool, VertxExecutionEngine engine) {
        this.configuration = configuration;
        this.pool = pool;
        this.engine = engine;
    }

    /**
     * Convenience constructor that builds a default engine from the given resolver/mapper,
     * auto-detecting the dialect from the pool.
     */
    public VertxSqlSessionFactory(Configuration configuration, Pool pool,
                                  SqlResolver sqlResolver, VertxRowMapper rowMapper) {
        this(configuration, pool, new VertxExecutionEngine(
                pool, sqlResolver, rowMapper,
                new DefaultSqlConnectionResolver(),
                DialectResolver.resolve(null, pool)));
    }

    @Override
    public SqlSession openSession() {
        return openSession(false);
    }

    @Override
    public SqlSession openSession(boolean autoCommit) {
        return openSession(ExecutorType.SIMPLE, autoCommit);
    }

    @Override
    public SqlSession openSession(TransactionIsolationLevel level) {
        return openSession(false);
    }

    @Override
    public SqlSession openSession(ExecutorType execType) {
        return openSession(execType, false);
    }

    @Override
    public SqlSession openSession(ExecutorType execType, TransactionIsolationLevel level) {
        return openSession(execType, false);
    }

    @Override
    public SqlSession openSession(ExecutorType execType, boolean autoCommit) {
        VertxTransaction vertxTransaction = new VertxTransaction(pool, autoCommit);

        // Create the original VertxExecutor (thin sync adapter over the shared engine)
        VertxExecutor vertxExecutor = new VertxExecutor(
                configuration, vertxTransaction, pool, vertxTransaction, engine, execType);

        // Apply interceptor chain (plugins)
        Executor wrappedExecutor = applyPlugins(vertxExecutor);

        return new VertxSqlSession(configuration, wrappedExecutor, vertxExecutor,
                vertxTransaction, autoCommit);
    }

    @Override
    public SqlSession openSession(Connection connection) {
        // JDBC connections not supported; ignore the parameter
        return openSession();
    }

    @Override
    public SqlSession openSession(ExecutorType execType, Connection connection) {
        return openSession(execType, false);
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    public Pool getPool() {
        return pool;
    }

    /** The shared non-blocking execution engine backing every session and mapper proxy. */
    public VertxExecutionEngine getEngine() {
        return engine;
    }

    /**
     * Apply all registered interceptors to the executor.
     */
    private Executor applyPlugins(Executor executor) {
        Executor result = executor;
        for (Interceptor interceptor : configuration.getInterceptors()) {
            result = (Executor) Plugin.wrap(result, interceptor);
        }
        return result;
    }
}
