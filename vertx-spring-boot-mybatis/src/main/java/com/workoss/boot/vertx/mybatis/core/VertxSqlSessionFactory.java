package com.workoss.boot.vertx.mybatis.core;

import io.vertx.sqlclient.Pool;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.Environment;
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
 */
public class VertxSqlSessionFactory implements SqlSessionFactory {

    private static final Logger logger = LoggerFactory.getLogger(VertxSqlSessionFactory.class);

    private final Configuration configuration;
    private final Pool pool;
    private final SqlResolver sqlResolver;
    private final VertxRowMapper rowMapper;

    public VertxSqlSessionFactory(Configuration configuration, Pool pool,
                                  SqlResolver sqlResolver, VertxRowMapper rowMapper) {
        this.configuration = configuration;
        this.pool = pool;
        this.sqlResolver = sqlResolver;
        this.rowMapper = rowMapper;
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

        // Create the original VertxExecutor
        VertxExecutor vertxExecutor = new VertxExecutor(
                configuration, vertxTransaction, pool, vertxTransaction,
                sqlResolver, rowMapper);

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
