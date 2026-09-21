package com.workoss.boot.vertx.mybatis.core;

import com.workoss.boot.vertx.mybatis.reactive.VertxContextTransactionBridge;
import com.workoss.boot.vertx.mybatis.spring.VertxDataSourceTransactionManager;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;

/**
 * Default {@link SqlConnectionResolver}, checking transaction sources in order and never blocking:
 * <ol>
 *   <li><b>Reactive</b> — the {@link VertxConnectionHolder} bound to the current Vert.x context by
 *       {@code VertxReactiveTransactionManager} (works for {@code Future} mapper methods on the
 *       event loop).</li>
 *   <li><b>Blocking Spring</b> — the {@link SqlConnection} bound to the current thread by
 *       {@link VertxDataSourceTransactionManager} ({@code @Transactional} on blocking code).</li>
 *   <li><b>Pool fallback</b> — the {@link Pool} itself, which acquires/releases a connection per
 *       execution and runs in auto-commit.</li>
 * </ol>
 * Returning the pool (rather than a checked-out connection) for the non-transactional case keeps the
 * hot path allocation-free and lets vertx-sql-client manage the connection lifecycle.
 */
public class DefaultSqlConnectionResolver implements SqlConnectionResolver {

    @Override
    public Future<SqlClient> resolve(Pool pool) {
        SqlConnection reactiveConn = VertxContextTransactionBridge.currentConnection();
        if (reactiveConn != null) {
            return Future.succeededFuture(reactiveConn);
        }
        SqlConnection springConn = VertxDataSourceTransactionManager.getCurrentConnection(pool);
        if (springConn != null) {
            return Future.succeededFuture(springConn);
        }
        return Future.succeededFuture(pool);
    }
}
