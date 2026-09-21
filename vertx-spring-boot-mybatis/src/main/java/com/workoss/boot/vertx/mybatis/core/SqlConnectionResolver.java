package com.workoss.boot.vertx.mybatis.core;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlClient;

/**
 * Strategy that decides which {@link SqlClient} an execution runs against.
 * <p>
 * Resolution must be <b>non-blocking</b>: implementations return an already-completed
 * {@link Future} when a transactional connection is bound, and never call {@code join()}.
 * This single abstraction unifies the three execution paths that previously each hard-coded their
 * own connection lookup (reactive transaction, blocking Spring transaction, plain pool).
 */
public interface SqlConnectionResolver {

    /**
     * Resolve the client to execute against.
     *
     * @param pool the fallback pool
     * @return a future of the resolved {@link SqlClient} (a transactional {@code SqlConnection} or the pool)
     */
    Future<SqlClient> resolve(Pool pool);
}
