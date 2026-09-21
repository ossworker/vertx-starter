package com.workoss.boot.vertx.mybatis.reactive;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;

/**
 * Holds the {@link SqlConnection} and vertx {@link Transaction} belonging to one reactive
 * transaction boundary, plus its rollback-only flag.
 * <p>
 * This is the resource bound both into the Reactor Context (authoritative, survives thread hops)
 * and into the Vert.x duplicated context (so raw {@link Future}-returning mapper methods can find
 * the same connection). All operations return {@link Future} — never block.
 */
public final class VertxConnectionHolder {

    private final SqlConnection connection;
    private final Transaction transaction;
    private volatile boolean rollbackOnly;

    public VertxConnectionHolder(SqlConnection connection, Transaction transaction) {
        this.connection = connection;
        this.transaction = transaction;
    }

    public SqlConnection getConnection() {
        return connection;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    public void setRollbackOnly() {
        this.rollbackOnly = true;
    }

    /** Commit the underlying vertx transaction; no-op when there is none (auto-commit). */
    public Future<Void> commit() {
        return transaction != null ? transaction.commit() : Future.succeededFuture();
    }

    /** Roll back the underlying vertx transaction; no-op when there is none. */
    public Future<Void> rollback() {
        return transaction != null ? transaction.rollback() : Future.succeededFuture();
    }

    /** Release the connection back to the pool. */
    public Future<Void> close() {
        return connection != null ? connection.close() : Future.succeededFuture();
    }
}
