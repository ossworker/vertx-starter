package com.workoss.boot.vertx.mybatis.core;

import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * MyBatis {@link org.apache.ibatis.transaction.Transaction} implementation
 * backed by vertx-sql-client.
 * <p>
 * Since vertx-sql-client does not use JDBC, {@link #getConnection()} returns null.
 * The actual {@link SqlConnection} and {@link Transaction} are managed internally
 * and exposed through dedicated methods for use by {@link VertxExecutor}.
 */
public class VertxTransaction implements org.apache.ibatis.transaction.Transaction {

    private static final Logger logger = LoggerFactory.getLogger(VertxTransaction.class);

    private final Pool pool;
    private SqlConnection connection;
    private Transaction transaction;
    private final boolean autoCommit;
    private boolean closed = false;

    public VertxTransaction(Pool pool, boolean autoCommit) {
        this.pool = pool;
        this.autoCommit = autoCommit;
    }

    /**
     * Opens a connection from the pool. Must be called before any operations.
     *
     * @return a Future that completes when the connection is ready
     */
    public Future<SqlConnection> openConnection() {
        if (connection != null) {
            return Future.succeededFuture(connection);
        }
        return pool.getConnection().map(conn -> {
            this.connection = conn;
            return conn;
        });
    }

    /**
     * Begins a transaction on the current connection.
     * Only meaningful when autoCommit is false.
     *
     * @return a Future that completes when the transaction has started
     */
    public Future<Void> beginTransaction() {
        if (autoCommit || connection == null) {
            return Future.succeededFuture();
        }
        return connection.begin().map(tx -> {
            this.transaction = tx;
            return null;
        });
    }

    /**
     * Returns the current SqlConnection, opening one if necessary (synchronous version).
     */
    public SqlConnection getConnectionSync() {
        if (connection == null) {
            this.connection = pool.getConnection()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
        }
        return connection;
    }

    /**
     * Ensures a transaction is started (synchronous version).
     * Called by VertxExecutor before write operations.
     */
    public void ensureTransaction() {
        getConnectionSync();
        if (!autoCommit && transaction == null) {
            this.transaction = connection.begin()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
        }
    }

    /**
     * Returns the current SqlConnection (may be null if not yet opened).
     */
    public SqlConnection getSqlConnection() {
        return connection;
    }

    /**
     * Returns the current vertx Transaction (may be null if auto-commit or not started).
     */
    public Transaction getVertxTransaction() {
        return transaction;
    }

    @Override
    public Connection getConnection() throws SQLException {
        // We don't use JDBC - return null
        return null;
    }

    @Override
    public void commit() throws SQLException {
        if (transaction != null) {
            try {
                transaction.commit()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();
                logger.debug("Transaction committed");
            } catch (Exception e) {
                throw new SQLException("Failed to commit transaction", e);
            } finally {
                transaction = null;
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (transaction != null) {
            try {
                transaction.rollback()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();
                logger.debug("Transaction rolled back");
            } catch (Exception e) {
                throw new SQLException("Failed to rollback transaction", e);
            } finally {
                transaction = null;
            }
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        if (transaction != null) {
            try {
                rollback();
            } catch (Exception e) {
                logger.warn("Rollback failed during close", e);
            }
        }
        if (connection != null) {
            try {
                connection.close()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();
            } catch (Exception e) {
                logger.warn("Connection close failed", e);
            }
            connection = null;
        }
    }

    @Override
    public Integer getTimeout() throws SQLException {
        return null;
    }
}
