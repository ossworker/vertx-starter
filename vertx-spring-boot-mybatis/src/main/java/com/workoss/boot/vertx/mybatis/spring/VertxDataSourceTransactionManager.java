package com.workoss.boot.vertx.mybatis.spring;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Spring {@link org.springframework.transaction.PlatformTransactionManager} implementation
 * for vertx-sql-client transactions.
 * <p>
 * Enables {@code @Transactional} support by:
 * <ul>
 *   <li>Obtaining a {@link SqlConnection} from the {@link Pool}</li>
 *   <li>Beginning a vertx {@link Transaction}</li>
 *   <li>Storing the connection in Spring's {@link TransactionSynchronizationManager}</li>
 *   <li>Committing or rolling back on transaction completion</li>
 * </ul>
 * <p>
 * The SqlConnection is stored as a resource keyed by the Pool instance,
 * allowing VertxExecutor to detect and reuse the transactional connection.
 */
public class VertxDataSourceTransactionManager extends AbstractPlatformTransactionManager {

    private static final Logger logger = LoggerFactory.getLogger(VertxDataSourceTransactionManager.class);

    private final Pool pool;

    public VertxDataSourceTransactionManager(Pool pool) {
        this.pool = pool;
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        // Check if there's already a connection bound to this Pool
        VertxTransactionResource resource = (VertxTransactionResource)
                TransactionSynchronizationManager.getResource(pool);
        return resource != null ? resource : new VertxTransactionResource();
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        VertxTransactionResource resource = (VertxTransactionResource) transaction;
        return resource.getConnection() != null && resource.getTransaction() != null;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        VertxTransactionResource resource = (VertxTransactionResource) transaction;

        try {
            // Get a connection from the pool
            SqlConnection connection = pool.getConnection()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            // Begin a transaction
            Transaction tx = connection.begin()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            resource.setConnection(connection);
            resource.setTransaction(tx);

            // Bind to Spring's TransactionSynchronizationManager
            TransactionSynchronizationManager.bindResource(pool, resource);

            logger.debug("Vert.x transaction started");
        } catch (Exception e) {
            throw new TransactionException("Failed to begin vertx-sql-client transaction", e) {};
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        VertxTransactionResource resource = (VertxTransactionResource)
                TransactionSynchronizationManager.getResource(pool);

        if (resource == null || resource.getTransaction() == null) {
            throw new TransactionException("No active vertx transaction to commit") {};
        }

        try {
            resource.getTransaction().commit()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
            logger.debug("Vert.x transaction committed");
        } catch (Exception e) {
            throw new TransactionException("Failed to commit vertx-sql-client transaction", e) {};
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        VertxTransactionResource resource = (VertxTransactionResource)
                TransactionSynchronizationManager.getResource(pool);

        if (resource == null || resource.getTransaction() == null) {
            logger.warn("No active vertx transaction to rollback");
            return;
        }

        try {
            resource.getTransaction().rollback()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
            logger.debug("Vert.x transaction rolled back");
        } catch (Exception e) {
            throw new TransactionException("Failed to rollback vertx-sql-client transaction", e) {};
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        // Unbind from TransactionSynchronizationManager
        TransactionSynchronizationManager.unbindResourceIfPossible(pool);

        VertxTransactionResource resource = (VertxTransactionResource) transaction;
        if (resource.getConnection() != null) {
            try {
                resource.getConnection().close()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join();
            } catch (Exception e) {
                logger.warn("Failed to close SqlConnection after transaction", e);
            }
        }
    }

    /**
     * Holds the SqlConnection and Transaction for the current Spring transaction.
     */
    public static class VertxTransactionResource {
        private SqlConnection connection;
        private Transaction transaction;

        public SqlConnection getConnection() {
            return connection;
        }

        public void setConnection(SqlConnection connection) {
            this.connection = connection;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
        }
    }

    /**
     * Get the current transactional SqlConnection bound to this thread, if any.
     * This can be used by VertxExecutor to participate in Spring-managed transactions.
     */
    public static SqlConnection getCurrentConnection(Pool pool) {
        VertxTransactionResource resource = (VertxTransactionResource)
                TransactionSynchronizationManager.getResource(pool);
        return resource != null ? resource.getConnection() : null;
    }
}
