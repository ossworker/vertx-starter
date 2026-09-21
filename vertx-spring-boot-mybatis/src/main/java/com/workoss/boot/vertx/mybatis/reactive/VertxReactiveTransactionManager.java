package com.workoss.boot.vertx.mybatis.reactive;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

/**
 * A fully reactive Spring {@link org.springframework.transaction.ReactiveTransactionManager} backed
 * by vertx-sql-client — no JDBC, no blocking, no {@code ThreadLocal}.
 * <p>
 * Each transaction boundary checks out one {@link SqlConnection}, begins a vertx {@link Transaction}
 * and wraps both in a {@link VertxConnectionHolder}. The holder is bound in <b>two</b> places:
 * <ol>
 *   <li><b>Reactor Context</b> (authoritative) — via {@link TransactionSynchronizationManager#bindResource}.
 *       This is what Spring's reactive infrastructure and nested propagation read, and it survives
 *       thread hops because it travels with the reactive chain.</li>
 *   <li><b>Vert.x duplicated context</b> (best effort) — via {@link VertxContextTransactionBridge#install}.
 *       This lets a raw {@link io.vertx.core.Future}-returning mapper method, which carries no Reactor
 *       Context, still find the same transactional connection while execution stays on the event loop.</li>
 * </ol>
 * <p>
 * All commit/rollback/suspend/resume hooks read the holder back from the Reactor Context resource map
 * ({@code sync.getResource(pool)}), which is authoritative and correct even for {@code REQUIRES_NEW},
 * where the framework may reuse the same transaction-object instance across the suspend/resume pair.
 * <p>
 * <b>Limitation.</b> The Reactor Context binding is always correct. The Vert.x-local binding is only
 * visible while execution stays on the Vert.x context where {@code doBegin} ran. If application code
 * hops to a non-Vert.x scheduler (e.g. {@code publishOn(Schedulers.boundedElastic())}) between the
 * transaction begin and a raw {@code Future} mapper call, {@link io.vertx.core.Vertx#currentContext()}
 * is {@code null} there and the mapper falls back to the pool (auto-commit). Keep DB calls on the event
 * loop, or drive them from the reactive chain so the Reactor Context binding applies.
 *
 * @see VertxConnectionHolder
 * @see VertxContextTransactionBridge
 */
public class VertxReactiveTransactionManager extends AbstractReactiveTransactionManager {

    private final Pool pool;

    public VertxReactiveTransactionManager(Pool pool) {
        this.pool = pool;
    }

    public Pool getPool() {
        return pool;
    }

    // ===== Transaction state =====

    @Override
    protected Object doGetTransaction(TransactionSynchronizationManager sync) {
        return new VertxTransactionObject(currentHolder(sync));
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        return transaction instanceof VertxTransactionObject txObject && txObject.holder != null;
    }

    // ===== Lifecycle =====

    @Override
    protected Mono<Void> doBegin(TransactionSynchronizationManager sync, Object transaction,
                                 TransactionDefinition definition) {
        return Mono.fromCompletionStage(() -> pool.getConnection().toCompletionStage())
                .flatMap(connection -> Mono
                        .fromCompletionStage(() -> connection.begin().toCompletionStage())
                        .map(tx -> new VertxConnectionHolder(connection, tx))
                        // Never leak the connection if BEGIN fails.
                        .onErrorResume(err -> Mono
                                .fromCompletionStage(() -> connection.close().toCompletionStage())
                                .then(Mono.error(err))))
                .doOnNext(holder -> {
                    // (A) authoritative Reactor Context binding
                    sync.bindResource(pool, holder);
                    // (B) best-effort Vert.x context binding for raw Future mappers
                    VertxContextTransactionBridge.install(holder);
                })
                .then();
    }

    @Override
    protected Mono<Void> doCommit(TransactionSynchronizationManager sync, GenericReactiveTransaction status) {
        VertxConnectionHolder holder = currentHolder(sync);
        if (holder == null) {
            return Mono.empty();
        }
        return Mono.fromCompletionStage(() -> holder.commit().toCompletionStage());
    }

    @Override
    protected Mono<Void> doRollback(TransactionSynchronizationManager sync, GenericReactiveTransaction status) {
        VertxConnectionHolder holder = currentHolder(sync);
        if (holder == null) {
            return Mono.empty();
        }
        return Mono.fromCompletionStage(() -> holder.rollback().toCompletionStage());
    }

    @Override
    protected Mono<Void> doSetRollbackOnly(TransactionSynchronizationManager sync, GenericReactiveTransaction status) {
        VertxConnectionHolder holder = currentHolder(sync);
        if (holder != null) {
            holder.setRollbackOnly();
        }
        return Mono.empty();
    }

    // ===== Suspension (PROPAGATION_REQUIRES_NEW / NOT_SUPPORTED) =====

    @Override
    protected Mono<Object> doSuspend(TransactionSynchronizationManager sync, Object transaction) {
        VertxConnectionHolder holder = currentHolder(sync);
        unbind(sync);
        return holder != null ? Mono.just(holder) : Mono.empty();
    }

    @Override
    protected Mono<Void> doResume(TransactionSynchronizationManager sync, Object transaction,
                                  Object suspendedResources) {
        if (suspendedResources instanceof VertxConnectionHolder holder) {
            sync.bindResource(pool, holder);
            VertxContextTransactionBridge.install(holder);
        }
        return Mono.empty();
    }

    // ===== Cleanup =====

    @Override
    protected Mono<Void> doCleanupAfterCompletion(TransactionSynchronizationManager sync, Object transaction) {
        VertxConnectionHolder holder = currentHolder(sync);
        unbind(sync);
        return holder != null
                ? Mono.fromCompletionStage(() -> holder.close().toCompletionStage())
                : Mono.empty();
    }

    // ===== Internals =====

    private VertxConnectionHolder currentHolder(TransactionSynchronizationManager sync) {
        Object resource = sync.getResource(pool);
        return resource instanceof VertxConnectionHolder holder ? holder : null;
    }

    private void unbind(TransactionSynchronizationManager sync) {
        if (sync.hasResource(pool)) {
            sync.unbindResource(pool);
        }
        VertxContextTransactionBridge.remove();
    }

    /**
     * Transient carrier returned by {@link #doGetTransaction}; only used to answer
     * {@link #isExistingTransaction}. The authoritative holder always comes from the Reactor Context
     * resource map, so this object is never mutated after creation.
     */
    private static final class VertxTransactionObject {
        private final VertxConnectionHolder holder;

        private VertxTransactionObject(VertxConnectionHolder holder) {
            this.holder = holder;
        }
    }
}
