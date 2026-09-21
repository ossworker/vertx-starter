package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.reactive.VertxContextTransactionBridge;
import com.workoss.boot.vertx.mybatis.reactive.VertxReactiveTransactionManager;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Integration tests for {@link VertxReactiveTransactionManager} driving the real Spring reactive
 * transaction machinery ({@link TransactionalOperator}) on a Vert.x event loop with a mocked
 * vertx-sql-client {@link Pool}.
 * <p>
 * Verifies the commit/rollback call sequence, connection cleanup, and — the key bridging behaviour —
 * that the transactional {@link SqlConnection} becomes visible to raw {@code Future} mapper calls via
 * the Vert.x context local while the reactive chain runs on the event loop.
 */
class VertxReactiveTransactionManagerTest {

    private static Vertx vertx;

    // Fresh mocks per test (JUnit creates a new instance per method).
    private Pool pool;
    private SqlConnection connection;
    private Transaction transaction;

    @BeforeAll
    static void startVertx() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    static void stopVertx() {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    private void mockHappyPathPool() {
        pool = mock(Pool.class);
        connection = mock(SqlConnection.class);
        transaction = mock(Transaction.class);
        when(pool.getConnection()).thenReturn(Future.succeededFuture(connection));
        when(connection.begin()).thenReturn(Future.succeededFuture(transaction));
        when(transaction.commit()).thenReturn(Future.succeededFuture());
        when(transaction.rollback()).thenReturn(Future.succeededFuture());
        when(connection.close()).thenReturn(Future.succeededFuture());
    }

    /**
     * Subscribe {@code work} on a <em>duplicated</em> Vert.x context, mirroring the WebFlux per-request
     * context on which the transaction bridge installs its binding. Returns a future of the outcome.
     */
    private CompletableFuture<String> subscribeOnDuplicatedContext(Mono<String> work) {
        ContextInternal duplicated = ((VertxInternal) vertx).getOrCreateContext().duplicate();
        CompletableFuture<String> cf = new CompletableFuture<>();
        duplicated.runOnContext(v -> work.subscribe(cf::complete, cf::completeExceptionally));
        return cf;
    }

    @Test
    void commitsAndExposesConnectionToVertxContext() throws Exception {
        mockHappyPathPool();
        VertxReactiveTransactionManager tm = new VertxReactiveTransactionManager(pool);
        TransactionalOperator txOp = TransactionalOperator.create(tm);

        AtomicReference<SqlConnection> seenInside = new AtomicReference<>();
        Mono<String> work = txOp.transactional(Mono.fromSupplier(() -> {
            // This is what a raw Future-returning mapper method relies on: the Vert.x context local.
            seenInside.set(VertxContextTransactionBridge.currentConnection());
            return "ok";
        }));

        CompletableFuture<String> cf = subscribeOnDuplicatedContext(work);

        assertEquals("ok", cf.get(5, TimeUnit.SECONDS));
        assertSame(connection, seenInside.get(),
                "transactional connection must be visible on the Vert.x context inside the tx");
        verify(transaction).commit();
        verify(connection).close();
        verify(transaction, never()).rollback();
    }

    @Test
    void rollsBackOnErrorAndClosesConnection() throws Exception {
        mockHappyPathPool();
        VertxReactiveTransactionManager tm = new VertxReactiveTransactionManager(pool);
        TransactionalOperator txOp = TransactionalOperator.create(tm);

        IllegalStateException boom = new IllegalStateException("boom");
        Mono<String> work = txOp.transactional(Mono.error(boom));

        CompletableFuture<String> cf = subscribeOnDuplicatedContext(work);

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> cf.get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, thrown.getCause());
        verify(transaction).rollback();
        verify(transaction, never()).commit();
        verify(connection).close();
    }

    @Test
    void closesConnectionWhenBeginFails() throws Exception {
        pool = mock(Pool.class);
        connection = mock(SqlConnection.class);
        when(pool.getConnection()).thenReturn(Future.succeededFuture(connection));
        when(connection.begin()).thenReturn(Future.failedFuture(new RuntimeException("begin failed")));
        when(connection.close()).thenReturn(Future.succeededFuture());

        VertxReactiveTransactionManager tm = new VertxReactiveTransactionManager(pool);
        TransactionalOperator txOp = TransactionalOperator.create(tm);

        Mono<String> work = txOp.transactional(Mono.just("ok"));
        CompletableFuture<String> cf = subscribeOnDuplicatedContext(work);

        assertThrows(ExecutionException.class, () -> cf.get(5, TimeUnit.SECONDS));
        // The connection must not leak when BEGIN fails; no transaction was ever created.
        verify(connection).close();
    }

    @Test
    void poolAccessorIsExposed() {
        mockHappyPathPool();
        VertxReactiveTransactionManager tm = new VertxReactiveTransactionManager(pool);
        assertSame(pool, tm.getPool());
    }
}
