package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.core.VertxExecutionEngine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the engine's single blocking boundary {@link VertxExecutionEngine#block(Future)}:
 * it works off the event loop, propagates failures cleanly, and — crucially — fails fast with
 * {@link IllegalStateException} when called on a Vert.x thread instead of silently deadlocking.
 */
class VertxExecutionEngineBlockGuardTest {

    // block() never touches the pool/resolver/dialect, so nulls are safe here.
    private final VertxExecutionEngine engine = new VertxExecutionEngine(null, null, null, null, null);

    private static Vertx vertx;

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

    @Test
    void blockReturnsValueOffVertxThread() {
        assertEquals(42, engine.block(Future.succeededFuture(42)));
    }

    @Test
    void blockRethrowsOriginalRuntimeExceptionOffVertxThread() {
        RuntimeException boom = new RuntimeException("boom");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> engine.block(Future.failedFuture(boom)));
        assertSame(boom, thrown);
    }

    @Test
    void blockOnVertxThreadThrowsIllegalState() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<Object> result = new AtomicReference<>();

        vertx.runOnContext(v -> {
            try {
                result.set(engine.block(Future.succeededFuture("x")));
            } catch (Throwable t) {
                error.set(t);
            } finally {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "runOnContext callback did not execute");
        assertNull(result.get(), "block() must not return a value on a Vert.x thread");
        assertNotNull(error.get(), "block() must throw on a Vert.x thread");
        assertInstanceOf(IllegalStateException.class, error.get());
    }
}
