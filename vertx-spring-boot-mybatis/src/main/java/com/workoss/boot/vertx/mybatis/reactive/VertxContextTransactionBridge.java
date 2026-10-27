package com.workoss.boot.vertx.mybatis.reactive;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binds the active reactive transaction's {@link VertxConnectionHolder} to the current Vert.x
 * {@link Context} so a raw {@link io.vertx.core.Future}-returning mapper method — which carries no
 * Reactor Context — can still find the transactional {@link SqlConnection}.
 * <p>
 * On the Vert.x web/WebFlux request path the current context is a per-request <em>duplicated</em>
 * context, whose local data is copied-on-duplicate, so the binding is request-scoped and never leaks
 * between concurrent requests.
 * <p>
 * <b>Storage choice.</b> This uses {@link ContextInternal}'s object-keyed local map rather than a
 * lazily {@link io.vertx.core.spi.context.storage.ContextLocal#registerLocal registered} typed
 * {@code ContextLocal}. A typed local's index is assigned when its class first loads; if that happens
 * after {@code Vertx.vertx()} built its contexts, the per-context local array is already sized and
 * {@code putLocal} throws {@link IllegalArgumentException}. The object-keyed map is backed by Vert.x's
 * own bootstrap-registered {@code LOCAL_MAP} (index 0), so it is always addressable regardless of when
 * this class loads.
 * <p>
 * <b>Limitation.</b> The binding is only visible while execution stays on a duplicated Vert.x context.
 * It is deliberately <em>not</em> installed on a non-duplicated (root/event-loop) context, because such
 * a context is shared by concurrent tasks and a binding there could leak across them; in that case the
 * authoritative Reactor Context binding still governs the transaction and raw {@code Future} mappers
 * fall back to the pool (auto-commit). Likewise, hopping to a non-Vert.x scheduler
 * ({@code publishOn(Schedulers.boundedElastic())}) between begin and the mapper call loses the binding.
 * Keep transactional DB calls on the event loop, or use the Reactor-Context-based reactive API.
 * <p>
 * The object-keyed {@code ContextInternal} local methods are deprecated for removal in Vert.x, but are
 * used deliberately here: unlike a typed {@code ContextLocal} (whose index is fixed when its class
 * loads, and which therefore throws {@code IllegalArgumentException} if registered after
 * {@code Vertx.vertx()} sized the per-context local array), the object-keyed map is backed by Vert.x's
 * bootstrap-registered {@code LOCAL_MAP} and is always addressable. Migration to the typed API is only
 * safe once this class is guaranteed to load before any {@code Vertx} instance is created.
 */
@SuppressWarnings({"deprecation", "removal"})
public final class VertxContextTransactionBridge {

    private static final Logger logger = LoggerFactory.getLogger(VertxContextTransactionBridge.class);

    /** Key into the Vert.x object-keyed context-local map; value is the innermost {@link VertxConnectionHolder}. */
    private static final String TX_KEY = VertxContextTransactionBridge.class.getName() + ".TX";

    private VertxContextTransactionBridge() {
    }

    /**
     * Install {@code holder} onto the current Vert.x context when it is a duplicated (request-scoped)
     * context. No-op when there is no Vert.x context or it is not duplicated.
     */
    public static void install(VertxConnectionHolder holder) {
        ContextInternal ctx = currentInternal();
        if (ctx == null) {
            return;
        }
        if (!ctx.isDuplicate()) {
            logger.debug("Skipping Vert.x context-local transaction binding on non-duplicated context [{}]; "
                            + "the Reactor Context binding remains authoritative and raw Future mappers will "
                            + "fall back to the pool. Run transactional DB calls on a duplicated "
                            + "(e.g. WebFlux request) context to enable the Future-mapper bridge.", ctx);
            return;
        }
        ctx.putLocal(TX_KEY, holder);
    }

    /** The holder bound to the current Vert.x context, or {@code null}. */
    public static VertxConnectionHolder current() {
        ContextInternal ctx = currentInternal();
        if (ctx == null) {
            return null;
        }
        Object value = ctx.getLocal(TX_KEY);
        return value instanceof VertxConnectionHolder holder ? holder : null;
    }

    /** Remove the binding from the current Vert.x context (no-op when there is none). */
    public static void remove() {
        ContextInternal ctx = currentInternal();
        if (ctx != null) {
            ctx.removeLocal(TX_KEY);
        }
    }

    /** Convenience: the transactional connection bound to the current context, or {@code null}. */
    public static SqlConnection currentConnection() {
        VertxConnectionHolder holder = current();
        return holder != null ? holder.getConnection() : null;
    }

    private static ContextInternal currentInternal() {
        Context ctx = Vertx.currentContext();
        return ctx instanceof ContextInternal internal ? internal : null;
    }
}
