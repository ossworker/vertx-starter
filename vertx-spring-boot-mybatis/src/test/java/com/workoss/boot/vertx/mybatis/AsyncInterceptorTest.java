package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.core.VertxExecutionEngine;
import com.workoss.boot.vertx.mybatis.mapper.VertxMapperProxy;
import io.vertx.core.Future;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies that MyBatis {@link Interceptor}s participate in the non-blocking {@code Future} mapper
 * path via {@link com.workoss.boot.vertx.mybatis.core.AsyncCapturingExecutor}: SQL-rewriting plugins
 * are honoured (the engine receives the rewritten statement), a short-circuiting plugin's value is
 * delivered without touching the engine, and — when no plugins are registered — the engine is still
 * called directly with zero proxy overhead.
 */
class AsyncInterceptorTest {

    @Mapper
    interface TestMapper {
        Future<List<String>> findAll();

        Future<String> findById(@Param("id") Long id);

        Future<Integer> insertAsync(@Param("name") String name);
    }

    @Test
    void sqlRewritingInterceptorIsHonouredOnAsyncSelect() {
        Configuration config = new Configuration();
        addStatement(config, "findAll", SqlCommandType.SELECT);
        MappedStatement rewritten = mock(MappedStatement.class);
        RewritingInterceptor interceptor = new RewritingInterceptor(rewritten);
        config.addInterceptor(interceptor);

        VertxExecutionEngine engine = mock(VertxExecutionEngine.class);
        doReturn(Future.succeededFuture(rows("a", "b")))
                .when(engine).query(any(), any(), any(), any());

        Future<List<String>> f = mapperProxy(config, engine).findAll();

        assertTrue(interceptor.invoked.get(), "interceptor must run on the async path");
        assertEquals(List.of("a", "b"), join(f));
        // The engine must receive the interceptor-rewritten statement, not the original.
        verify(engine).query(eq(rewritten), any(), any(), any());
    }

    @Test
    void sqlRewritingInterceptorIsHonouredOnAsyncUpdate() {
        Configuration config = new Configuration();
        addStatement(config, "insertAsync", SqlCommandType.INSERT);
        MappedStatement rewritten = mock(MappedStatement.class);
        RewritingInterceptor interceptor = new RewritingInterceptor(rewritten);
        config.addInterceptor(interceptor);

        VertxExecutionEngine engine = mock(VertxExecutionEngine.class);
        doReturn(Future.succeededFuture(7)).when(engine).update(any(), any());

        Future<Integer> f = mapperProxy(config, engine).insertAsync("x");

        assertTrue(interceptor.invoked.get());
        assertEquals(7, join(f));
        verify(engine).update(eq(rewritten), any());
    }

    @Test
    void singleResultIsMappedFromListThroughChain() {
        Configuration config = new Configuration();
        addStatement(config, "findById", SqlCommandType.SELECT);
        config.addInterceptor(new RewritingInterceptor(null)); // proceeds without rewriting

        VertxExecutionEngine engine = mock(VertxExecutionEngine.class);
        doReturn(Future.succeededFuture(rows("only")))
                .when(engine).query(any(), any(), any(), any());

        Future<String> f = mapperProxy(config, engine).findById(1L);

        assertEquals("only", join(f));
    }

    @Test
    void shortCircuitInterceptorDeliversValueWithoutTouchingEngine() {
        Configuration config = new Configuration();
        addStatement(config, "findAll", SqlCommandType.SELECT);
        config.addInterceptor(new ShortCircuitInterceptor(rows("cached")));

        VertxExecutionEngine engine = mock(VertxExecutionEngine.class);

        Future<List<String>> f = mapperProxy(config, engine).findAll();

        assertEquals(List.of("cached"), join(f));
        verifyNoInteractions(engine);
    }

    @Test
    void noInterceptorsCallsEngineDirectly() {
        Configuration config = new Configuration();
        addStatement(config, "findAll", SqlCommandType.SELECT);

        VertxExecutionEngine engine = mock(VertxExecutionEngine.class);
        doReturn(Future.succeededFuture(rows("z")))
                .when(engine).query(any(), any(), any(), any());

        Future<List<String>> f = mapperProxy(config, engine).findAll();

        assertEquals(List.of("z"), join(f));
        verify(engine).query(any(), any(), any(), any());
    }

    // ===== fixtures =====

    @SuppressWarnings("unchecked")
    private TestMapper mapperProxy(Configuration config, VertxExecutionEngine engine) {
        SqlSession session = mock(SqlSession.class);
        when(session.getConfiguration()).thenReturn(config);
        VertxMapperProxy<TestMapper> handler =
                new VertxMapperProxy<>(session, TestMapper.class, new HashMap<>(), engine);
        return (TestMapper) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{TestMapper.class}, handler);
    }

    private void addStatement(Configuration config, String method, SqlCommandType type) {
        String id = TestMapper.class.getName() + "." + method;
        config.addMappedStatement(new MappedStatement.Builder(
                config, id, new RawSqlSource(config, "SELECT 1", null), type).build());
    }

    private static List<Object> rows(Object... values) {
        List<Object> list = new ArrayList<>();
        for (Object v : values) {
            list.add(v);
        }
        return list;
    }

    private static <T> T join(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }

    /** Swaps the {@link MappedStatement} (the classic SQL-rewrite move) before proceeding. */
    @Intercepts({
            @Signature(type = Executor.class, method = "query",
                    args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
            @Signature(type = Executor.class, method = "update",
                    args = {MappedStatement.class, Object.class})
    })
    static class RewritingInterceptor implements Interceptor {
        private final MappedStatement rewriteTo;
        final AtomicBoolean invoked = new AtomicBoolean();

        RewritingInterceptor(MappedStatement rewriteTo) {
            this.rewriteTo = rewriteTo;
        }

        @Override
        public Object intercept(Invocation invocation) throws Throwable {
            invoked.set(true);
            if (rewriteTo != null) {
                invocation.getArgs()[0] = rewriteTo;
            }
            return invocation.proceed();
        }
    }

    /** Returns a value without calling {@code proceed()} — models a cache-hit short circuit. */
    @Intercepts({
            @Signature(type = Executor.class, method = "query",
                    args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class})
    })
    static class ShortCircuitInterceptor implements Interceptor {
        private final List<?> result;

        ShortCircuitInterceptor(List<?> result) {
            this.result = result;
        }

        @Override
        public Object intercept(Invocation invocation) {
            return result;
        }
    }
}
