package com.workoss.boot.vertx.mybatis.mapper;

import com.workoss.boot.vertx.mybatis.core.AsyncCapturingExecutor;
import com.workoss.boot.vertx.mybatis.core.VertxExecutionEngine;
import com.workoss.boot.vertx.mybatis.core.VertxSqlSessionFactory;
import io.vertx.core.Future;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Enhanced {@link InvocationHandler} for MyBatis mapper proxies.
 * <p>
 * Detects the return type of each method call:
 * <ul>
 *   <li>{@code Future<T>} — async path (non-blocking, returns Future directly)</li>
 *   <li>{@code Future<List<T>>} — async list path</li>
 *   <li>Regular types — sync path (blocking, delegates to {@link SqlSession})</li>
 * </ul>
 * <p>
 * The sync path delegates to the injected {@link SqlSession}, which may be a
 * {@link org.mybatis.spring.SqlSessionTemplate} (thread-safe, auto-managed)
 * or a raw {@link com.workoss.boot.vertx.mybatis.core.VertxSqlSession}.
 * <p>
 * The async path runs on the shared, thread-safe {@link VertxExecutionEngine} — no session is opened
 * per call and nothing blocks; connection/transaction selection is delegated to the engine's
 * {@link com.workoss.boot.vertx.mybatis.core.SqlConnectionResolver} (reactive transaction → blocking
 * Spring transaction → pool). When MyBatis {@link Interceptor}s are registered, the call is routed
 * through an {@link AsyncCapturingExecutor} wrapped by the interceptor chain, so SQL-rewriting plugins
 * are honoured without blocking; with no interceptors it calls the engine directly. The first-level
 * (session) cache is bypassed on this path for throughput.
 * <p>
 * Also supports interface default methods and Object methods (toString, hashCode, equals).
 */
public class VertxMapperProxy<T> implements InvocationHandler {

    private final SqlSession sqlSession;
    private final Class<T> mapperInterface;
    private final Map<Method, MapperMethodInfo> methodCache;
    private final VertxExecutionEngine engine;

    /**
     * Preferred constructor: the async path runs against the shared {@link VertxExecutionEngine}.
     *
     * @param engine the shared engine; when {@code null}, {@code Future}-returning mapper methods fail
     */
    public VertxMapperProxy(SqlSession sqlSession, Class<T> mapperInterface,
                            Map<Method, MapperMethodInfo> methodCache,
                            VertxExecutionEngine engine) {
        this.sqlSession = sqlSession;
        this.mapperInterface = mapperInterface;
        this.methodCache = methodCache;
        this.engine = engine;
    }

    /**
     * @deprecated use {@link #VertxMapperProxy(SqlSession, Class, Map, VertxExecutionEngine)}. The
     *             engine is derived from {@code sqlSessionFactory} when it is a
     *             {@link VertxSqlSessionFactory}; otherwise async mapper methods are unsupported.
     */
    @Deprecated
    public VertxMapperProxy(SqlSession sqlSession, Class<T> mapperInterface,
                            Map<Method, MapperMethodInfo> methodCache,
                            SqlSessionFactory sqlSessionFactory) {
        this(sqlSession, mapperInterface, methodCache,
                sqlSessionFactory instanceof VertxSqlSessionFactory f ? f.getEngine() : null);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 1. Object methods (toString, hashCode, equals)
        if (Object.class.equals(method.getDeclaringClass())) {
            return method.invoke(this, args);
        }

        // 2. Default interface methods
        if (method.isDefault()) {
            return invokeDefaultMethod(proxy, method, args);
        }

        // 3. Get cached method info
        MapperMethodInfo info = methodCache.computeIfAbsent(method,
                m -> new MapperMethodInfo(mapperInterface, m, sqlSession.getConfiguration()));

        // 4. Dispatch based on return type
        if (info.isFutureReturnType()) {
            return executeAsync(info, args);
        } else {
            return executeSync(info, args);
        }
    }

    /**
     * Execute via the async path — returns {@link Future}, never blocks, runs on the shared engine.
     * When interceptors are registered, the call is routed through the interceptor chain via an
     * {@link AsyncCapturingExecutor} so SQL-rewriting plugins are honoured; otherwise the engine is
     * called directly (zero proxy overhead).
     */
    private Object executeAsync(MapperMethodInfo info, Object[] args) {
        if (engine == null) {
            return Future.failedFuture(
                    new BindingException("Async operations require a VertxExecutionEngine. "
                            + "Use @VertxMapperScan or @MapperScan(factoryBean=VertxMapperFactoryBean.class) "
                            + "to enable async mapper methods."));
        }

        Object param = info.extractParameter(args);
        String statementId = info.getStatementId();
        Configuration configuration = sqlSession.getConfiguration();

        final MappedStatement ms;
        try {
            ms = configuration.getMappedStatement(statementId);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        List<Interceptor> interceptors = configuration.getInterceptors();
        boolean useChain = interceptors != null && !interceptors.isEmpty();

        try {
            return switch (info.getSqlCommandType()) {
                case SELECT -> {
                    RowBounds rowBounds = info.extractRowBounds(args);
                    Future<List<Object>> rows = useChain
                            ? queryThroughChain(ms, param, rowBounds, interceptors)
                            : engine.query(ms, param, rowBounds, null);
                    yield info.isFutureSingleResult() ? (Future<?>) singleResult(rows) : (Future<?>) rows;
                }
                case INSERT, UPDATE, DELETE -> useChain
                        ? updateThroughChain(ms, param, interceptors)
                        : engine.update(ms, param);
                default -> Future.failedFuture(
                        new BindingException("Unknown execution type for '" + statementId + "'."));
            };
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * Run a SELECT through the interceptor chain: the chain may rewrite the statement before reaching
     * the {@link AsyncCapturingExecutor}, which triggers the engine and captures its {@link Future}.
     */
    @SuppressWarnings("unchecked")
    private Future<List<Object>> queryThroughChain(MappedStatement ms, Object param, RowBounds rowBounds,
                                                   List<Interceptor> interceptors) throws SQLException {
        AsyncCapturingExecutor capturing = new AsyncCapturingExecutor(engine);
        Executor wrapped = AsyncCapturingExecutor.chain(capturing, interceptors);
        Object syncResult = wrapped.query(ms, param, rowBounds, null);
        Future<?> captured = capturing.getCaptured();
        if (captured != null) {
            return (Future<List<Object>>) captured;
        }
        // An interceptor short-circuited (returned without proceed(), e.g. a cache hit).
        List<Object> rows = syncResult instanceof List ? (List<Object>) syncResult : Collections.emptyList();
        return Future.succeededFuture(rows);
    }

    /** Run an INSERT/UPDATE/DELETE through the interceptor chain, capturing the engine {@link Future}. */
    @SuppressWarnings("unchecked")
    private Future<Integer> updateThroughChain(MappedStatement ms, Object param,
                                               List<Interceptor> interceptors) throws SQLException {
        AsyncCapturingExecutor capturing = new AsyncCapturingExecutor(engine);
        Executor wrapped = AsyncCapturingExecutor.chain(capturing, interceptors);
        Object syncResult = wrapped.update(ms, param);
        Future<?> captured = capturing.getCaptured();
        if (captured != null) {
            return (Future<Integer>) captured;
        }
        int rows = syncResult instanceof Number n ? n.intValue() : 0;
        return Future.succeededFuture(rows);
    }

    /** Map a row-list {@link Future} to a single-result {@link Future} (MyBatis {@code selectOne} rules). */
    private static Future<Object> singleResult(Future<List<Object>> rows) {
        return rows.map(list -> {
            if (list == null) {
                return null;
            }
            if (list.size() > 1) {
                throw new TooManyResultsException(
                        "Expected one result (or null) to be returned by selectOne(), but found: "
                                + list.size());
            }
            return list.isEmpty() ? null : list.get(0);
        });
    }

    /**
     * Execute via sync path — delegates to the injected {@link SqlSession}.
     * Works with both {@link org.mybatis.spring.SqlSessionTemplate} and raw
     * {@link com.workoss.boot.vertx.mybatis.core.VertxSqlSession}.
     */
    private Object executeSync(MapperMethodInfo info, Object[] args) {
        Object param = info.extractParameter(args);
        String statementId = info.getStatementId();
        RowBounds rowBounds = info.extractRowBounds(args);

        return switch (info.getSqlCommandType()) {
            case SELECT -> {
                if (info.isCursorReturnType()) {
                    yield sqlSession.selectCursor(statementId, param, rowBounds);
                } else if (info.isSingleResult() && !info.isOptionalReturn()) {
                    yield sqlSession.selectOne(statementId, param);
                } else if (info.isOptionalReturn()) {
                    yield Optional.ofNullable(sqlSession.selectOne(statementId, param));
                } else {
                    yield sqlSession.selectList(statementId, param, rowBounds);
                }
            }
            case INSERT -> sqlSession.insert(statementId, param);
            case UPDATE -> sqlSession.update(statementId, param);
            case DELETE -> sqlSession.delete(statementId, param);
            case FLUSH -> sqlSession.flushStatements();
            default -> throw new BindingException(
                    "Unknown execution type for '" + statementId + "'. " +
                            "Please check your mapper XML or annotation.");
        };
    }

    /**
     * Invoke a default interface method using MethodHandles.
     */
    private Object invokeDefaultMethod(Object proxy, Method method, Object[] args) throws Throwable {
        // Java 9+ approach
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(
                    method.getDeclaringClass(), MethodHandles.lookup());
            return lookup.findSpecial(
                    method.getDeclaringClass(),
                    method.getName(),
                    MethodType.methodType(method.getReturnType(), method.getParameterTypes()),
                    method.getDeclaringClass())
                    .bindTo(proxy)
                    .invokeWithArguments(args);
        } catch (IllegalAccessException e) {
            // Fallback for restricted access
            Constructor<MethodHandles.Lookup> constructor =
                    MethodHandles.Lookup.class.getDeclaredConstructor(Class.class);
            constructor.setAccessible(true);
            return constructor.newInstance(method.getDeclaringClass())
                    .in(method.getDeclaringClass())
                    .unreflectSpecial(method, method.getDeclaringClass())
                    .bindTo(proxy)
                    .invokeWithArguments(args);
        }
    }

    @Override
    public String toString() {
        return "VertxMapperProxy[" + mapperInterface.getName() + "]";
    }
}
