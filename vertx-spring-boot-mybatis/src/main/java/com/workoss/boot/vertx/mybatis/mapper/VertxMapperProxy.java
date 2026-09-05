package com.workoss.boot.vertx.mybatis.mapper;

import com.workoss.boot.vertx.mybatis.core.VertxSqlSession;
import io.vertx.core.Future;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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
 * or a raw {@link VertxSqlSession}.
 * <p>
 * The async path opens a new session from {@link SqlSessionFactory} to obtain a
 * {@link VertxSqlSession} directly, bypassing the interceptor chain and first-level
 * cache for maximum throughput.
 * <p>
 * Also supports interface default methods and Object methods (toString, hashCode, equals).
 */
public class VertxMapperProxy<T> implements InvocationHandler {

    private final SqlSession sqlSession;
    private final Class<T> mapperInterface;
    private final Map<Method, MapperMethodInfo> methodCache;
    private final SqlSessionFactory sqlSessionFactory;

    public VertxMapperProxy(SqlSession sqlSession, Class<T> mapperInterface,
                            Map<Method, MapperMethodInfo> methodCache,
                            SqlSessionFactory sqlSessionFactory) {
        this.sqlSession = sqlSession;
        this.mapperInterface = mapperInterface;
        this.methodCache = methodCache;
        this.sqlSessionFactory = sqlSessionFactory;
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
     * Execute via async path — returns {@link Future}.
     * Opens a new VertxSqlSession for each async call (bypasses interceptor chain).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object executeAsync(MapperMethodInfo info, Object[] args) {
        if (sqlSessionFactory == null) {
            return Future.failedFuture(
                    new BindingException("Async operations require SqlSessionFactory. "
                            + "Use @VertxMapperScan or @MapperScan(factoryBean=VertxMapperFactoryBean.class) "
                            + "to enable async mapper methods."));
        }

        Object param = info.extractParameter(args);
        String statementId = info.getStatementId();

        // Open a new VertxSqlSession for async execution
        SqlSession asyncSession = sqlSessionFactory.openSession(true);
        if (!(asyncSession instanceof VertxSqlSession vertxSession)) {
            asyncSession.close();
            return Future.failedFuture(
                    new BindingException("Async operations require VertxSqlSession, got: "
                            + asyncSession.getClass().getName()));
        }

        Future<?> future;
        try {
            future = switch (info.getSqlCommandType()) {
                case SELECT -> {
                    if (info.isFutureSingleResult()) {
                        yield vertxSession.selectOneAsync(statementId, param);
                    } else {
                        RowBounds rowBounds = info.extractRowBounds(args);
                        yield vertxSession.selectListAsync(statementId, param, rowBounds);
                    }
                }
                case INSERT -> vertxSession.insertAsync(statementId, param);
                case UPDATE -> vertxSession.updateAsync(statementId, param);
                case DELETE -> vertxSession.deleteAsync(statementId, param);
                default -> Future.failedFuture(
                        new BindingException("Unknown execution type for '" + statementId + "'."));
            };
        } catch (Exception e) {
            asyncSession.close();
            return Future.failedFuture(e);
        }

        // Close the session after async completion
        return future.onComplete(r -> asyncSession.close());
    }

    /**
     * Execute via sync path — delegates to the injected {@link SqlSession}.
     * Works with both {@link org.mybatis.spring.SqlSessionTemplate} and raw {@link VertxSqlSession}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
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
