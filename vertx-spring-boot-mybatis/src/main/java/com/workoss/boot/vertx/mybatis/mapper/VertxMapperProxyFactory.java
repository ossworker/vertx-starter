package com.workoss.boot.vertx.mybatis.mapper;

import com.workoss.boot.vertx.mybatis.core.VertxExecutionEngine;
import com.workoss.boot.vertx.mybatis.core.VertxSqlSessionFactory;
import org.apache.ibatis.binding.MapperProxyFactory;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating JDK dynamic proxy instances of mapper interfaces,
 * extending MyBatis's {@link MapperProxyFactory} for compatibility.
 * <p>
 * Each proxy uses a shared method cache for efficiency and is backed by
 * {@link VertxMapperProxy} which supports both sync and async execution.
 *
 * @param <T> the mapper interface type
 * @see MapperProxyFactory
 */
public class VertxMapperProxyFactory<T> extends MapperProxyFactory<T> {

    private final Map<Method, MapperMethodInfo> methodCache = new ConcurrentHashMap<>();

    public VertxMapperProxyFactory(Class<T> mapperInterface) {
        super(mapperInterface);
    }

    /**
     * Create a new proxy instance backed by {@link VertxMapperProxy}.
     *
     * @param sqlSession the SqlSession to use for sync operations
     * @return a new mapper proxy
     */
    @Override
    public T newInstance(SqlSession sqlSession) {
        // When called via Configuration.getMapper(), no engine is available.
        // Async (Future-returning) methods will not be supported in this path.
        VertxMapperProxy<T> handler = new VertxMapperProxy<>(
                sqlSession, getMapperInterface(), methodCache, (VertxExecutionEngine) null);
        return newProxyInstance(handler);
    }

    /**
     * Create a new proxy instance with an explicit {@link SqlSessionFactory}.
     * This is the preferred method called by {@link VertxMapperFactoryBean}.
     *
     * @param sqlSession        the SqlSession for sync operations
     * @param sqlSessionFactory the factory providing the shared async engine
     * @return a new mapper proxy
     */
    public T newInstance(SqlSession sqlSession, SqlSessionFactory sqlSessionFactory) {
        VertxExecutionEngine engine = sqlSessionFactory instanceof VertxSqlSessionFactory vertxFactory
                ? vertxFactory.getEngine() : null;
        VertxMapperProxy<T> handler = new VertxMapperProxy<>(
                sqlSession, getMapperInterface(), methodCache, engine);
        return newProxyInstance(handler);
    }

    @SuppressWarnings("unchecked")
    private T newProxyInstance(VertxMapperProxy<T> handler) {
        return (T) Proxy.newProxyInstance(
                getMapperInterface().getClassLoader(),
                new Class[]{getMapperInterface()},
                handler);
    }
}
