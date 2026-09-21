package com.workoss.boot.vertx.mybatis.mapper;

import com.workoss.boot.vertx.mybatis.core.VertxExecutionEngine;
import com.workoss.boot.vertx.mybatis.core.VertxSqlSession;
import com.workoss.boot.vertx.mybatis.core.VertxSqlSessionFactory;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.mapper.MapperFactoryBean;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spring {@link org.springframework.beans.factory.FactoryBean} that creates a
 * MyBatis mapper proxy backed by {@link VertxMapperProxy}.
 * <p>
 * Extends mybatis-spring's {@link MapperFactoryBean} to inherit:
 * <ul>
 *   <li>Spring lifecycle callbacks ({@code setSqlSessionFactory}, {@code setSqlSessionTemplate})</li>
 *   <li>DAO config validation ({@code checkDaoConfig}, {@code addToConfig})</li>
 *   <li>Automatic XML mapper file loading (when mapper XML is in the same classpath location)</li>
 *   <li>Integration with {@code @MapperScan} via {@code factoryBean} attribute</li>
 * </ul>
 * <p>
 * Overrides {@link #getObject()} to create a {@link VertxMapperProxy} that supports
 * both synchronous (standard MyBatis) and asynchronous ({@code Future}) return types.
 * <p>
 * Usage with mybatis-spring's {@code @MapperScan}:
 * <pre>{@code
 * @MapperScan(value = "com.example.mapper", factoryBean = VertxMapperFactoryBean.class)
 * }</pre>
 * Or use the convenience annotation:
 * <pre>{@code
 * @VertxMapperScan("com.example.mapper")
 * }</pre>
 *
 * @param <T> the mapper interface type
 * @see org.mybatis.spring.mapper.MapperFactoryBean
 */
public class VertxMapperFactoryBean<T> extends MapperFactoryBean<T> {

    /** Shared method cache across all proxy instances of the same mapper interface. */
    private final Map<java.lang.reflect.Method, MapperMethodInfo> methodCache = new ConcurrentHashMap<>();

    public VertxMapperFactoryBean() {
        super();
    }

    public VertxMapperFactoryBean(Class<T> mapperInterface) {
        super(mapperInterface);
    }

    /**
     * Creates a mapper proxy backed by {@link VertxMapperProxy} with async support.
     * <p>
     * The session is obtained from the configured {@link org.apache.ibatis.session.SqlSessionFactory}
     * (or {@link org.mybatis.spring.SqlSessionTemplate}). Each proxy instance gets its own session,
     * and the factory bean ensures proper lifecycle management via mybatis-spring.
     */
    @Override
    public T getObject() throws Exception {
        SqlSession session = getSqlSession();
        SqlSessionFactory factory = getSqlSessionFactory();
        VertxExecutionEngine engine = factory instanceof VertxSqlSessionFactory vertxFactory
                ? vertxFactory.getEngine() : null;
        VertxMapperProxy<T> handler = new VertxMapperProxy<>(
                session, getMapperInterface(), methodCache, engine);
        @SuppressWarnings("unchecked")
        T proxy = (T) Proxy.newProxyInstance(
                getMapperInterface().getClassLoader(),
                new Class[]{getMapperInterface()},
                handler);
        return proxy;
    }
}
