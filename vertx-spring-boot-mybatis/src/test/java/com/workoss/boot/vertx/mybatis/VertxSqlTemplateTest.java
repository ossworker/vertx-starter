package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.template.VertxSqlTemplate;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VertxSqlTemplate}.
 * <p>
 * Tests the wrapper API structure, configuration, and fluent interface.
 * Note: Template caching and execution tests require a real SqlClient
 * (SqlTemplate internally casts to SqlClientInternal), so they are
 * covered by integration tests.
 */
class VertxSqlTemplateTest {

    @Test
    void testConstructorAcceptsPool() {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);
        assertNotNull(sqlTemplate);
    }

    @Test
    void testQueryReturnsQueryTemplate() {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        VertxSqlTemplate.QueryTemplate qt = sqlTemplate.query("SELECT * FROM users WHERE id = #{id}");
        assertNotNull(qt, "query() should return a non-null QueryTemplate");
    }

    @Test
    void testUpdateReturnsUpdateTemplate() {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        VertxSqlTemplate.UpdateTemplate ut = sqlTemplate.update("UPDATE users SET name = #{name} WHERE id = #{id}");
        assertNotNull(ut, "update() should return a non-null UpdateTemplate");
    }

    @Test
    void testQueryTemplateFluentApiExists() throws Exception {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        // Verify QueryTemplate has the expected methods
        Class<?> qtClass = VertxSqlTemplate.QueryTemplate.class;
        assertNotNull(qtClass.getMethod("mapTo", Class.class));
        assertNotNull(qtClass.getMethod("mapTo", io.vertx.sqlclient.templates.RowMapper.class));
        assertNotNull(qtClass.getMethod("execute", java.util.Map.class));
        assertNotNull(qtClass.getMethod("execute"));
        assertNotNull(qtClass.getMethod("executeBlocking", java.util.Map.class));
        assertNotNull(qtClass.getMethod("executeBlocking"));
    }

    @Test
    void testTypedQueryTemplateFluentApiExists() throws Exception {
        Class<?> tqtClass = VertxSqlTemplate.TypedQueryTemplate.class;
        assertNotNull(tqtClass.getMethod("execute", java.util.Map.class));
        assertNotNull(tqtClass.getMethod("execute"));
        assertNotNull(tqtClass.getMethod("executeBlocking", java.util.Map.class));
        assertNotNull(tqtClass.getMethod("executeBlocking"));
        assertNotNull(tqtClass.getMethod("executeList", java.util.Map.class));
        assertNotNull(tqtClass.getMethod("executeListBlocking", java.util.Map.class));
        assertNotNull(tqtClass.getMethod("executeOne", java.util.Map.class));
        assertNotNull(tqtClass.getMethod("executeOneBlocking", java.util.Map.class));
    }

    @Test
    void testUpdateTemplateFluentApiExists() throws Exception {
        Class<?> utClass = VertxSqlTemplate.UpdateTemplate.class;
        assertNotNull(utClass.getMethod("mapFrom", Class.class));
        assertNotNull(utClass.getMethod("mapFrom", io.vertx.sqlclient.templates.TupleMapper.class));
        assertNotNull(utClass.getMethod("execute", java.util.Map.class));
        assertNotNull(utClass.getMethod("execute"));
        assertNotNull(utClass.getMethod("executeBlocking", java.util.Map.class));
        assertNotNull(utClass.getMethod("executeBlocking"));
    }

    @Test
    void testTypedUpdateTemplateFluentApiExists() throws Exception {
        Class<?> tutClass = VertxSqlTemplate.TypedUpdateTemplate.class;
        assertNotNull(tutClass.getMethod("execute", Object.class));
        assertNotNull(tutClass.getMethod("executeBlocking", Object.class));
        assertNotNull(tutClass.getMethod("executeBatch", java.util.List.class));
    }

    @Test
    void testConvenienceQueryMethod() {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        // query(sql, params) shorthand
        VertxSqlTemplate.QueryTemplate qt = sqlTemplate.query("SELECT 1");
        assertNotNull(qt);
    }

    @Test
    void testConvenienceUpdateMethod() {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        VertxSqlTemplate.UpdateTemplate ut = sqlTemplate.update("DELETE FROM users WHERE id = #{id}");
        assertNotNull(ut);
    }

    @Test
    void testCacheFieldsInitialized() throws Exception {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        Field queryCacheField = VertxSqlTemplate.class.getDeclaredField("queryCache");
        queryCacheField.setAccessible(true);
        ConcurrentHashMap<?, ?> queryCache = (ConcurrentHashMap<?, ?>) queryCacheField.get(sqlTemplate);
        assertNotNull(queryCache);
        assertTrue(queryCache.isEmpty());

        Field updateCacheField = VertxSqlTemplate.class.getDeclaredField("updateCache");
        updateCacheField.setAccessible(true);
        ConcurrentHashMap<?, ?> updateCache = (ConcurrentHashMap<?, ?>) updateCacheField.get(sqlTemplate);
        assertNotNull(updateCache);
        assertTrue(updateCache.isEmpty());
    }

    @Test
    void testMultipleTemplatesCreatedIndependently() {
        Pool mockPool = createStubPool();
        VertxSqlTemplate sqlTemplate = new VertxSqlTemplate(mockPool);

        VertxSqlTemplate.QueryTemplate qt1 = sqlTemplate.query("SELECT * FROM users");
        VertxSqlTemplate.QueryTemplate qt2 = sqlTemplate.query("SELECT * FROM orders");
        VertxSqlTemplate.UpdateTemplate ut1 = sqlTemplate.update("DELETE FROM users WHERE id = #{id}");

        assertNotSame(qt1, qt2);
        assertNotNull(qt1);
        assertNotNull(qt2);
        assertNotNull(ut1);
    }

    /**
     * Create a minimal Pool stub. Returns null/empty for most methods.
     * Sufficient for testing wrapper API structure (not template execution).
     */
    private Pool createStubPool() {
        return (Pool) java.lang.reflect.Proxy.newProxyInstance(
                Pool.class.getClassLoader(),
                new Class[]{Pool.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("close".equals(name)) {
                        return Future.succeededFuture();
                    }
                    if ("hashCode".equals(name)) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(name)) {
                        return proxy == args[0];
                    }
                    if ("toString".equals(name)) {
                        return "StubPool";
                    }
                    // Return null/default for other methods
                    return null;
                }
        );
    }
}
