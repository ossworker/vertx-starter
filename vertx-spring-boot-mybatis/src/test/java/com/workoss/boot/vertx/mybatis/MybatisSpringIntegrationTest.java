package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.annotation.VertxMapperScan;
import com.workoss.boot.vertx.mybatis.mapper.VertxMapperFactoryBean;
import com.workoss.boot.vertx.mybatis.mapper.VertxMapperProxyFactory;
import org.apache.ibatis.binding.MapperProxyFactory;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.boot.autoconfigure.ConfigurationCustomizer;
import org.mybatis.spring.boot.autoconfigure.MybatisProperties;
import org.mybatis.spring.mapper.MapperFactoryBean;

import java.lang.annotation.Annotation;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify mybatis-spring integration points.
 */
class MybatisSpringIntegrationTest {

    @Test
    void testVertxMapperFactoryBeanExtendsMybatisSpring() {
        // VertxMapperFactoryBean should extend mybatis-spring's MapperFactoryBean
        assertTrue(MapperFactoryBean.class.isAssignableFrom(VertxMapperFactoryBean.class),
                "VertxMapperFactoryBean should extend MapperFactoryBean from mybatis-spring");
    }

    @Test
    void testVertxMapperProxyFactoryExtendsMyBatisCore() {
        // VertxMapperProxyFactory should extend MyBatis core's MapperProxyFactory
        assertTrue(MapperProxyFactory.class.isAssignableFrom(VertxMapperProxyFactory.class),
                "VertxMapperProxyFactory should extend MapperProxyFactory from MyBatis core");
    }

    @Test
    void testVertxMapperScanAnnotatedWithMapperScan() {
        // @VertxMapperScan should be meta-annotated with @MapperScan
        MapperScan mapperScan = VertxMapperScan.class.getAnnotation(MapperScan.class);
        assertNotNull(mapperScan, "@VertxMapperScan should be meta-annotated with @MapperScan");

        // factoryBean should be VertxMapperFactoryBean
        assertEquals(VertxMapperFactoryBean.class, mapperScan.factoryBean(),
                "@VertxMapperScan's @MapperScan should use VertxMapperFactoryBean");
    }

    @Test
    void testVertxMapperFactoryBeanInheritance() throws Exception {
        // Verify VertxMapperFactoryBean has the overridden getObject method
        var method = VertxMapperFactoryBean.class.getMethod("getObject");
        assertNotNull(method, "VertxMapperFactoryBean should override getObject()");

        // Verify it inherits setSqlSessionFactory from MapperFactoryBean
        var setter = VertxMapperFactoryBean.class.getMethod("setSqlSessionFactory",
                org.apache.ibatis.session.SqlSessionFactory.class);
        assertNotNull(setter, "Should inherit setSqlSessionFactory from MapperFactoryBean");
    }

    @Test
    void testMybatisSpringBootAutoconfigureOnClasspath() {
        // Verify mybatis-spring-boot-autoconfigure classes are available
        assertNotNull(MybatisProperties.class, "MybatisProperties from mybatis-spring-boot should be on classpath");
        assertNotNull(ConfigurationCustomizer.class, "ConfigurationCustomizer should be on classpath");
        assertNotNull(SqlSessionTemplate.class, "SqlSessionTemplate should be on classpath");

        // MybatisProperties should use standard 'mybatis' prefix
        assertEquals("mybatis", MybatisProperties.MYBATIS_PREFIX);
    }

    @Test
    void testMybatisPropertiesCoreConfigurationAvailable() {
        // CoreConfiguration should provide all standard mybatis.configuration.* settings
        MybatisProperties props = new MybatisProperties();
        assertNull(props.getConfiguration()); // null by default, created only when mybatis.configuration.* is set

        // CoreConfiguration should have the settings we used to duplicate
        MybatisProperties.CoreConfiguration coreConfig = new MybatisProperties.CoreConfiguration();
        coreConfig.setMapUnderscoreToCamelCase(true);
        coreConfig.setCacheEnabled(true);
        coreConfig.setLazyLoadingEnabled(false);
        assertTrue(coreConfig.getMapUnderscoreToCamelCase());
        assertTrue(coreConfig.getCacheEnabled());
        assertFalse(coreConfig.getLazyLoadingEnabled());
    }

    @Test
    void testVertxMapperProxyFactoryInheritance() throws Exception {
        // Verify VertxMapperProxyFactory overrides newInstance
        var method = VertxMapperProxyFactory.class.getMethod("newInstance",
                org.apache.ibatis.session.SqlSession.class);
        assertNotNull(method, "VertxMapperProxyFactory should override newInstance(SqlSession)");

        // Verify it has the two-arg variant
        var method2 = VertxMapperProxyFactory.class.getMethod("newInstance",
                org.apache.ibatis.session.SqlSession.class,
                org.apache.ibatis.session.SqlSessionFactory.class);
        assertNotNull(method2, "VertxMapperProxyFactory should have newInstance(SqlSession, SqlSessionFactory)");
    }
}
