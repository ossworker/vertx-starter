package com.workoss.boot.vertx.mybatis.config;

import com.workoss.boot.vertx.autoconfigure.VertxAutoConfiguration;
import com.workoss.boot.vertx.mybatis.core.SqlResolver;
import com.workoss.boot.vertx.mybatis.core.VertxRowMapper;
import com.workoss.boot.vertx.mybatis.core.VertxSqlSessionFactory;
import com.workoss.boot.vertx.mybatis.core.VertxSqlSessionManager;
import com.workoss.boot.vertx.mybatis.spring.VertxDataSourceTransactionManager;
import com.workoss.boot.vertx.mybatis.template.VertxSqlTemplate;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.boot.autoconfigure.ConfigurationCustomizer;
import org.mybatis.spring.boot.autoconfigure.MybatisProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.InputStream;
import java.util.List;

/**
 * Auto-configuration for vertx-mybatis integration.
 * <p>
 * Activates when both {@link Pool} (vertx-sql-client) and
 * {@link SqlSessionFactory} (MyBatis) are on the classpath.
 * <p>
 * Standard MyBatis properties are read via {@link MybatisProperties} from
 * mybatis-spring-boot-autoconfigure ({@code mybatis.*}), the same way as
 * {@code mybatis-spring-boot-starter}. Vert.x-specific properties are in
 * {@link VertxMybatisProperties} ({@code vertx.mybatis.*}).
 * <p>
 * Note: mybatis-spring-boot's {@code MybatisAutoConfiguration} requires a
 * {@code DataSource} ({@code @ConditionalOnSingleCandidate(DataSource.class)}),
 * so it won't activate here. This class provides the equivalent beans
 * ({@code Configuration}, {@code SqlSessionFactory}, {@code SqlSessionTemplate})
 * using vertx-sql-client as the underlying SQL engine.
 * <p>
 * Users can further customize the {@code Configuration} by registering
 * {@link ConfigurationCustomizer} beans — the standard mybatis-spring-boot
 * extension point.
 */
@AutoConfiguration(after = VertxAutoConfiguration.class)
@ConditionalOnClass({Pool.class, SqlSessionFactory.class})
@EnableConfigurationProperties({MybatisProperties.class, VertxMybatisProperties.class})
public class VertxMybatisAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(VertxMybatisAutoConfiguration.class);

    /**
     * Vert.x-specific defaults: enable underscore-to-camelCase mapping (common in Vert.x apps).
     * Users can override via {@code mybatis.configuration.map-underscore-to-camel-case=false}
     * or their own {@link ConfigurationCustomizer} bean.
     */
    @Bean
    @ConditionalOnMissingBean(name = "vertxDefaultCustomizer")
    public ConfigurationCustomizer vertxDefaultCustomizer() {
        return configuration -> {
            configuration.setMapUnderscoreToCamelCase(true);
            configuration.setCacheEnabled(true);
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public SqlResolver sqlResolver(Configuration mybatisConfiguration, VertxMybatisProperties properties) {
        SqlResolver.Dialect dialect;
        try {
            dialect = SqlResolver.Dialect.valueOf(properties.getDialect().toUpperCase());
        } catch (IllegalArgumentException e) {
            dialect = SqlResolver.Dialect.AUTO;
        }
        return new SqlResolver(mybatisConfiguration, dialect);
    }

    @Bean
    @ConditionalOnMissingBean
    public VertxRowMapper vertxRowMapper(Configuration mybatisConfiguration) {
        return new VertxRowMapper(mybatisConfiguration);
    }

    @Bean
    @ConditionalOnMissingBean(Configuration.class)
    public Configuration mybatisConfiguration(MybatisProperties properties,
                                              ObjectProvider<List<Interceptor>> interceptorsProvider,
                                              ObjectProvider<List<ConfigurationCustomizer>> customizersProvider,
                                              Pool pool) {
        Configuration configuration;

        // Load from mybatis-config.xml if specified (same as MybatisAutoConfiguration)
        if (properties.getConfigLocation() != null && !properties.getConfigLocation().isEmpty()) {
            try {
                InputStream is = Resources.getResourceAsStream(properties.getConfigLocation());
                XMLConfigBuilder parser = new XMLConfigBuilder(is);
                configuration = parser.parse();
                logger.info("Loaded MyBatis config from: {}", properties.getConfigLocation());
            } catch (Exception e) {
                throw new RuntimeException("Failed to load mybatis-config.xml: " + properties.getConfigLocation(), e);
            }
        } else {
            configuration = new Configuration();
            // Apply standard mybatis.configuration.* properties (same as MybatisAutoConfiguration)
            MybatisProperties.CoreConfiguration coreConfig = properties.getConfiguration();
            if (coreConfig != null) {
                coreConfig.applyTo(configuration);
            }
        }

        // Apply ConfigurationCustomizer beans (standard mybatis-spring-boot extension point)
        List<ConfigurationCustomizer> customizers = customizersProvider.getIfAvailable();
        if (customizers != null) {
            for (ConfigurationCustomizer customizer : customizers) {
                customizer.customize(configuration);
            }
        }

        // Set Environment (ManagedTransactionFactory — we manage transactions via VertxDataSourceTransactionManager)
        configuration.setEnvironment(new Environment("vertx",
                new org.apache.ibatis.transaction.managed.ManagedTransactionFactory(), null));

        // Register interceptors
        List<Interceptor> interceptors = interceptorsProvider.getIfAvailable();
        if (interceptors != null) {
            for (Interceptor interceptor : interceptors) {
                configuration.addInterceptor(interceptor);
                logger.info("Registered MyBatis interceptor: {}", interceptor.getClass().getName());
            }
        }

        // Scan type aliases (same as MybatisAutoConfiguration)
        if (properties.getTypeAliasesPackage() != null && !properties.getTypeAliasesPackage().isEmpty()) {
            configuration.getTypeAliasRegistry().registerAliases(properties.getTypeAliasesPackage());
            logger.info("Scanned type aliases from package: {}", properties.getTypeAliasesPackage());
        }

        // Scan type handlers (same as MybatisAutoConfiguration)
        if (properties.getTypeHandlersPackage() != null && !properties.getTypeHandlersPackage().isEmpty()) {
            configuration.getTypeHandlerRegistry().register(properties.getTypeHandlersPackage());
            logger.info("Scanned type handlers from package: {}", properties.getTypeHandlersPackage());
        }

        // Load mapper XML files using MybatisProperties.resolveMapperLocations()
        Resource[] mapperLocations = properties.resolveMapperLocations();
        if (mapperLocations != null) {
            for (Resource resource : mapperLocations) {
                try {
                    XMLMapperBuilder builder = new XMLMapperBuilder(
                            resource.getInputStream(), configuration,
                            resource.getURL().toString(), configuration.getSqlFragments());
                    builder.parse();
                    logger.debug("Loaded mapper XML: {}", resource.getURL());
                } catch (Exception e) {
                    logger.warn("Failed to load mapper XML: {}", resource, e);
                }
            }
        }

        return configuration;
    }

    @Bean
    @ConditionalOnMissingBean(SqlSessionFactory.class)
    public SqlSessionFactory sqlSessionFactory(Configuration mybatisConfiguration, Pool pool,
                                               SqlResolver sqlResolver, VertxRowMapper rowMapper) {
        logger.info("Creating VertxSqlSessionFactory");
        return new VertxSqlSessionFactory(mybatisConfiguration, pool, sqlResolver, rowMapper);
    }

    /**
     * Standard SqlSessionTemplate from mybatis-spring — same bean as MybatisAutoConfiguration
     * would create if a DataSource were present.
     */
    @Bean
    @ConditionalOnMissingBean
    public SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory sqlSessionFactory,
                                                 MybatisProperties properties) {
        ExecutorType executorType = properties.getExecutorType();
        if (executorType != null) {
            return new SqlSessionTemplate(sqlSessionFactory, executorType);
        }
        return new SqlSessionTemplate(sqlSessionFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public VertxSqlSessionManager vertxSqlSessionManager(SqlSessionFactory sqlSessionFactory) {
        return new VertxSqlSessionManager(sqlSessionFactory);
    }

    @Bean
    @ConditionalOnMissingBean(PlatformTransactionManager.class)
    @ConditionalOnClass(PlatformTransactionManager.class)
    public VertxDataSourceTransactionManager vertxTransactionManager(Pool pool) {
        logger.info("Creating VertxDataSourceTransactionManager for @Transactional support");
        return new VertxDataSourceTransactionManager(pool);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(SqlTemplate.class)
    public VertxSqlTemplate vertxSqlTemplate(Pool pool) {
        logger.info("Creating VertxSqlTemplate for named-parameter SQL template support");
        return new VertxSqlTemplate(pool);
    }
}
