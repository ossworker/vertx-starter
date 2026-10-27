package com.workoss.boot.vertx.data.dict;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoss.boot.vertx.cluster.zenoh.ZenohClusterManager;
import com.workoss.boot.vertx.cluster.zenoh.ZenohClusterProperties;
import com.workoss.boot.vertx.data.dict.cache.CaffeineDictCache;
import com.workoss.boot.vertx.data.dict.cache.DictCache;
import com.workoss.boot.vertx.data.dict.cache.DictCacheManager;
import com.workoss.boot.vertx.data.dict.cache.ZenohDictCache;
import com.workoss.boot.vertx.data.dict.cache.spring.CaffeineZenohCacheManager;
import com.workoss.boot.vertx.data.dict.cache.spring.ZenohCacheManager;
import com.workoss.boot.vertx.data.dict.repository.DictDataProvider;
import com.workoss.boot.vertx.data.dict.service.DictService;
import com.workoss.zenoh.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for dictionary data with two-level cache.
 * <p>
 * Provides:
 * <ul>
 *   <li>L1 Cache: Caffeine (local in-memory)</li>
 *   <li>L2 Cache: Zenoh (distributed, backed by RocksDB via Zenoh Router)</li>
 *   <li>Lazy loading from database via {@link DictDataProvider}</li>
 *   <li>Tree structure support</li>
 * </ul>
 * <p>
 * Zenoh keys follow the isolation pattern: zenoh/{app}/{env}/dict/...
 * ensuring multi-tenant separation across applications and environments.
 */
@AutoConfiguration
@ConditionalOnClass(com.github.benmanes.caffeine.cache.Cache.class)
@EnableConfigurationProperties(DictCacheProperties.class)
@EnableCaching
@ConditionalOnProperty(prefix = "vertx.data.dict", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DictDataAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(DictDataAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(DictCache.class)
    public CaffeineDictCache caffeineDictCache(DictCacheProperties properties) {
        return new CaffeineDictCache(properties.getL1MaxSize(), properties.getL1ExpireSeconds());
    }

    @Bean
    @ConditionalOnMissingBean(DictCacheManager.class)
    public DictCacheManager dictCacheManager(
            CaffeineDictCache caffeineDictCache,
            ObjectProvider<DictCache> l2CacheProvider,
            ObjectProvider<ZenohClusterManager> zenohClusterManagerProvider,
            DictCacheProperties properties,
            ObjectProvider<ZenohClusterProperties> zenohPropertiesProvider) {

        // Apply defaults from Zenoh cluster properties for consistent isolation
        applyZenohDefaults(properties, zenohPropertiesProvider);
        String zenohPrefix = properties.resolveZenohPrefix();

        DictCache l2Cache = null;
        Session zenohSession = resolveZenohSession(zenohClusterManagerProvider);
        if (zenohSession != null) {
            try {
                l2Cache = new ZenohDictCache(zenohSession, zenohPrefix, new ObjectMapper());
                logger.info("L2 Zenoh cache initialized with keyPrefix='{}'", zenohPrefix);
            } catch (Exception e) {
                logger.warn("Failed to initialize L2 Zenoh cache, falling back to L1 only: {}", e.getMessage());
            }
        }

        return new DictCacheManager(caffeineDictCache, l2Cache);
    }

    @Bean
    @ConditionalOnMissingBean
    public DictService dictService(DictCacheManager cacheManager, ObjectProvider<DictDataProvider> dataProvider) {
        DictDataProvider provider = dataProvider.getIfAvailable();
        return new DictService(cacheManager, provider);
    }

    /**
     * Spring {@link CacheManager} bean combining Caffeine (L1) and Zenoh (L2).
     * <p>
     * When Zenoh cluster manager is available and active, a two-level cache is used.
     * Otherwise, falls back to Caffeine only.
     */
    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    @ConditionalOnClass(name = "org.springframework.cache.CacheManager")
    public CacheManager cacheManager(
            DictCacheProperties properties,
            ApplicationContext applicationContext,
            ObjectProvider<ZenohClusterManager> zenohClusterManagerProvider,
            ObjectProvider<ZenohClusterProperties> zenohPropertiesProvider,
            ObjectProvider<ObjectMapper> objectMapperProvider) {

        // Ensure isolation defaults are applied
        applyZenohDefaults(properties, zenohPropertiesProvider);

        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCacheSpecification(
                "maximumSize=" + properties.getL1MaxSize());

        // Try to integrate with Zenoh cluster manager for L2 cache
        ZenohCacheManager zenohCacheManager = createZenohCacheManager(
                properties, applicationContext, zenohClusterManagerProvider,objectMapperProvider);
        if (zenohCacheManager != null) {
            logger.info("Registering two-level CacheManager (Caffeine + Zenoh)");
            return new CaffeineZenohCacheManager(caffeineCacheManager, zenohCacheManager);
        }

        logger.info("Registering Caffeine CacheManager (Zenoh not available)");
        return caffeineCacheManager;
    }

    private ZenohCacheManager createZenohCacheManager(
            DictCacheProperties properties,
            ApplicationContext ctx,
            ObjectProvider<ZenohClusterManager> zenohClusterManagerProvider,
            ObjectProvider<ObjectMapper> objectMapperProvider) {

        ZenohClusterManager clusterManager = zenohClusterManagerProvider.getIfAvailable();
        if (clusterManager == null) {
            return null;
        }
        // Session is obtained lazily via supplier to handle initialization ordering
        return new ZenohCacheManager(
                clusterManager::getZenohSession,
                properties.resolveZenohPrefix(),
                objectMapperProvider.getIfAvailable(ObjectMapper::new)
        );
    }

    private Session resolveZenohSession(ObjectProvider<ZenohClusterManager> zenohClusterManagerProvider) {
        ZenohClusterManager clusterManager = zenohClusterManagerProvider.getIfAvailable();
        return clusterManager != null ? clusterManager.getZenohSession() : null;
    }

    /**
     * Apply app-name and environment defaults from Zenoh cluster properties
     * to ensure dict cache keys share the same isolation prefix as cluster data.
     */
    private void applyZenohDefaults(
            DictCacheProperties dictProps,
            ObjectProvider<ZenohClusterProperties> zenohPropsProvider) {

        ZenohClusterProperties zenohProps = zenohPropsProvider.getIfAvailable();
        if (zenohProps == null) {
            return;
        }
        if (dictProps.getAppName() == null) {
            dictProps.setAppName(zenohProps.getAppName());
        }
        if (dictProps.getEnvironment() == null) {
            dictProps.setEnvironment(zenohProps.getEnvironment());
        }
    }
}
