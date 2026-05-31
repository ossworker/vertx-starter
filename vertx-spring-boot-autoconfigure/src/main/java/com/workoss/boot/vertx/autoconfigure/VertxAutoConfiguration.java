package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.Vertx;
import io.vertx.core.VertxBuilder;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Auto-configuration for the core Vert.x instance.
 * <p>
 * For Zenoh-based clustering, this creates a non-clustered Vert.x instance
 * (local EventBus) and relies on ZenohEventBusBridge for cross-node communication.
 * This avoids the need for additional TCP ports used by Vert.x's built-in ClusteredEventBus.
 */
@AutoConfiguration
@ConditionalOnClass(Vertx.class)
@EnableConfigurationProperties(VertxProperties.class)
public class VertxAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(VertxAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public VertxOptions vertxOptions(VertxProperties properties,
                                     ObjectProvider<List<VertxOptionsCustomizer>> customizersProvider) {
        VertxOptions options = new VertxOptions();

        if (properties.getEventLoopPoolSize() > 0) {
            options.setEventLoopPoolSize(properties.getEventLoopPoolSize());
        }
        options.setWorkerPoolSize(properties.getWorkerPoolSize());
        options.setInternalBlockingPoolSize(properties.getInternalBlockingPoolSize());
        options.setPreferNativeTransport(properties.isPreferNativeTransport());

        // Apply customizers
        List<VertxOptionsCustomizer> customizers = customizersProvider.getIfAvailable();
        if (customizers != null) {
            for (VertxOptionsCustomizer customizer : customizers) {
                options = customizer.customize(options);
            }
        }

        return options;
    }

    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean
    public Vertx vertx(VertxOptions vertxOptions, VertxProperties properties,
                       ObjectProvider<ClusterManager> clusterManagerProvider) {
        VertxBuilder builder = Vertx.builder().with(vertxOptions);

        if (properties.getCluster().isEnabled()) {
            ClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
            if (clusterManager != null) {
                // Check if this is a Zenoh-based cluster manager
                boolean isZenohCluster = isZenohClusterManager(clusterManager);

                if (isZenohCluster) {
                    // For Zenoh: create non-clustered Vert.x with local EventBus
                    // Cross-node communication is handled by ZenohEventBusBridge
                    logger.info("Creating Vert.x instance with Zenoh cluster (local EventBus + Zenoh bridge)");
                    return builder.build();
                } else {
                    // For other cluster managers (Hazelcast, etc.): use built-in ClusteredEventBus
                    builder.withClusterManager(clusterManager);
                    logger.info("Creating clustered Vert.x instance with built-in ClusteredEventBus");
                    Vertx vertx = builder
                            .buildClustered()
                            .toCompletionStage()
                            .toCompletableFuture()
                            .join();
                    logger.info("Clustered Vert.x instance created successfully");
                    return vertx;
                }
            }
            logger.info("Creating standalone Vert.x instance (no ClusterManager available)");
            return builder.build();
        } else {
            logger.info("Creating standalone Vert.x instance");
            return builder.build();
        }
    }

    /**
     * Check if the cluster manager is Zenoh-based.
     */
    private boolean isZenohClusterManager(ClusterManager clusterManager) {
        return clusterManager.getClass().getName().contains("zenoh") ||
               clusterManager.getClass().getSimpleName().contains("Zenoh");
    }

    @Bean
    @ConditionalOnMissingBean
    public VertxLifecycleManager vertxLifecycleManager(Vertx vertx, VertxProperties properties,
                                                       ObjectProvider<ClusterManager> clusterManagerProvider) {
        ClusterManager clusterManager = clusterManagerProvider.getIfAvailable();
        return new VertxLifecycleManager(vertx, properties, clusterManager);
    }

    @Bean
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    public VertxHealthIndicator vertxHealthIndicator(Vertx vertx) {
        return new VertxHealthIndicator(vertx);
    }

    @Bean
    public EventBusListenerRegistry eventBusListenerRegistry(Vertx vertx) {
        return new EventBusListenerRegistry(vertx);
    }
}
