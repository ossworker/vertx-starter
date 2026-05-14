package com.vertx.spring.boot.autoconfigure;

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
                builder.withClusterManager(clusterManager);
            }
            logger.info("Creating clustered Vert.x instance");
            Vertx vertx = builder
                    .buildClustered()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
            logger.info("Clustered Vert.x instance created successfully");
            return vertx;
        } else {
            logger.info("Creating standalone Vert.x instance");
            return builder.build();
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public VertxLifecycleManager vertxLifecycleManager(Vertx vertx) {
        return new VertxLifecycleManager(vertx);
    }
}
