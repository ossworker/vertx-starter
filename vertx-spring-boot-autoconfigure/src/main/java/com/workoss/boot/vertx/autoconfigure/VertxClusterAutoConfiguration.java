package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.spi.cluster.ClusterManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration that wires the ClusterManager into VertxOptions when clustering is enabled.
 * In Vert.x 5, clustering is configured via VertxBuilder, not EventBusOptions.
 */
@AutoConfiguration(before = VertxAutoConfiguration.class)
@ConditionalOnProperty(prefix = "vertx.cluster", name = "enabled", havingValue = "true")
@ConditionalOnBean(ClusterManager.class)
public class VertxClusterAutoConfiguration {

    @Bean
    public VertxOptionsCustomizer clusterManagerCustomizer(ClusterManager clusterManager,
                                                           VertxProperties properties) {
        return options -> {
            // In Vert.x 5, clustering is handled by VertxBuilder.withClusterManager()
            // The EventBus host/port can still be set on EventBusOptions
            if (properties.getCluster().getHost() != null) {
                options.getEventBusOptions().setHost(properties.getCluster().getHost());
            }
            if (properties.getCluster().getPort() > 0) {
                options.getEventBusOptions().setPort(properties.getCluster().getPort());
            }
            return options;
        };
    }
}
