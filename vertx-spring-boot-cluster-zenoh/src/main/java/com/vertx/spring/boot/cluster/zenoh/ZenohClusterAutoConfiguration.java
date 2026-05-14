package com.vertx.spring.boot.cluster.zenoh;

import io.vertx.core.spi.cluster.ClusterManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Zenoh-based Vert.x ClusterManager.
 * Only activates when vertx.cluster.enabled=true.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "vertx.cluster", name = "enabled", havingValue = "true")
@ConditionalOnClass(ClusterManager.class)
@EnableConfigurationProperties(ZenohClusterProperties.class)
public class ZenohClusterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ClusterManager.class)
    public ZenohClusterManager zenohClusterManager(ZenohClusterProperties properties) {
        return new ZenohClusterManager(properties);
    }
}
