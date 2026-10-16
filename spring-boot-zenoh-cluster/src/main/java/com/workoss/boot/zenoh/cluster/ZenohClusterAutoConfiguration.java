package com.workoss.boot.zenoh.cluster;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the pure Spring Boot Zenoh cluster component.
 * <p>
 * Active by default when this module is on the classpath; disable with
 * {@code zenoh.cluster.enabled=false}. Unlike {@code vertx-spring-boot-cluster-zenoh} there is no
 * {@code @ConditionalOnClass} gate — nothing here needs Vert.x.
 * <p>
 * The node joins the cluster when the context finishes refreshing ({@link ZenohClusterManager} is a
 * {@link org.springframework.context.SmartLifecycle}) and leaves when the context closes.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "zenoh.cluster", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(ZenohClusterProperties.class)
public class ZenohClusterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ZenohClusterManager zenohClusterManager(ZenohClusterProperties properties) {
        return new ZenohClusterManager(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ZenohMessageBroker zenohMessageBroker(ZenohClusterManager clusterManager) {
        return clusterManager.getMessageBroker();
    }

    @Bean
    @ConditionalOnBean(ZenohClusterManager.class)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    public ZenohClusterHealthIndicator zenohClusterHealthIndicator(ZenohClusterManager clusterManager) {
        return new ZenohClusterHealthIndicator(clusterManager);
    }
}
