package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.spi.cluster.ClusterManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration that wires the ClusterManager into VertxOptions when clustering is enabled.
 * In Vert.x 5, clustering is configured via VertxBuilder, not EventBusOptions.
 * <p>
 * The clustered event bus opens a <strong>second</strong> listener next to {@code server.port}:
 * {@code ClusteredEventBus} creates its own NetServer and publishes the bound port as its
 * {@code NodeInfo}, which peers then dial. That port is not configurable away — if it cannot be
 * reached between nodes, cross-node messaging cannot work.
 */
@AutoConfiguration(before = VertxAutoConfiguration.class)
@ConditionalOnProperty(prefix = "vertx.cluster", name = "enabled", havingValue = "true")
@ConditionalOnBean(ClusterManager.class)
public class VertxClusterAutoConfiguration {

    /**
     * Wildcard binds cannot be advertised to peers: they end up verbatim in {@code NodeInfo} and the
     * remote end would try to connect to {@code 0.0.0.0}.
     */
    private static final String WILDCARD_HOST = "0.0.0.0";

    @Bean
    public VertxOptionsCustomizer clusterManagerCustomizer(ClusterManager clusterManager,
                                                           VertxProperties properties) {
        return options -> {
            // In Vert.x 5, clustering is handled by VertxBuilder.withClusterManager()
            // The EventBus host/port can still be set on EventBusOptions
            VertxProperties.Cluster cluster = properties.getCluster();

            // publicHost is what peers dial; fall back to host only when host is a real address.
            String advertised = cluster.getPublicHost();
            if (advertised != null && !advertised.isBlank()) {
                options.getEventBusOptions().setClusterPublicHost(advertised);
            }

            if (cluster.getHost() != null && !WILDCARD_HOST.equals(cluster.getHost())) {
                options.getEventBusOptions().setHost(cluster.getHost());
            }
            if (cluster.getPort() > 0) {
                options.getEventBusOptions().setPort(cluster.getPort());
            }
            return options;
        };
    }
}
