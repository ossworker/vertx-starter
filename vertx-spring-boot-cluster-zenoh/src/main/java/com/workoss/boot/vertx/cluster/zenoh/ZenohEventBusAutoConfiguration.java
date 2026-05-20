package com.workoss.boot.vertx.cluster.zenoh;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Zenoh-based EventBus bridge.
 * <p>
 * This configuration creates a {@link ZenohEventBusBridge} that routes EventBus
 * publish operations through Zenoh for cross-node communication.
 */
@AutoConfiguration
@ConditionalOnBean(ZenohClusterManager.class)
@ConditionalOnProperty(name = "vertx.cluster.enabled", havingValue = "true")
public class ZenohEventBusAutoConfiguration {

    /**
     * Creates the EventBus bridge that routes messages through Zenoh.
     */
    @Bean
    public ZenohEventBusBridge zenohEventBusBridge(ZenohClusterManager clusterManager, io.vertx.core.Vertx vertx) {
        return new ZenohEventBusBridge(
                vertx.eventBus(),
                clusterManager
        );
    }
}
