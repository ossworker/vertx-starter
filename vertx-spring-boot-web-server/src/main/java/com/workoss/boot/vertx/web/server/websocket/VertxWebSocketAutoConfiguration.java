package com.workoss.boot.vertx.web.server.websocket;

import com.workoss.boot.vertx.web.server.VertxWebServerAutoConfiguration;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.socket.server.RequestUpgradeStrategy;
import org.springframework.web.reactive.socket.server.WebSocketService;

/**
 * Auto-configuration for WebSocket support on the Vert.x HTTP server.
 * <p>
 * Users declare endpoints with plain WebFlux building blocks: a {@link
 * org.springframework.web.reactive.socket.WebSocketHandler} bean plus a {@code HandlerMapping}
 * that routes a path to it. Everything else (handshake validation, sub-protocol negotiation,
 * session attributes) is handled by Spring's {@code HandshakeWebSocketService}.
 */
@AutoConfiguration(after = VertxWebServerAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnClass({HttpServer.class, WebSocketService.class, RequestUpgradeStrategy.class})
@ConditionalOnProperty(prefix = "vertx.websocket", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(VertxWebSocketProperties.class)
public class VertxWebSocketAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RequestUpgradeStrategy.class)
    public VertxRequestUpgradeStrategy vertxRequestUpgradeStrategy(Vertx vertx, VertxWebSocketProperties properties) {
        return new VertxRequestUpgradeStrategy(vertx, properties);
    }

    @Bean
    public VertxWebFluxConfigurer vertxWebFluxConfigurer(RequestUpgradeStrategy upgradeStrategy) {
        return new VertxWebFluxConfigurer(upgradeStrategy);
    }

    /**
     * Cluster-wide fan-out for WebSocket messages. The consumer is registered here so a
     * {@code broadcast} from any node — including a peer that learned this address through the
     * clustered event bus — reaches the sessions this node owns.
     * <p>
     * With a non-clustered Vert.x instance this degrades to a plain local broadcaster, so the same
     * code path serves both deployment modes.
     */
    @Bean
    @ConditionalOnMissingBean
    public ClusterWebSocketBroadcaster clusterWebSocketBroadcaster(Vertx vertx,
                                                                   VertxWebSocketProperties properties) {
        ClusterWebSocketBroadcaster broadcaster =
                new ClusterWebSocketBroadcaster(vertx, properties.getBroadcastAddress());
        broadcaster.registerListener();
        return broadcaster;
    }
}
