package com.workoss.boot.vertx.web.server.websocket;

import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.socket.server.RequestUpgradeStrategy;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;

/**
 * Plugs the Vert.x {@link RequestUpgradeStrategy} into Spring WebFlux.
 * <p>
 * {@code WebFluxConfigurer#getWebSocketService()} is the framework's seam for containers it
 * cannot auto-detect. {@code DelegatingWebFluxConfiguration} prefers this value when building
 * its {@code webFluxWebSocketHandlerAdapter} bean, so no bean overriding is involved. Without
 * it, {@code HandshakeWebSocketService.initUpgradeStrategy()} finds no known container and the
 * handshake fails.
 */
public class VertxWebFluxConfigurer implements WebFluxConfigurer {

    private final RequestUpgradeStrategy upgradeStrategy;

    public VertxWebFluxConfigurer(RequestUpgradeStrategy upgradeStrategy) {
        this.upgradeStrategy = upgradeStrategy;
    }

    @Override
    public WebSocketService getWebSocketService() {
        return new HandshakeWebSocketService(this.upgradeStrategy);
    }
}
