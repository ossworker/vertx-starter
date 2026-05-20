package com.workoss.boot.vertx.web.server;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.http.server.reactive.HttpHandler;

/**
 * Auto-configuration for Vert.x reactive web server.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnClass({HttpServer.class, HttpHandler.class})
@ConditionalOnMissingBean(ReactiveWebServerFactory.class)
@EnableConfigurationProperties(VertxHttpServerProperties.class)
public class VertxWebServerAutoConfiguration {

    @Bean
    public VertxReactiveWebServerFactory vertxReactiveWebServerFactory(
            Vertx vertx,
            VertxHttpServerProperties serverProperties) {
        return new VertxReactiveWebServerFactory(vertx, serverProperties);
    }
}
