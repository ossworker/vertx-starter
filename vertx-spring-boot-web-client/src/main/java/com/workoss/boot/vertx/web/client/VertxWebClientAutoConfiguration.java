package com.workoss.boot.vertx.web.client;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.reactive.ClientHttpConnector;

/**
 * Auto-configuration for Vert.x-backed HTTP clients.
 */
@AutoConfiguration
@ConditionalOnClass({HttpClient.class})
@EnableConfigurationProperties(VertxHttpClientProperties.class)
public class VertxWebClientAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ClientHttpRequestFactory.class)
    public VertxClientHttpRequestFactory vertxClientHttpRequestFactory(
            Vertx vertx,
            VertxHttpClientProperties properties) {
        return new VertxClientHttpRequestFactory(vertx, properties);
    }

    @Bean
    @ConditionalOnMissingBean(ClientHttpConnector.class)
    @ConditionalOnClass(name = "org.springframework.http.client.reactive.ClientHttpConnector")
    public VertxClientHttpConnector vertxClientHttpConnector(
            Vertx vertx,
            VertxHttpClientProperties properties) {
        return new VertxClientHttpConnector(vertx, properties);
    }
}
