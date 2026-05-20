package com.workoss.boot.vertx.web.server;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class VertxWebServerAutoConfigurationTest {

    private static Vertx vertx;

    @BeforeAll
    static void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    static void tearDown() {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    private final ReactiveWebApplicationContextRunner contextRunner = new ReactiveWebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(VertxWebServerAutoConfiguration.class))
            .withBean(Vertx.class, () -> vertx);

    @Test
    void autoConfigurationCreatesFactory() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ReactiveWebServerFactory.class);
            assertThat(context).hasSingleBean(VertxReactiveWebServerFactory.class);
        });
    }

    @Test
    void factoryBacksOffWhenAnotherExists() {
        contextRunner
                .withBean(ReactiveWebServerFactory.class, () -> httpHandler -> null)
                .run(context -> {
                    assertThat(context).hasSingleBean(ReactiveWebServerFactory.class);
                    assertThat(context).doesNotHaveBean(VertxReactiveWebServerFactory.class);
                });
    }

    @Test
    void vertxSpecificPropertiesBound() {
        contextRunner
                .withPropertyValues(
                        "vertx.http-server.idle-timeout=120",
                        "vertx.http-server.max-header-size=16384",
                        "vertx.http-server.compression-level=4",
                        "vertx.http-server.accept-backlog=2048"
                )
                .run(context -> {
                    VertxHttpServerProperties props = context.getBean(VertxHttpServerProperties.class);
                    assertThat(props.getIdleTimeout()).isEqualTo(120);
                    assertThat(props.getMaxHeaderSize()).isEqualTo(16384);
                    assertThat(props.getCompressionLevel()).isEqualTo(4);
                    assertThat(props.getAcceptBacklog()).isEqualTo(2048);
                });
    }

    @Test
    void factoryExtendsAbstractReactiveWebServerFactory() {
        contextRunner.run(context -> {
            VertxReactiveWebServerFactory factory = context.getBean(VertxReactiveWebServerFactory.class);
            // Verifies factory inherits standard server.* property binding support
            assertThat(factory).isInstanceOf(
                    org.springframework.boot.web.reactive.server.AbstractReactiveWebServerFactory.class);
        });
    }

    @Test
    void factorySupportsStandardPropertySetters() {
        contextRunner.run(context -> {
            VertxReactiveWebServerFactory factory = context.getBean(VertxReactiveWebServerFactory.class);
            factory.setPort(9090);
            assertThat(factory.getPort()).isEqualTo(9090);
        });
    }
}
