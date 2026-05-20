package com.workoss.boot.vertx.web.client;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.reactive.ClientHttpConnector;

import static org.assertj.core.api.Assertions.assertThat;

class VertxWebClientAutoConfigurationTest {

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

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(VertxWebClientAutoConfiguration.class))
            .withBean(Vertx.class, () -> vertx);

    @Test
    void autoConfigurationCreatesRequestFactory() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ClientHttpRequestFactory.class);
            assertThat(context).hasSingleBean(VertxClientHttpRequestFactory.class);
        });
    }

    @Test
    void autoConfigurationCreatesConnector() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ClientHttpConnector.class);
            assertThat(context).hasSingleBean(VertxClientHttpConnector.class);
        });
    }

    @Test
    void requestFactoryBacksOffWhenAnotherExists() {
        contextRunner
                .withBean(ClientHttpRequestFactory.class, () -> (uri, httpMethod) -> null)
                .run(context -> {
                    assertThat(context).hasSingleBean(ClientHttpRequestFactory.class);
                    assertThat(context).doesNotHaveBean(VertxClientHttpRequestFactory.class);
                });
    }

    @Test
    void propertiesBinding() {
        contextRunner
                .withPropertyValues(
                        "vertx.http-client.connect-timeout=10000",
                        "vertx.http-client.idle-timeout=120",
                        "vertx.http-client.keep-alive=true",
                        "vertx.http-client.pipelining=true"
                )
                .run(context -> {
                    VertxHttpClientProperties props = context.getBean(VertxHttpClientProperties.class);
                    assertThat(props.getConnectTimeout()).isEqualTo(10000);
                    assertThat(props.getIdleTimeout()).isEqualTo(120);
                    assertThat(props.isKeepAlive()).isTrue();
                    assertThat(props.isPipelining()).isTrue();
                });
    }
}
