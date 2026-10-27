package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class VertxAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(VertxAutoConfiguration.class));

    @Test
    void defaultBeansCreated() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(Vertx.class);
            assertThat(context).hasSingleBean(VertxOptions.class);
            assertThat(context).hasSingleBean(VertxLifecycleManager.class);
        });
    }

    @Test
    void vertxOptionsUsesDefaultProperties() {
        contextRunner.run(context -> {
            VertxOptions options = context.getBean(VertxOptions.class);
            assertThat(options.getWorkerPoolSize()).isEqualTo(20);
            assertThat(options.getPreferNativeTransport()).isFalse();
        });
    }

    @Test
    void vertxOptionsCustomizedByProperties() {
        contextRunner
                .withPropertyValues(
                        "vertx.worker-pool-size=50",
                        "vertx.event-loop-pool-size=4",
                        "vertx.prefer-native-transport=false"
                )
                .run(context -> {
                    VertxOptions options = context.getBean(VertxOptions.class);
                    assertThat(options.getWorkerPoolSize()).isEqualTo(50);
                    assertThat(options.getEventLoopPoolSize()).isEqualTo(4);
                    assertThat(options.getPreferNativeTransport()).isFalse();
                });
    }

    @Test
    void vertxOptionsCustomizerApplied() {
        contextRunner
                .withBean(VertxOptionsCustomizer.class, () -> options -> {
                    options.setWorkerPoolSize(99);
                    return options;
                })
                .run(context -> {
                    VertxOptions options = context.getBean(VertxOptions.class);
                    assertThat(options.getWorkerPoolSize()).isEqualTo(99);
                });
    }

    @Test
    void existingVertxBeanNotOverridden() {
        Vertx existingVertx = Vertx.vertx();
        try {
            contextRunner
                    .withBean(Vertx.class, () -> existingVertx)
                    .run(context -> {
                        assertThat(context).hasSingleBean(Vertx.class);
                        assertThat(context.getBean(Vertx.class)).isSameAs(existingVertx);
                    });
        } finally {
            existingVertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Test
    void propertiesBinding() {
        contextRunner
                .withPropertyValues(
                        "vertx.cluster.enabled=false",
                        "vertx.cluster.host=127.0.0.1",
                        "vertx.cluster.port=5701",
                        "vertx.virtual-threads.enabled=true"
                )
                .run(context -> {
                    VertxProperties props = context.getBean(VertxProperties.class);
                    assertThat(props.getCluster().isEnabled()).isFalse();
                    assertThat(props.getCluster().getHost()).isEqualTo("127.0.0.1");
                    assertThat(props.getCluster().getPort()).isEqualTo(5701);
                    assertThat(props.getVirtualThreads().isEnabled()).isTrue();
                });
    }

    /**
     * The clustered event bus advertises its bind host verbatim as {@code NodeInfo}, so a wildcard
     * must never reach that field — peers would try to dial {@code 0.0.0.0}.
     */
    @Test
    void wildcardBindHostIsNotAdvertised() {
        VertxProperties properties = new VertxProperties();
        properties.getCluster().setHost("0.0.0.0");
        properties.getCluster().setPublicHost("10.0.0.7");

        VertxOptions options = new VertxOptions();
        new VertxClusterAutoConfiguration().clusterManagerCustomizer(null, properties).customize(options);

        assertThat(options.getEventBusOptions().getHost()).isNotEqualTo("0.0.0.0");
        assertThat(options.getEventBusOptions().getClusterPublicHost()).isEqualTo("10.0.0.7");
    }

    @Test
    void concreteBindHostIsApplied() {
        VertxProperties properties = new VertxProperties();
        properties.getCluster().setHost("127.0.0.1");
        properties.getCluster().setPort(5701);

        VertxOptions options = new VertxOptions();
        new VertxClusterAutoConfiguration().clusterManagerCustomizer(null, properties).customize(options);

        assertThat(options.getEventBusOptions().getHost()).isEqualTo("127.0.0.1");
        assertThat(options.getEventBusOptions().getPort()).isEqualTo(5701);
    }
}
