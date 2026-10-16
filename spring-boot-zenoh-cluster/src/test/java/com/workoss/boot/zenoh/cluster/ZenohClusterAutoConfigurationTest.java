package com.workoss.boot.zenoh.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Auto-configuration wiring: enabled-by-default bean registration, the {@code enabled=false} opt-out,
 * and the actuator-gated health indicator.
 */
@DisplayName("ZenohClusterAutoConfiguration wiring")
class ZenohClusterAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class));

    @Test
    @DisplayName("enabled by default: manager, broker and health indicator beans are created")
    void enabledByDefault() throws Exception {
        int port = ZenohTestSupport.freePort();
        runner.withPropertyValues(
                        "zenoh.cluster.app-name=autoconfig",
                        "zenoh.cluster.mode=router",
                        "zenoh.cluster.listen=tcp/127.0.0.1:" + port,
                        "zenoh.cluster.heartbeat-interval=500")
                .run(context -> {
                    assertThat(context).hasBean("zenohClusterManager");
                    assertThat(context).hasBean("zenohMessageBroker");
                    assertThat(context).hasBean("zenohClusterHealthIndicator");
                    ZenohClusterManager manager = context.getBean(ZenohClusterManager.class);
                    assertThat(manager.getNodeId()).isNotBlank();
                    assertThat(manager.isRunning()).isTrue();
                });
    }

    @Test
    @DisplayName("zenoh.cluster.enabled=false: no cluster beans are created")
    void disabledOptOut() {
        runner.withPropertyValues("zenoh.cluster.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ZenohClusterManager.class);
                    assertThat(context).doesNotHaveBean(ZenohMessageBroker.class);
                    assertThat(context).doesNotHaveBean("zenohClusterHealthIndicator");
                });
    }
}
