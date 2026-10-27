package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.internal.ClusterSettings;
import com.workoss.boot.zenoh.cluster.internal.DefaultZenohCluster;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport.Mode;
import com.workoss.zenoh.link.LinkProtocol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.env.MockEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Zenoh cluster auto-configuration")
class ZenohClusterAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ZenohClusterAutoConfiguration.class));

    @Test
    @DisplayName("router mode starts a running cluster with messaging and health beans")
    void routerMode() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        runner.withPropertyValues(
                        "zenoh.cluster.app-name=autoconfig",
                        "zenoh.cluster.node-id=node-1",
                        "zenoh.cluster.mode=router",
                        "zenoh.cluster.listen=tcp/127.0.0.1:" + port,
                        "zenoh.cluster.heartbeat-interval=300ms")
                .run(context -> {
                    assertThat(context).hasSingleBean(ZenohCluster.class);
                    assertThat(context).hasSingleBean(ClusterMessaging.class);
                    ZenohCluster cluster = context.getBean(ZenohCluster.class);
                    assertThat(((DefaultZenohCluster) cluster).isRunning()).isTrue();
                    assertThat(cluster.nodeId()).isEqualTo("node-1");
                    assertThat(cluster.members()).extracting("nodeId").containsExactly("node-1");
                    assertThat(context.getBean(ClusterMessaging.class)).isSameAs(cluster.messaging());

                    Health health = context.getBean(ZenohClusterHealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.UP);
                    assertThat(health.getDetails()).containsEntry("memberCount", 1).containsKey("sessionId");
                });
    }

    @Test
    @DisplayName("zenoh.cluster.protocol switches a router listening on a bare address to WebSocket")
    void protocolPropertySwitchesTransport() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        runner.withPropertyValues(
                        "zenoh.cluster.app-name=autoconfig-ws",
                        "zenoh.cluster.node-id=node-ws",
                        "zenoh.cluster.mode=router",
                        "zenoh.cluster.protocol=ws",
                        "zenoh.cluster.listen=127.0.0.1:" + port,
                        "zenoh.cluster.heartbeat-interval=300ms")
                .run(context -> {
                    DefaultZenohCluster cluster = (DefaultZenohCluster) context.getBean(ZenohCluster.class);
                    assertThat(cluster.isRunning()).isTrue();
                    assertThat(cluster.members()).extracting("nodeId").containsExactly("node-ws");
                    // The router really listens over WebSocket: a plain HTTP upgrade gets answered.
                    try (Socket socket = new Socket("127.0.0.1", port)) {
                        socket.getOutputStream().write(("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                                + "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                                + "Sec-WebSocket-Version: 13\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                        String status = new BufferedReader(new InputStreamReader(socket.getInputStream(),
                                StandardCharsets.US_ASCII)).readLine();
                        assertThat(status).startsWith("HTTP/1.1 101");
                    }
                });
    }

    @Test
    @DisplayName("protocol binds with relaxed names, including dashed ones")
    void protocolBindsRelaxed() {
        ZenohClusterProperties properties = new Binder(new MapConfigurationPropertySource(
                Map.of("zenoh.cluster.protocol", "unixsock-stream")))
                .bind("zenoh.cluster", ZenohClusterProperties.class).get();
        assertThat(properties.getProtocol()).isEqualTo(LinkProtocol.UNIXSOCK_STREAM);
        assertThat(new ZenohClusterProperties().toSettings(new MockEnvironment()).transport().protocol())
                .isEqualTo(LinkProtocol.TCP);
    }

    @Test
    @DisplayName("enabled=false contributes no beans")
    void disabled() {
        runner.withPropertyValues("zenoh.cluster.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ZenohCluster.class);
                    assertThat(context).doesNotHaveBean(ClusterMessaging.class);
                    assertThat(context).doesNotHaveBean(ZenohClusterHealthIndicator.class);
                });
    }

    @Test
    @DisplayName("failure timeout must exceed the heartbeat interval")
    void invalidTimeouts() {
        runner.withPropertyValues("zenoh.cluster.heartbeat-interval=2s", "zenoh.cluster.failure-timeout=1s")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("appName defaults to spring.application.name and environment to the active profile")
    void defaultsFromSpringEnvironment() {
        MockEnvironment springEnvironment = new MockEnvironment()
                .withProperty("spring.application.name", "my-app");
        springEnvironment.setActiveProfiles("prod", "extra");
        ClusterSettings settings = new ZenohClusterProperties().toSettings(springEnvironment);
        assertThat(settings.appName()).isEqualTo("my-app");
        assertThat(settings.environment()).isEqualTo("prod");
    }

    @Test
    @DisplayName("without spring.application.name and profiles the defaults are 'default'")
    void defaultsWithoutSpringEnvironmentValues() {
        ClusterSettings settings = new ZenohClusterProperties().toSettings(new MockEnvironment());
        assertThat(settings.appName()).isEqualTo("default");
        assertThat(settings.environment()).isEqualTo("default");
    }

    @Test
    @DisplayName("explicit appName and environment win over the Spring defaults")
    void explicitValuesWin() {
        MockEnvironment springEnvironment = new MockEnvironment()
                .withProperty("spring.application.name", "my-app");
        springEnvironment.setActiveProfiles("prod");
        ZenohClusterProperties properties = new ZenohClusterProperties();
        properties.setAppName("explicit-app");
        properties.setEnvironment("explicit-env");
        ClusterSettings settings = properties.toSettings(springEnvironment);
        assertThat(settings.appName()).isEqualTo("explicit-app");
        assertThat(settings.environment()).isEqualTo("explicit-env");
    }

    @Test
    @DisplayName("tls convenience properties map to quoted zenoh config paths with slashes preserved")
    void tlsPropertiesMapToConfig() {
        ZenohClusterProperties properties = new ZenohClusterProperties();
        properties.getTls().setListenCertificate("/etc/zenoh/server-cert.pem");
        properties.getTls().setListenPrivateKey("/etc/zenoh/server-key.pem");
        properties.getTls().setRootCaCertificate("/etc/zenoh/ca.pem");
        properties.getTls().setEnableMtls(true);
        ClusterSettings settings = properties.toSettings(new MockEnvironment());
        Map<String, String> config = settings.transport().config();
        assertThat(config).containsEntry(
                "transport/link/tls/listen_certificate", "\"/etc/zenoh/server-cert.pem\"");
        assertThat(config).containsEntry(
                "transport/link/tls/listen_private_key", "\"/etc/zenoh/server-key.pem\"");
        assertThat(config).containsEntry(
                "transport/link/tls/root_ca_certificate", "\"/etc/zenoh/ca.pem\"");
        assertThat(config).containsEntry("transport/link/tls/enable_mtls", "true");
    }

    @Test
    @DisplayName("raw config map overrides tls convenience entries")
    void rawConfigOverridesTls() {
        ZenohClusterProperties properties = new ZenohClusterProperties();
        properties.getTls().setRootCaCertificate("/etc/zenoh/ca.pem");
        properties.setConfig(Map.of("transport/link/tls/root_ca_certificate", "\"/override/ca.pem\""));
        ClusterSettings settings = properties.toSettings(new MockEnvironment());
        assertThat(settings.transport().config())
                .containsEntry("transport/link/tls/root_ca_certificate", "\"/override/ca.pem\"");
    }

    @Test
    @DisplayName("scouting properties bind relaxed and map to zenoh config; autoconnect applies to peer and client")
    void scoutingPropertiesMapToConfig() {
        ZenohClusterProperties properties = new Binder(new MapConfigurationPropertySource(Map.of(
                "zenoh.cluster.scouting.multicast-enabled", "false",
                "zenoh.cluster.scouting.gossip-enabled", "true",
                "zenoh.cluster.scouting.gossip-multihop", "true",
                "zenoh.cluster.scouting.gossip-autoconnect", "router|peer")))
                .bind("zenoh.cluster", ZenohClusterProperties.class).get();
        Map<String, String> config = properties.toSettings(new MockEnvironment()).transport().config();
        assertThat(config)
                .containsEntry("scouting/multicast/enabled", "false")
                .containsEntry("scouting/gossip/enabled", "true")
                .containsEntry("scouting/gossip/multihop", "true")
                .containsEntry("scouting/gossip/autoconnect/peer", "\"router|peer\"")
                .containsEntry("scouting/gossip/autoconnect/client", "\"router|peer\"");
    }

    @Test
    @DisplayName("unset scouting values stay out of the config so zenoh defaults apply")
    void scoutingDefaultsUntouched() {
        ZenohClusterProperties properties = new ZenohClusterProperties();
        Map<String, String> config = properties.toSettings(new MockEnvironment()).transport().config();
        assertThat(config).doesNotContainKey("scouting/gossip/enabled")
                .doesNotContainKey("scouting/multicast/enabled")
                .doesNotContainKey("scouting/gossip/autoconnect/peer");
    }

    @Test
    @DisplayName("raw config map overrides scouting entries")
    void rawConfigOverridesScouting() {
        ZenohClusterProperties properties = new ZenohClusterProperties();
        properties.getScouting().setGossipEnabled(false);
        properties.setConfig(Map.of("scouting/gossip/enabled", "true"));
        ClusterSettings settings = properties.toSettings(new MockEnvironment());
        assertThat(settings.transport().config()).containsEntry("scouting/gossip/enabled", "true");
    }

    @Test
    void dnsPropertiesBindWithoutStaticConnect() {
        ZenohClusterProperties properties = new Binder(new MapConfigurationPropertySource(Map.of(
                "zenoh.cluster.listen", "10.0.0.1:7447",
                "zenoh.cluster.dns.name", "zenoh-peers.default.svc.cluster.local",
                "zenoh.cluster.dns.port", "7448",
                "zenoh.cluster.dns.refresh-interval", "3s",
                "zenoh.cluster.dns.self-address", "10.0.0.1",
                "zenoh.cluster.dns.timeout", "500ms")))
                .bind("zenoh.cluster", ZenohClusterProperties.class).get();
        ClusterSettings settings = properties.toSettings(new MockEnvironment());
        assertThat(settings.dns().enabled()).isTrue();
        assertThat(settings.dns().name()).isEqualTo("zenoh-peers.default.svc.cluster.local");
        assertThat(settings.dns().port()).isEqualTo(7448);
        assertThat(settings.dns().refreshMillis()).isEqualTo(3_000L);
        assertThat(settings.dns().selfAddress()).isEqualTo("10.0.0.1");
        assertThat(settings.dns().timeoutMillis()).isEqualTo(500L);
        assertThat(settings.transport().connect()).isEmpty();
        assertThat(new ZenohClusterProperties().toSettings(new MockEnvironment()).dns().enabled()).isFalse();
    }

    @Test
    void dnsRequiresPeerModeAndListener() {
        ZenohClusterProperties properties = new ZenohClusterProperties();
        properties.getDns().setName("zenoh-peers.default.svc.cluster.local");
        assertThatThrownBy(() -> properties.toSettings(new MockEnvironment()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("listen");
        properties.setListen(List.of("10.0.0.1:7447"));
        properties.setMode(Mode.CLIENT);
        assertThatThrownBy(() -> properties.toSettings(new MockEnvironment()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("mode=peer");
        properties.setMode(Mode.PEER);
        properties.setProtocol(LinkProtocol.UNIXSOCK_STREAM);
        assertThatThrownBy(() -> properties.toSettings(new MockEnvironment()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("IP link");
    }
}
