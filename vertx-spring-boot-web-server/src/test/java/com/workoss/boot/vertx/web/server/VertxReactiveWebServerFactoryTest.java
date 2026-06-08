package com.workoss.boot.vertx.web.server;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.server.Compression;
import org.springframework.http.server.reactive.HttpHandler;

import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class VertxReactiveWebServerFactoryTest {

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

    @Test
    void usesSpringBootPortProperty() {
        VertxHttpServerProperties props = new VertxHttpServerProperties();
        VertxReactiveWebServerFactory factory = new VertxReactiveWebServerFactory(vertx, props, new ServerProperties());
        factory.setPort(9999);

        HttpHandler handler = mock(HttpHandler.class);
        VertxWebServer webServer = (VertxWebServer) factory.getWebServer(handler);

        assertThat(factory.getPort()).isEqualTo(9999);
    }

    @Test
    void usesSpringBootAddress() throws Exception {
        VertxHttpServerProperties props = new VertxHttpServerProperties();
        VertxReactiveWebServerFactory factory = new VertxReactiveWebServerFactory(vertx, props,new ServerProperties());
        factory.setAddress(InetAddress.getByName("127.0.0.1"));

        assertThat(factory.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
    }

    @Test
    void usesSpringBootCompressionEnabled() {
        VertxHttpServerProperties props = new VertxHttpServerProperties();
        props.setCompressionLevel(3);
        VertxReactiveWebServerFactory factory = new VertxReactiveWebServerFactory(vertx, props,new ServerProperties());

        Compression compression = new Compression();
        compression.setEnabled(true);
        factory.setCompression(compression);

        assertThat(factory.getCompression().getEnabled()).isTrue();
        assertThat(props.getCompressionLevel()).isEqualTo(3);
    }

    @Test
    void defaultVertxProperties() {
        VertxHttpServerProperties props = new VertxHttpServerProperties();
        assertThat(props.getIdleTimeout()).isEqualTo(60);
        assertThat(props.getMaxHeaderSize()).isEqualTo(8192);
        assertThat(props.getMaxChunkSize()).isEqualTo(8192);
        assertThat(props.getMaxInitialLineLength()).isEqualTo(4096);
        assertThat(props.getAcceptBacklog()).isEqualTo(1024);
        assertThat(props.getCompressionLevel()).isEqualTo(6);
    }

    @Test
    void webServerCanBeCreated() {
        VertxHttpServerProperties props = new VertxHttpServerProperties();
        VertxReactiveWebServerFactory factory = new VertxReactiveWebServerFactory(vertx, props,new ServerProperties());

        HttpHandler handler = mock(HttpHandler.class);
        VertxWebServer webServer = (VertxWebServer) factory.getWebServer(handler);
        assertThat(webServer).isNotNull();
    }
}
