package com.workoss.boot.vertx.web.server.websocket;

import com.workoss.boot.vertx.web.server.VertxWebServerAutoConfiguration;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.autoconfigure.web.reactive.HttpHandlerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.WebFluxAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.web.reactive.server.ConfigurableReactiveWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.server.WebServerFactoryCustomizerBeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Heartbeat + idle-reclamation coverage for {@link VertxWebSocketSession}.
 * <p>
 * Properties are deliberately short so a full reclaim cycle fits in the test window:
 * {@code vertx.websocket.ping-interval=1} and {@code vertx.websocket.idle-timeout=3}.
 */
class WebSocketHeartbeatIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static Vertx vertx;
    private static ConfigurableApplicationContext context;
    private static int port;
    private static HttpClient httpClient;

    @BeforeAll
    static void startServer() {
        vertx = Vertx.vertx();
        context = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.REACTIVE)
                .properties("vertx.websocket.ping-interval=1", "vertx.websocket.idle-timeout=3")
                .run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    /**
     * The server writes a PING every second and a healthy JDK client auto-replies PONG, which resets
     * the idle timer. So an idle connection must survive well past {@code idle-timeout=3}; a message
     * sent after 5s of silence still round-trips through the echo handler.
     */
    @Test
    void heartbeatKeepsAnIdleConnectionAlive() throws Exception {
        Conn conn = connect("/ws/echo");
        Thread.sleep(5000);
        conn.socket.sendText("still-here", true).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(conn.listener.nextText()).isEqualTo("still-here");
        conn.socket.sendClose(WebSocket.NORMAL_CLOSURE, "").get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        conn.close();
    }

    /**
     * A raw TCP socket that completes the handshake but never answers PING with PONG must be reclaimed
     * by the server once {@code idle-timeout=3} elapses with no inbound frame. The read loop sees EOF
     * (the server tore the TCP connection down) within the window.
     */
    @Test
    void deadClientWithNoPongIsReclaimedAfterIdleTimeout() throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout(30000);
            OutputStream out = socket.getOutputStream();
            String key = Base64.getEncoder().encodeToString("0123456789abcdef".getBytes(StandardCharsets.US_ASCII));
            out.write(("GET /ws/echo HTTP/1.1\r\n"
                    + "Host: localhost:" + port + "\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: " + key + "\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.flush();

            InputStream in = socket.getInputStream();
            readUntilHeaderEnd(in); // consume the 101 response head; upgrade is now complete

            // The silent peer never answers PING with PONG, so after idle-timeout the server must
            // send a WebSocket close frame (and eventually drop the TCP connection). Either is proof
            // of reclamation. We read frames until we see the close opcode (0x8) or EOF.
            long deadline = System.currentTimeMillis() + 20000;
            boolean reclaimed = false;
            while (System.currentTimeMillis() < deadline) {
                int opcode = readFrameOpcode(in);
                if (opcode == -1 || opcode == OP_CLOSE) {
                    reclaimed = true;
                    break;
                }
            }
            assertThat(reclaimed)
                    .as("server reclaimed the silent peer after idle-timeout elapsed")
                    .isTrue();
        }
    }

    private static Conn connect(String path) throws Exception {
        SimpleListener listener = new SimpleListener();
        WebSocket socket = httpClient.newWebSocketBuilder()
                .buildAsync(URI.create("ws://localhost:" + port + path), listener)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return new Conn(socket, listener);
    }

    private static void readUntilHeaderEnd(InputStream in) throws Exception {
        int window = 0;
        int b;
        while ((b = in.read()) != -1) {
            window = ((window << 8) | (b & 0xFF)) & 0x7FFF_FFFF;
            if (window == 0x0D0A0D0A) {
                return;
            }
        }
        throw new IllegalStateException("connection closed before the handshake reply completed");
    }

    /** WebSocket close frame opcode. */
    private static final int OP_CLOSE = 0x8;

    /**
     * Reads a single server-to-client (unmasked) WebSocket frame and returns its opcode, skipping
     * the payload. Returns {@code -1} on EOF. Control frames (PING 0x9, CLOSE 0x8) are what the
     * server sends here, so this is enough to detect reclamation without a full parser.
     */
    private static int readFrameOpcode(InputStream in) throws Exception {
        int b0 = in.read();
        if (b0 == -1) {
            return -1;
        }
        int opcode = b0 & 0x0F;
        int b1 = in.read();
        if (b1 == -1) {
            return -1;
        }
        int len = b1 & 0x7F;
        if (len == 126) {
            in.read();
            in.read();
        } else if (len == 127) {
            for (int i = 0; i < 8; i++) {
                in.read();
            }
        }
        for (int i = 0; i < len; i++) {
            if (in.read() == -1) {
                return -1;
            }
        }
        return opcode;
    }

    private static final class Conn implements AutoCloseable {
        final WebSocket socket;
        final SimpleListener listener;

        Conn(WebSocket socket, SimpleListener listener) {
            this.socket = socket;
            this.listener = listener;
        }

        @Override
        public void close() {
            socket.abort();
        }
    }

    /** Buffers text frames so a test can pull them in order. */
    private static final class SimpleListener implements WebSocket.Listener {
        private final BlockingQueue<String> texts = new LinkedBlockingQueue<>();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(Long.MAX_VALUE);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if (last) {
                texts.add(data.toString());
            }
            return null;
        }

        String nextText() throws InterruptedException {
            String t = texts.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (t == null) {
                throw new AssertionError("no text frame within " + TIMEOUT);
            }
            return t;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({
            WebFluxAutoConfiguration.class,
            HttpHandlerAutoConfiguration.class,
            VertxWebServerAutoConfiguration.class,
            VertxWebSocketAutoConfiguration.class
    })
    @EnableConfigurationProperties(ServerProperties.class)
    static class TestApplication {

        @Bean
        static WebServerFactoryCustomizerBeanPostProcessor webServerFactoryCustomizerBeanPostProcessor() {
            return new WebServerFactoryCustomizerBeanPostProcessor();
        }

        @Bean
        WebServerFactoryCustomizer<ConfigurableReactiveWebServerFactory> ephemeralPort() {
            return factory -> factory.setPort(0);
        }

        @Bean
        Vertx vertx() {
            return vertx;
        }

        @Bean
        SimpleUrlHandlerMapping webSocketHandlerMapping() {
            Map<String, Object> handlers = new LinkedHashMap<>();
            handlers.put("/ws/echo", echoHandler());
            return new SimpleUrlHandlerMapping(handlers, -1);
        }

        private static WebSocketHandler echoHandler() {
            return (WebSocketSession session) -> session.send(
                    session.receive().map(message -> session.textMessage(message.getPayloadAsText())));
        }
    }
}
