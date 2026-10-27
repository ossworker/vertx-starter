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
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.net.http.WebSocketHandshakeException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for Phase 1 of {@code docs/vertx-websocket-support-design.md}: T1 same-port
 * coexistence, T2 text/binary/fragmented echo, T3 the R1 async-filter regression, T4 handshake
 * wire format, T5 sub-protocol negotiation, T8 close semantics and T11 WebFilter short-circuit.
 */
class VertxWebSocketIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    /**
     * Deliberately long enough that Vert.x would have delivered the request end event on the event
     * loop long before the upgrade runs, which is what makes every handshake below a T3 assertion.
     */
    private static final Duration FILTER_DELAY = Duration.ofMillis(50);
    private static final CloseStatus SERVER_CLOSE_STATUS = new CloseStatus(4001, "custom-reason");
    private static final BlockingQueue<CloseStatus> SEEN_CLOSE_STATUSES = new LinkedBlockingQueue<>();

    private static Vertx vertx;
    private static ConfigurableApplicationContext context;
    private static int port;
    private static HttpClient httpClient;

    @BeforeAll
    static void startServer() {
        vertx = Vertx.vertx();
        context = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.REACTIVE)
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

    @Test
    void httpAndWebSocketShareTheListeningPort() throws Exception {
        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/ping")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("pong");

        try (Connection connection = connect("/ws/echo")) {
            connection.sendText("same-port");
            assertThat(connection.nextText()).isEqualTo("same-port");
        }
    }

    @Test
    void echoesTextAndBinaryMessages() throws Exception {
        try (Connection connection = connect("/ws/echo")) {
            connection.sendText("hello");
            connection.sendText("你好");
            assertThat(connection.nextText()).isEqualTo("hello");
            assertThat(connection.nextText()).isEqualTo("你好");
        }

        byte[] payload = new byte[4096];
        new Random(42).nextBytes(payload);
        try (Connection connection = connect("/ws/binary")) {
            connection.sendBinary(payload);
            assertThat(connection.nextBinary()).isEqualTo(payload);
        }
    }

    @Test
    void aggregatesFragmentedMessageThatCrossesTheFrameLimit() throws Exception {
        // Each part stays under vertx.websocket.max-frame-size (64 KiB); the aggregate does not, so
        // the message only arrives intact if Vert.x's frame aggregator reassembles the CONTINUATION
        // frames before the handler sees it.
        String head = "A".repeat(40_000);
        String tail = "B".repeat(40_000);

        try (Connection connection = connect("/ws/echo")) {
            connection.sendText(head, false);
            connection.sendText(tail, true);
            assertThat(connection.nextText()).isEqualTo(head + tail);
        }
    }

    @Test
    void handshakeReplyIsABareUpgradeWithNoHttpHeadersInjected() throws Exception {
        String request = "GET /ws/echo HTTP/1.1\r\n"
                + "Host: localhost:" + port + "\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + "\r\n";

        Map<String, String> headers;
        String statusLine;
        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout((int) TIMEOUT.toMillis());
            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String head = readResponseHead(socket.getInputStream());
            String[] lines = head.split("\r\n");
            statusLine = lines[0];
            headers = new LinkedHashMap<>();
            for (int i = 1; i < lines.length; i++) {
                int colon = lines[i].indexOf(':');
                if (colon > 0) {
                    headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT),
                            lines[i].substring(colon + 1).trim());
                }
            }
        }

        assertThat(statusLine).isEqualTo("HTTP/1.1 101 Switching Protocols");
        assertThat(headers).containsEntry("upgrade", "websocket");
        assertThat(headers.get("connection")).isEqualToIgnoringCase("upgrade");
        assertThat(headers).containsEntry("sec-websocket-accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
        // R9: the upgrade strategy never completes the Spring response, so VertxServerHttpResponse
        // .applyHeaders() must not have stamped the 101 reply with the plain-HTTP headers.
        assertThat(headers).doesNotContainKey("date");
        assertThat(headers).doesNotContainValue("keep-alive");
    }

    @Test
    void negotiatesSubProtocolDeclaredByTheHandlerOnly() throws Exception {
        // vertx.websocket.sub-protocols is left empty on purpose: Netty's handshaker then selects
        // nothing, and the Sec-WebSocket-Protocol the client sees is the one the upgrade strategy
        // put on the response headers from HandshakeWebSocketService's own negotiation.
        try (Connection connection = connect("/ws/sub", "v1.chat", "v0.legacy")) {
            assertThat(connection.socket.getSubprotocol()).isEqualTo("v1.chat");
            assertThat(connection.nextText()).isEqualTo("sub=v1.chat");

            connection.sendText("ping");
            assertThat(connection.nextText()).isEqualTo("ping");
        }
    }

    @Test
    void propagatesCloseInitiatedByTheServer() throws Exception {
        try (Connection connection = connect("/ws/server-close")) {
            connection.sendText("go");
            Closed closed = connection.nextClose();
            assertThat(closed.code()).isEqualTo(SERVER_CLOSE_STATUS.getCode());
            assertThat(closed.reason()).isEqualTo(SERVER_CLOSE_STATUS.getReason());
        }
    }

    @Test
    void completesReceiveAndPublishesCloseStatusWhenTheClientHangsUp() throws Exception {
        try (Connection connection = connect("/ws/close-watch")) {
            connection.socket.sendClose(CloseStatus.NORMAL.getCode(), "client-bye")
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            CloseStatus seen = SEEN_CLOSE_STATUSES.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(seen).isNotNull();
            assertThat(seen.getCode()).isEqualTo(CloseStatus.NORMAL.getCode());
            assertThat(seen.getReason()).isEqualTo("client-bye");
        }
    }

    @Test
    void webFilterCanRejectAHandshakeWithAnHttpStatus() throws Exception {
        Throwable failure = connectFailure("/ws/secret");

        assertThat(failure).isInstanceOf(WebSocketHandshakeException.class);
        HttpResponse<?> response = ((WebSocketHandshakeException) failure).getResponse();
        assertThat(response.statusCode()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
    }

    private static Connection connect(String path, String... subProtocols) throws Exception {
        WebSocket.Builder builder = httpClient.newWebSocketBuilder();
        if (subProtocols.length > 0) {
            builder.subprotocols(subProtocols[0], Arrays.copyOfRange(subProtocols, 1, subProtocols.length));
        }
        RecordingListener listener = new RecordingListener();
        WebSocket socket = builder
                .buildAsync(URI.create("ws://localhost:" + port + path), listener)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return new Connection(socket, listener);
    }

    private static Throwable connectFailure(String path) {
        try (Connection ignored = connect(path)) {
            return new AssertionError("handshake unexpectedly succeeded for " + path);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            return e;
        }
    }

    private static String readResponseHead(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int window = 0;
        int b;
        while ((b = in.read()) != -1) {
            head.write(b);
            window = ((window << 8) | (b & 0xFF)) & 0x7FFF_FFFF;
            if (window == 0x0D0A0D0A) {
                return head.toString(StandardCharsets.US_ASCII);
            }
        }
        throw new IOException("connection closed before the handshake reply was complete");
    }

    private record Closed(int code, String reason) {
    }

    /** Collects everything the peer sends so assertions can be made in order. */
    private static final class RecordingListener implements WebSocket.Listener {

        private final BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        private final StringBuilder text = new StringBuilder();
        private final ByteArrayOutputStream binary = new ByteArrayOutputStream();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(Long.MAX_VALUE);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            text.append(data);
            if (last) {
                events.add(text.toString());
                text.setLength(0);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            byte[] chunk = new byte[data.remaining()];
            data.get(chunk);
            binary.write(chunk, 0, chunk.length);
            if (last) {
                events.add(binary.toByteArray());
                binary.reset();
            }
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            events.add(new Closed(statusCode, reason));
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            events.add(error);
        }

        private Object next() {
            Object event;
            try {
                event = events.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for a WebSocket event", e);
            }
            if (event == null) {
                throw new AssertionError("no WebSocket event within " + TIMEOUT);
            }
            if (event instanceof Throwable throwable) {
                throw new AssertionError("WebSocket failed", throwable);
            }
            return event;
        }

        String nextText() {
            return (String) next();
        }

        byte[] nextBinary() {
            return (byte[]) next();
        }

        Closed nextClose() {
            return (Closed) next();
        }
    }

    private static final class Connection implements AutoCloseable {

        private final WebSocket socket;
        private final RecordingListener listener;

        Connection(WebSocket socket, RecordingListener listener) {
            this.socket = socket;
            this.listener = listener;
        }

        void sendText(String text) throws Exception {
            sendText(text, true);
        }

        void sendText(String text, boolean last) throws Exception {
            socket.sendText(text, last).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        void sendBinary(byte[] bytes) throws Exception {
            socket.sendBinary(ByteBuffer.wrap(bytes), true).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        String nextText() {
            return listener.nextText();
        }

        byte[] nextBinary() {
            return listener.nextBinary();
        }

        Closed nextClose() {
            return listener.nextClose();
        }

        @Override
        public void close() {
            socket.abort();
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
            handlers.put("/ws/echo", textEcho());
            handlers.put("/ws/binary", binaryEcho());
            handlers.put("/ws/sub", new SubProtocolEchoHandler());
            handlers.put("/ws/server-close", serverClose());
            handlers.put("/ws/close-watch", closeWatcher());
            return new SimpleUrlHandlerMapping(handlers, -1);
        }

        @Bean
        PingController pingController() {
            return new PingController();
        }

        @Bean
        WebFilter delayFilter() {
            return (exchange, chain) -> Mono.delay(FILTER_DELAY).then(chain.filter(exchange));
        }

        @Bean
        WebFilter secretGuardFilter() {
            return (exchange, chain) -> {
                if ("/ws/secret".equals(exchange.getRequest().getPath().value())) {
                    exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                    return exchange.getResponse().setComplete();
                }
                return chain.filter(exchange);
            };
        }

        private static WebSocketHandler textEcho() {
            return session -> session.send(
                    session.receive().map(message -> session.textMessage(message.getPayloadAsText())));
        }

        private static WebSocketHandler binaryEcho() {
            return session -> session.send(session.receive()
                    .map(message -> session.binaryMessage(factory -> factory.wrap(read(message.getPayload())))));
        }

        private static WebSocketHandler serverClose() {
            // flatMap rather than next().then(...): receive() must stay subscribed so the test also
            // covers the closeHandler completing receive(), which the handler chain depends on.
            return session -> session.receive()
                    .flatMap(message -> session.close(SERVER_CLOSE_STATUS))
                    .then();
        }

        private static WebSocketHandler closeWatcher() {
            return session -> session.receive()
                    .then(session.closeStatus())
                    .doOnNext(SEEN_CLOSE_STATUSES::offer)
                    .then();
        }

        private static byte[] read(DataBuffer payload) {
            byte[] bytes = new byte[payload.readableByteCount()];
            payload.read(bytes);
            return bytes;
        }
    }

    /** Echoes the server-side view of the negotiated sub-protocol first, then relays messages. */
    private static final class SubProtocolEchoHandler implements WebSocketHandler {

        @Override
        public List<String> getSubProtocols() {
            return List.of("v1.chat");
        }

        @Override
        public Mono<Void> handle(WebSocketSession session) {
            String negotiated = session.getHandshakeInfo().getSubProtocol();
            return session.send(Flux.concat(
                    Flux.just(session.textMessage("sub=" + negotiated)),
                    session.receive().map(message -> session.textMessage(message.getPayloadAsText()))));
        }
    }

    @RestController
    static class PingController {

        @GetMapping("/api/ping")
        String ping() {
            return "pong";
        }
    }
}
