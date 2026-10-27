package com.workoss.boot.vertx.cluster.zenoh;

import com.workoss.boot.vertx.web.server.VertxServerHttpRequest;
import com.workoss.boot.vertx.web.server.VertxServerHttpResponse;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import com.workoss.boot.vertx.web.server.websocket.VertxRequestUpgradeStrategy;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双节点 WebSocket 集群广播 E2E。
 * <p>
 * 覆盖单元测试碰不到的一环:一个节点 publish 的消息,能否真正落到<em>另一个节点</em>持有的那条
 * TCP 连接上。它依赖整条链路 —— 跨节点注册表 → {@code DefaultNodeSelector} 选路 → Netty 出站 →
 * 对端本地会话表 → socket 写入。任一环断掉,消息就静默地留在本机。
 * <p>
 * 每侧起一个真实的 Vert.x {@link HttpServer},WebSocket 与 HTTP 共用该节点的端口,客户端用 JDK 的
 * {@link java.net.http.WebSocket} 完成真实握手与 upgrade。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusteredWebSocketE2ETest {

    private static final String APP = "e2e-ws-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String PATH = "/ws/cluster";
    private static final long TIMEOUT_MS = 30_000L;

    private static Vertx vertxA;
    private static Vertx vertxB;
    private static HttpServer serverA;
    private static HttpServer serverB;
    private static ClusterWebSocketBroadcaster broadcasterA;
    private static ClusterWebSocketBroadcaster broadcasterB;
    private static int portA;
    private static int portB;

    @BeforeAll
    static void startCluster() throws Exception {
        int zenohPort = freePort();

        vertxA = clustered(peerProperties(zenohPort), freePort());
        Thread.sleep(500);
        vertxB = clustered(clientProperties(zenohPort), freePort());

        broadcasterA = new ClusterWebSocketBroadcaster(vertxA);
        broadcasterA.registerListener();
        broadcasterB = new ClusterWebSocketBroadcaster(vertxB);
        broadcasterB.registerListener();

        portA = startServer(vertxA, broadcasterA);
        portB = startServer(vertxB, broadcasterB);
    }

    private static ZenohClusterProperties peerProperties(int zenohPort) {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("test");
        props.setMode("peer");
        props.setListen(List.of("tcp/127.0.0.1:" + zenohPort));
        return props;
    }

    private static ZenohClusterProperties clientProperties(int zenohPort) {
        ZenohClusterProperties props = new ZenohClusterProperties();
        props.setAppName(APP);
        props.setEnvironment("test");
        props.setMode("client");
        props.setConnect(List.of("tcp/127.0.0.1:" + zenohPort));
        return props;
    }

    private static Vertx clustered(ZenohClusterProperties props, int eventBusPort) {
        VertxOptions options = new VertxOptions();
        options.getEventBusOptions().setHost("127.0.0.1").setPort(eventBusPort);
        return Vertx.builder().with(options).withClusterManager(new ZenohClusterManager(props))
                .buildClustered().toCompletionStage().toCompletableFuture().join();
    }

    /**
     * Serves WebSocket on the same listener that would serve HTTP routes, upgrading in place via
     * {@link VertxRequestUpgradeStrategy} — the production path, minus the Spring context.
     */
    private static int startServer(Vertx vertx, ClusterWebSocketBroadcaster broadcaster) throws Exception {
        WebSocketHandler handler = session -> {
            String group = groupOf(session);
            broadcaster.register(session, group);
            return session.receive()
                    .doOnNext(message -> broadcaster.broadcast("echo:" + message.getPayloadAsText(), group))
                    .doFinally(signal -> broadcaster.unregister(session.getId()))
                    .then();
        };

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(request -> {
            if (request.canUpgradeToWebSocket() && pathOf(request.path()).equals(PATH)) {
                upgrade(request, handler);
            } else {
                request.response().setStatusCode(404).end();
            }
        });
        return server.listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().join().actualPort();
    }

    private static void upgrade(HttpServerRequest request, WebSocketHandler handler) {
        VertxServerHttpRequest springRequest = new VertxServerHttpRequest(request);
        VertxServerHttpResponse springResponse = new VertxServerHttpResponse(request.response());
        MockServerWebExchange base = MockServerWebExchange.from(
                MockServerHttpRequest.method(org.springframework.http.HttpMethod.GET, pathOf(request.path()))
                        .build());

        // The strategy reads the Vert.x request off the exchange to perform the real upgrade, so the
        // exchange must hand back the native-backed adapters rather than the mock ones.
        org.springframework.web.server.ServerWebExchange exchange =
                new org.springframework.web.server.ServerWebExchangeDecorator(base) {
                    @Override
                    public org.springframework.http.server.reactive.ServerHttpRequest getRequest() {
                        return springRequest;
                    }

                    @Override
                    public org.springframework.http.server.reactive.ServerHttpResponse getResponse() {
                        return springResponse;
                    }
                };

        HandshakeInfo handshake = new HandshakeInfo(
                URI.create(request.absoluteURI()),
                new HttpHeaders(),
                reactor.core.publisher.Mono.just(() -> "e2e"),
                null);

        new VertxRequestUpgradeStrategy()
                .upgrade(exchange, handler, null, () -> handshake)
                .subscribe(null, t -> request.response().setStatusCode(500).end());
    }

    private static String groupOf(WebSocketSession session) {
        String query = session.getHandshakeInfo().getUri().getQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && "group".equals(pair.substring(0, eq))) {
                return pair.substring(eq + 1);
            }
        }
        return null;
    }

    private static String pathOf(String uri) {
        int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }

    @AfterAll
    static void stopCluster() {
        closeQuietly(serverA);
        closeQuietly(serverB);
        closeQuietly(vertxB);
        closeQuietly(vertxA);
    }

    private static void closeQuietly(HttpServer server) {
        if (server != null) {
            try {
                server.close().toCompletionStage().toCompletableFuture().join();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    private static void closeQuietly(Vertx vertx) {
        if (vertx != null) {
            try {
                vertx.close().toCompletionStage().toCompletableFuture().join();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("a broadcast on A is delivered to a session held by B")
    void broadcastReachesRemoteSession() throws Exception {
        try (RecordingClient onA = RecordingClient.connect(portA, null);
             RecordingClient onB = RecordingClient.connect(portB, null)) {

            assertTrue(awaitUntil(() -> broadcasterA.localSessionCount() == 1
                            && broadcasterB.localSessionCount() == 1),
                    "each node should hold exactly one session, A=" + broadcasterA.localSessionCount()
                            + " B=" + broadcasterB.localSessionCount());

            // Keep publishing: a broadcast issued before the selector knows the peer is legitimately
            // local-only, and there is no flush between registering a consumer and being routable.
            awaitUntil(() -> {
                broadcasterA.broadcast("hello-from-a");
                return !onB.frames.isEmpty();
            });

            assertEquals("hello-from-a", onB.nextText(), "node B's session never received the message");
            assertEquals("hello-from-a", onA.nextText(), "node A should also deliver to its own session");
        }
    }

    @Test
    @Order(2)
    @DisplayName("a client message on B is re-broadcast and reaches the session on A")
    void clientMessageOnBReachesSessionOnA() throws Exception {
        try (RecordingClient onA = RecordingClient.connect(portA, null);
             RecordingClient onB = RecordingClient.connect(portB, null)) {

            assertTrue(awaitUntil(() -> broadcasterA.localSessionCount() == 1
                            && broadcasterB.localSessionCount() == 1),
                    "sessions were not registered on both nodes");

            // The first publish after a consumer registers can legitimately stay local: the node
            // selector has not resolved the peer yet. Send until it crosses — the same thing a real
            // client-retry loop would do.
            String aGot = null;
            long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (aGot == null && System.currentTimeMillis() < deadline) {
                onB.send("hi-from-b");
                aGot = onA.pollText(2_000);
            }

            assertEquals("echo:hi-from-b", aGot,
                    "the message typed on node B never reached the session on node A");
        }
    }

    @Test
    @Order(3)
    @DisplayName("a group broadcast does not leak across nodes")
    void groupIsolatedAcrossNodes() throws Exception {
        try (RecordingClient inGroupA = RecordingClient.connect(portA, "room-1");
             RecordingClient otherGroupB = RecordingClient.connect(portB, "room-2")) {

            assertTrue(awaitUntil(() -> broadcasterA.localSessionCount() == 1
                            && broadcasterB.localSessionCount() == 1),
                    "sessions were not registered on both nodes");

            awaitUntil(() -> {
                broadcasterA.broadcast("scoped", "room-1");
                return !inGroupA.frames.isEmpty();
            });

            assertEquals("scoped", inGroupA.nextText());
            assertTrue(otherGroupB.frames.isEmpty(),
                    "a room-2 session on another node must not receive a room-1 broadcast");
        }
    }

    // --- client ---

    /**
     * A JDK WebSocket client that records every text frame it receives.
     */
    private static final class RecordingClient implements AutoCloseable {

        private final WebSocket socket;
        /** Shared with the listener: copying it once would miss every frame that arrives later. */
        private final BlockingQueue<String> frames;

        static RecordingClient connect(int port, String group) throws Exception {
            URI uri = URI.create("ws://127.0.0.1:" + port + PATH
                    + (group == null ? "" : "?group=" + group));
            BlockingQueue<String> sink = new LinkedBlockingQueue<>();
            WebSocket socket = HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(20))
                    .buildAsync(uri, new CollectingListener(sink))
                    .get(20, TimeUnit.SECONDS);
            return new RecordingClient(socket, sink);
        }

        private RecordingClient(WebSocket socket, BlockingQueue<String> frames) {
            this.socket = socket;
            this.frames = frames;
        }

        void send(String text) {
            socket.sendText(text, true).join();
        }

        String pollText(long timeoutMs) throws InterruptedException {
            return frames.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }

        String nextText() throws InterruptedException {
            String value = frames.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertNotNull(value, "no frame arrived within " + TIMEOUT_MS + "ms");
            return value;
        }

        @Override
        public void close() {
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
            } catch (Exception ignored) {
                // best effort
            }
            socket.abort();
        }
    }

    private static final class CollectingListener implements WebSocket.Listener {

        private final BlockingQueue<String> sink;
        private final StringBuilder partial = new StringBuilder();

        private CollectingListener(BlockingQueue<String> sink) {
            this.sink = sink;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(Long.MAX_VALUE);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                sink.add(partial.toString());
                partial.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            // Surfaced as a missing frame by the assertions.
        }
    }

    private static boolean awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port for the E2E cluster", e);
        }
    }
}
