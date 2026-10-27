package com.workoss.boot.vertx.web.server.websocket;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ClusterWebSocketBroadcaster} 测试。
 * <p>
 * 用真实 Vert.x 实例(含本地 eventbus),验证「发布 → 本地会话收到」这条链路。真正的跨节点投递
 * 依赖 {@code ClusteredEventBus},依赖真实 Zenoh session,不在单元测试范围内;这里覆盖的是本节点
 * 侧的注册表与投递语义,跨节点部分由 {@code ClusterRegistrationTableTest} 保证注册表可见性。
 */
class ClusterWebSocketBroadcasterTest {

    private Vertx vertx;
    private ClusterWebSocketBroadcaster broadcaster;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        broadcaster = new ClusterWebSocketBroadcaster(vertx);
        broadcaster.registerListener();
    }

    @AfterEach
    void tearDown() {
        vertx.close().await();
    }

    /**
     * Waits until {@code condition} holds. The event bus delivers on the event loop, so assertions
     * made straight after a publish race the consumer; polling keeps the test deterministic without
     * pinning a sleep long enough to slow the suite.
     */
    private static void awaitUntil(java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(5).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void awaitTexts(RecordingSession session, int expected) {
        awaitUntil(() -> session.texts.size() >= expected);
    }

    @Test
    @DisplayName("broadcast reaches every session held by this node")
    void broadcastReachesAllLocalSessions() {
        RecordingSession a = new RecordingSession("s1");
        RecordingSession b = new RecordingSession("s2");
        broadcaster.register(a, null);
        broadcaster.register(b, null);

        broadcaster.broadcast("hello");
        awaitTexts(a, 1);
        awaitTexts(b, 1);

        assertEquals(List.of("hello"), a.texts);
        assertEquals(List.of("hello"), b.texts);
    }

    @Test
    @DisplayName("group broadcast skips sessions outside the group")
    void groupBroadcastIsScoped() {
        RecordingSession inGroup = new RecordingSession("s1");
        RecordingSession outside = new RecordingSession("s2");
        broadcaster.register(inGroup, "room-1");
        broadcaster.register(outside, "room-2");

        broadcaster.broadcast("scoped", "room-1");
        awaitTexts(inGroup, 1);

        assertEquals(List.of("scoped"), inGroup.texts);
        assertTrue(outside.texts.isEmpty(), "a session in another group must not receive this");
    }

    @Test
    @DisplayName("unregistered sessions stop receiving")
    void unregisteredSessionIsSkipped() {
        RecordingSession session = new RecordingSession("s1");
        broadcaster.register(session, null);
        broadcaster.unregister("s1");

        broadcaster.broadcast("after-close");

        assertTrue(session.texts.isEmpty());
        assertEquals(0, broadcaster.localSessionCount());
    }

    @Test
    @DisplayName("unregister also drops group membership")
    void unregisterClearsGroup() {
        RecordingSession session = new RecordingSession("s1");
        broadcaster.register(session, "room-1");
        broadcaster.unregister("s1");

        broadcaster.broadcast("nobody", "room-1");

        assertTrue(session.texts.isEmpty());
    }

    @Test
    @DisplayName("a closed session is filtered out before writing")
    void closedSessionIsFiltered() {
        RecordingSession open = new RecordingSession("s1");
        RecordingSession closed = new RecordingSession("s2");
        closed.open = false;
        broadcaster.register(open, null);
        broadcaster.register(closed, null);

        broadcaster.broadcast("ping");
        awaitTexts(open, 1);

        assertEquals(List.of("ping"), open.texts);
        assertTrue(closed.texts.isEmpty());
    }

    @Test
    @DisplayName("a failing write does not cancel the writes behind it")
    void failingSessionDoesNotBreakTheRest() {
        RecordingSession failing = new RecordingSession("bad");
        failing.failOnSend = true;
        RecordingSession healthy = new RecordingSession("good");
        broadcaster.register(failing, null);
        broadcaster.register(healthy, null);

        broadcaster.broadcast("resilient");
        awaitTexts(healthy, 1);

        assertEquals(List.of("resilient"), healthy.texts,
                "one broken peer must not starve the sessions queued behind it");
        // The failing session is evicted rather than retried on every future broadcast.
        awaitUntil(() -> broadcaster.localSessionCount() == 1);
    }

    @Test
    @DisplayName("sendToLocal only writes to sessions this node owns")
    void sendToLocalIsNodeScoped() {
        RecordingSession mine = new RecordingSession("mine");
        broadcaster.register(mine, null);

        assertTrue(broadcaster.sendToLocal("mine", "direct"));
        awaitTexts(mine, 1);
        assertEquals(List.of("direct"), mine.texts);

        assertFalse(broadcaster.sendToLocal("not-mine", "direct"),
                "a session owned by another node must not resolve here");
    }

    @Test
    @DisplayName("localSessions exposes an immutable snapshot")
    void localSessionsIsImmutable() {
        broadcaster.register(new RecordingSession("s1"), null);
        Map<String, WebSocketSession> snapshot = broadcaster.localSessions();
        assertEquals(1, snapshot.size());
        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                () -> snapshot.put("x", new RecordingSession("x")));
    }

    @Test
    @DisplayName("a non-String payload is stringified rather than dropped")
    void nonStringPayloadIsStringified() {
        RecordingSession session = new RecordingSession("s1");
        broadcaster.register(session, null);

        broadcaster.broadcast(42);
        awaitTexts(session, 1);

        assertEquals(List.of("42"), session.texts);
    }

    @Test
    @DisplayName("concurrent broadcasts to one session never overlap session.send()")
    void concurrentBroadcastsDoNotOverlapSends() throws Exception {
        // A 5ms per-send window makes a real overlap almost certain under the old (un-serialized) code,
        // and proves the per-session write chain keeps sends strictly sequential now.
        ConcurrentTrackingSession session = new ConcurrentTrackingSession("s1", 5);
        broadcaster.register(session, null);

        int threads = 20;
        int perThread = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    broadcaster.broadcast("msg", null);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        // Give the serialized chain time to drain every queued write.
        Thread.sleep(1000);

        assertFalse(session.overlapped.get(),
                "session.send() ran concurrently (peak in-flight=" + session.inFlight.get() + ")");
        assertEquals(threads * perThread, session.texts.size(),
                "every broadcast must have been delivered");
        assertTrue(session.isOpen(), "the connection must not have been torn down by a write conflict");
    }

    /**
     * Minimal {@link WebSocketSession} that records what it was asked to write. Extends
     * {@code AbstractWebSocketSession} so the message factories and handshake plumbing come from
     * Spring instead of being reimplemented here.
     */
    private static final class RecordingSession
            extends org.springframework.web.reactive.socket.adapter.AbstractWebSocketSession<String> {

        private final List<String> texts = new CopyOnWriteArrayList<>();
        private final Sinks.One<CloseStatus> closeStatus = Sinks.one();
        private boolean open = true;
        private boolean failOnSend;

        private RecordingSession(String id) {
            super(id, id, new org.springframework.web.reactive.socket.HandshakeInfo(
                    java.net.URI.create("ws://localhost/" + id),
                    new org.springframework.http.HttpHeaders(),
                    Mono.just(() -> "test-principal"), null),
                    new org.springframework.core.io.buffer.DefaultDataBufferFactory());
        }

        @Override
        public Flux<WebSocketMessage> receive() {
            return Flux.never();
        }

        @Override
        public Mono<Void> send(org.reactivestreams.Publisher<WebSocketMessage> messages) {
            if (failOnSend) {
                return Mono.error(new IllegalStateException("peer gone"));
            }
            return Flux.from(messages)
                    .doOnNext(m -> texts.add(m.getPayloadAsText()))
                    .then();
        }

        @Override
        public Mono<Void> close(CloseStatus status) {
            this.open = false;
            closeStatus.tryEmitValue(status);
            return Mono.empty();
        }

        @Override
        public Mono<CloseStatus> closeStatus() {
            return closeStatus.asMono().timeout(Duration.ofSeconds(5));
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }

    /**
     * Like {@link RecordingSession} but flags any moment where more than one {@code send()} is in
     * flight at the same time — the exact contract violation that previously tore connections down.
     */
    private static final class ConcurrentTrackingSession
            extends org.springframework.web.reactive.socket.adapter.AbstractWebSocketSession<String> {

        private final List<String> texts = new CopyOnWriteArrayList<>();
        private final Sinks.One<CloseStatus> closeStatus = Sinks.one();
        private final AtomicInteger inFlight = new AtomicInteger(0);
        private final AtomicBoolean overlapped = new AtomicBoolean(false);
        private volatile boolean open = true;
        private final long sendDelayMs;

        private ConcurrentTrackingSession(String id, long sendDelayMs) {
            super(id, id, new org.springframework.web.reactive.socket.HandshakeInfo(
                    java.net.URI.create("ws://localhost/" + id),
                    new org.springframework.http.HttpHeaders(),
                    Mono.just(() -> "test-principal"), null),
                    new org.springframework.core.io.buffer.DefaultDataBufferFactory());
            this.sendDelayMs = sendDelayMs;
        }

        @Override
        public Flux<WebSocketMessage> receive() {
            return Flux.never();
        }

        @Override
        public Mono<Void> send(org.reactivestreams.Publisher<WebSocketMessage> messages) {
            if (inFlight.incrementAndGet() > 1) {
                overlapped.set(true);
            }
            return Flux.from(messages)
                    .doOnNext(m -> {
                        texts.add(m.getPayloadAsText());
                        if (sendDelayMs > 0) {
                            try {
                                Thread.sleep(sendDelayMs);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    })
                    .then()
                    .doFinally(sig -> {
                        int now = inFlight.decrementAndGet();
                        if (now < 0) {
                            inFlight.set(0);
                        }
                    });
        }

        @Override
        public Mono<Void> close(CloseStatus status) {
            this.open = false;
            closeStatus.tryEmitValue(status);
            return Mono.empty();
        }

        @Override
        public Mono<CloseStatus> closeStatus() {
            return closeStatus.asMono().timeout(Duration.ofSeconds(5));
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }
}
