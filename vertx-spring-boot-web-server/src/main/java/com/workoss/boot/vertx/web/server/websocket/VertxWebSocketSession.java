package com.workoss.boot.vertx.web.server.websocket;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.adapter.AbstractWebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link WebSocketSession} backed by a Vert.x {@link ServerWebSocket}.
 * <p>
 * Inbound frames are delivered through Vert.x's typed message handlers rather than
 * {@code frameHandler} so that Vert.x's own frame aggregator reassembles fragmented
 * messages. Both {@code textMessageHandler} and {@code binaryMessageHandler} must be set:
 * Vert.x nulls out the other one when only a single handler is installed, which would
 * silently drop that message type.
 * <p>
 * Vert.x answers PING frames on its own and exposes no {@code pingHandler}, so
 * {@link WebSocketMessage.Type#PING} never appears in {@link #receive()}.
 * <p>
 * Two connection-lifecycle behaviours are applied here, both driven by
 * {@link VertxWebSocketProperties}:
 * <ul>
 *   <li><b>Idle reclamation</b> — a periodic watchdog tracks the last frame received
 *       (text/binary/PONG all count) and closes the socket once quiet for
 *       {@code idleTimeout}. That close surfaces a peer that vanished without a close frame,
 *       cascading to roster / async-map cleanup in the handler.</li>
 *   <li><b>Heartbeat</b> — the same watchdog writes a {@code PING} every {@code pingInterval},
 *       which a healthy browser answers with a {@code PONG}; the PONG resets the activity
 *       timestamp, so an idle-but-live peer stays under the idle threshold and is never
 *       reclaimed. Only a peer that truly stopped answering PING gets reclaimed.</li>
 * </ul>
 * The watchdog timer is cancelled on close so a dead connection never leaks a periodic task.
 */
public class VertxWebSocketSession extends AbstractWebSocketSession<ServerWebSocket> {

    private static final Logger log = LoggerFactory.getLogger(VertxWebSocketSession.class);

    /** Empty application-data payload for PING frames; clients reply with an equally empty PONG. */
    private static final Buffer HEARTBEAT_PAYLOAD = Buffer.buffer();

    private final Sinks.One<CloseStatus> closeStatusSink = Sinks.one();
    private final NettyDataBufferFactory nettyBufferFactory;
    private final Vertx vertx;
    private final VertxWebSocketProperties properties;

    /** Id of the periodic ping task, or {@code -1} once cancelled. Guarded by CAS on close. */
    private final AtomicLong heartbeatTimer = new AtomicLong(-1L);

    /** Epoch millis of the last frame received; the idle watchdog compares against this. */
    private final AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

    private volatile FluxSink<WebSocketMessage> receiveSink;
    private volatile boolean closed;
    private volatile Throwable failure;

    public VertxWebSocketSession(ServerWebSocket webSocket, HandshakeInfo handshakeInfo,
                                 NettyDataBufferFactory bufferFactory, Vertx vertx,
                                 VertxWebSocketProperties properties) {
        super(webSocket, UUID.randomUUID().toString(), handshakeInfo, bufferFactory);
        this.nettyBufferFactory = bufferFactory;
        this.vertx = vertx;
        this.properties = properties;

        // Vert.x 5's ServerWebSocket exposes no per-socket idle timeout, so idle reclamation is
        // driven manually: a periodic watchdog writes the heartbeat PING and, if no frame has been
        // received within idleTimeout, closes the socket (which then cascades to reclamation).
        long idleTimeout = properties.getIdleTimeout();
        long pingInterval = properties.getPingInterval();
        if ((idleTimeout > 0 || pingInterval > 0) && vertx != null) {
            long idleMillis = TimeUnit.SECONDS.toMillis(idleTimeout);
            long periodMillis = pingInterval > 0
                    ? TimeUnit.SECONDS.toMillis(pingInterval)
                    : Math.max(idleMillis / 2, 500);
            long timerId = vertx.setPeriodic(periodMillis, id -> {
                if (webSocket.isClosed()) {
                    cancelHeartbeat();
                    return;
                }
                if (pingInterval > 0) {
                    // A failed future here just means the peer already went away between the
                    // isClosed() check and the write; swallowing it avoids noise on hot teardown.
                    webSocket.writePing(HEARTBEAT_PAYLOAD)
                            .onFailure(t -> log.debug("heartbeat ping dropped: {}", t.toString()));
                }
                if (idleMillis > 0 && (System.currentTimeMillis() - lastActivity.get()) > idleMillis) {
                    log.debug("reclaiming idle WebSocket {} after {}ms of silence", webSocket, idleMillis);
                    webSocket.close();
                }
            });
            heartbeatTimer.set(timerId);
        }

        webSocket.closeHandler(v -> {
            cancelHeartbeat();
            this.closed = true;
            this.closeStatusSink.tryEmitValue(readCloseStatus());
            completeReceive();
        });
    }

    private void cancelHeartbeat() {
        long id = heartbeatTimer.getAndSet(-1L);
        if (id >= 0 && vertx != null) {
            vertx.cancelTimer(id);
        }
    }

    @Override
    public Flux<WebSocketMessage> receive() {
        return Flux.create(sink -> {
            this.receiveSink = sink;
            Throwable err = this.failure;
            if (err != null) {
                sink.error(err);
                return;
            }
            if (this.closed) {
                sink.complete();
                return;
            }
            ServerWebSocket webSocket = getDelegate();
            webSocket.exceptionHandler(t -> {
                this.failure = t;
                this.closed = true;
                sink.error(t);
            });
            // Any inbound frame counts as activity and resets the idle watchdog, so a peer that is
            // genuinely sending (text/binary) is never reclaimed even if it never happens to PONG.
            webSocket.textMessageHandler(text -> {
                lastActivity.set(System.currentTimeMillis());
                sink.next(textMessage(text));
            });
            webSocket.binaryMessageHandler(buffer -> {
                lastActivity.set(System.currentTimeMillis());
                sink.next(binaryMessage(factory -> toDataBuffer(buffer)));
            });
            // PONG is a transport-level keepalive frame: it resets our idle watchdog but must NOT
            // be surfaced to the application, otherwise every heartbeat becomes a spurious message
            // (an echo handler would bounce an empty PONG payload straight back to the peer).
            webSocket.pongHandler(buffer -> lastActivity.set(System.currentTimeMillis()));
            // The Vert.x stream starts paused; resume drives the channel autoRead back on.
            sink.onRequest(n -> webSocket.resume());
            sink.onCancel(webSocket::pause);
            sink.onDispose(webSocket::pause);
        }, FluxSink.OverflowStrategy.BUFFER);
    }

    @Override
    public Mono<Void> send(Publisher<WebSocketMessage> messages) {
        return Flux.from(messages)
                .concatMap(message -> Mono.fromCompletionStage(() -> write(message).toCompletionStage()))
                .then();
    }

    @Override
    public boolean isOpen() {
        return !getDelegate().isClosed();
    }

    @Override
    public Mono<Void> close(CloseStatus status) {
        ServerWebSocket webSocket = getDelegate();
        if (webSocket.isClosed()) {
            return Mono.empty();
        }
        return Mono.fromCompletionStage(
                () -> webSocket.close((short) status.getCode(), status.getReason()).toCompletionStage());
    }

    @Override
    public Mono<CloseStatus> closeStatus() {
        return this.closeStatusSink.asMono();
    }

    private Future<Void> write(WebSocketMessage message) {
        DataBuffer payload = message.getPayload();
        try {
            return switch (message.getType()) {
                case TEXT -> getDelegate().writeTextMessage(message.getPayloadAsText());
                case BINARY -> getDelegate().writeBinaryMessage(toVertxBuffer(payload));
                case PING -> getDelegate().writePing(toVertxBuffer(payload));
                case PONG -> getDelegate().writePong(toVertxBuffer(payload));
            };
        } finally {
            DataBufferUtils.release(payload);
        }
    }

    private CloseStatus readCloseStatus() {
        ServerWebSocket webSocket = getDelegate();
        Short code = webSocket.closeStatusCode();
        if (code == null) {
            return CloseStatus.NO_STATUS_CODE;
        }
        return CloseStatus.create(code, webSocket.closeReason());
    }

    private void completeReceive() {
        FluxSink<WebSocketMessage> sink = this.receiveSink;
        if (sink != null) {
            sink.complete();
        }
    }

    /**
     * Copies the frame payload into an unpooled buffer. Retaining Vert.x's {@code ByteBuf} instead
     * would be zero-copy, but {@code receive()} queues messages until the subscriber requests them,
     * by which time Vert.x may have released the frame — and nothing in the {@code WebSocketHandler}
     * contract guarantees the retained buffer is ever released.
     */
    private DataBuffer toDataBuffer(Buffer buffer) {
        return this.nettyBufferFactory.wrap(buffer.getBytes());
    }

    private static Buffer toVertxBuffer(DataBuffer dataBuffer) {
        byte[] bytes = new byte[dataBuffer.readableByteCount()];
        dataBuffer.read(bytes);
        return Buffer.buffer(bytes);
    }
}
