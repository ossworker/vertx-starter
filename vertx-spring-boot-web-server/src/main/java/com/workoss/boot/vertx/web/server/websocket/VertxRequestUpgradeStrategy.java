package com.workoss.boot.vertx.web.server.websocket;

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.http.HttpClosedException;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.lang.Nullable;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.RequestUpgradeStrategy;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.DisconnectedClientHelper;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * {@link RequestUpgradeStrategy} that upgrades a Vert.x request in place, so HTTP and
 * WebSocket keep sharing the single {@code HttpServer} port.
 * <p>
 * Deliberately does <strong>not</strong> call {@code exchange.getResponse().setComplete()}
 * the way {@code ReactorNettyRequestUpgradeStrategy} does: on Vert.x that would end the
 * response with a 200 and make the upgrade impossible. A consequence is that callbacks
 * registered via {@code response.beforeCommit(...)} never run for WebSocket handshakes.
 * <p>
 * Requires the {@code HttpServer} to have no {@code webSocketHandler} registered, which is
 * what keeps upgrade requests flowing through the WebFlux pipeline.
 */
public class VertxRequestUpgradeStrategy implements RequestUpgradeStrategy {

    private static final Logger logger = LoggerFactory.getLogger(VertxRequestUpgradeStrategy.class);

    private static final NettyDataBufferFactory BUFFER_FACTORY =
            new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);

    private final Vertx vertx;
    private final VertxWebSocketProperties properties;

    /**
     * Full constructor used by the auto-configuration; supplies the {@link Vertx} instance (needed
     * to schedule the heartbeat timer) and the {@link VertxWebSocketProperties} that tune idle
     * reclamation and ping cadence.
     */
    public VertxRequestUpgradeStrategy(Vertx vertx, VertxWebSocketProperties properties) {
        this.vertx = vertx;
        this.properties = properties;
    }

    /**
     * Backward-compatible no-arg constructor. Heartbeats are disabled because no {@link Vertx}
     * is available to schedule the timer; idle reclamation still applies from the default
     * properties. Retained for direct callers/tests that build the strategy by hand.
     */
    public VertxRequestUpgradeStrategy() {
        this(null, new VertxWebSocketProperties());
    }

    @Override
    public Mono<Void> upgrade(ServerWebExchange exchange, WebSocketHandler handler,
                              @Nullable String subProtocol, Supplier<HandshakeInfo> handshakeInfoFactory) {
        ServerHttpRequest request = exchange.getRequest();
        URI uri = request.getURI();
        HttpServerRequest nativeRequest = ServerHttpRequestDecorator.getNativeRequest(request);

        if (!nativeRequest.canUpgradeToWebSocket()) {
            return Mono.error(new IllegalStateException(
                    "Cannot upgrade to WebSocket; an HTTP/1.1 GET with 'Upgrade: websocket' is required: " + uri));
        }

        // Vert.x negotiates the sub-protocol from the server-wide list, but Netty's handshaker
        // merges the response headers into the 101 reply, so echoing the per-handler value here
        // is what the client actually sees.
        if (subProtocol != null && !subProtocol.isEmpty()) {
            nativeRequest.response().putHeader(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, subProtocol);
        }

        HandshakeInfo handshakeInfo = handshakeInfoFactory.get();

        return Mono.fromCompletionStage(() -> nativeRequest.toWebSocket().toCompletionStage())
                .flatMap(webSocket -> handler
                        .handle(new VertxWebSocketSession(webSocket, handshakeInfo, BUFFER_FACTORY, vertx, properties))
                        .checkpoint(uri + " [VertxRequestUpgradeStrategy]")
                        // Past this point the 101 is on the wire: there is no HTTP response left
                        // for the WebFlux error pipeline to write, and Vert.x rejects any attempt to.
                        .onErrorResume(err -> {
                            if (isPeerGone(err)) {
                                logger.debug("WebSocket {} closed by the peer: {}", uri, err.toString());
                            } else {
                                logger.error("WebSocket handler failed for {}", uri, err);
                            }
                            return Mono.empty();
                        }));
    }

    static boolean isPeerGone(Throwable error) {
        for (Throwable t = error; t != null; t = (t.getCause() == t ? null : t.getCause())) {
            if (t instanceof HttpClosedException || t instanceof SocketException
                    || t instanceof ClosedChannelException) {
                return true;
            }
            // An abrupt teardown reaches us as a bare SocketException, but native transports report
            // it as another IOException carrying the same text. DisconnectedClientHelper matches
            // only the literal "connection reset by peer", so neither would be recognised below.
            if (t instanceof IOException && mentionsPeerGone(t.getMessage())) {
                return true;
            }
        }
        return DisconnectedClientHelper.isClientDisconnectedException(error);
    }

    private static boolean mentionsPeerGone(String message) {
        if (message == null) {
            return false;
        }
        String text = message.toLowerCase(Locale.ROOT);
        return text.contains("connection reset") || text.contains("broken pipe");
    }
}
