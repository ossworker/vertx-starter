package com.workoss.boot.vertx.web.server.websocket;

import com.workoss.boot.vertx.web.server.VertxServerHttpRequest;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClosedException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the error contract of {@link VertxRequestUpgradeStrategy}: once the 101 has been
 * written there is no HTTP response left to render an error into, so failures must terminate
 * the exchange instead of propagating to the WebFlux error pipeline.
 */
class VertxRequestUpgradeStrategyTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String WS_URI = "http://localhost/ws/echo";

    private final VertxRequestUpgradeStrategy strategy = new VertxRequestUpgradeStrategy();

    @Test
    void terminatesInsteadOfPropagatingAFailureRaisedAfterTheUpgrade() {
        Throwable[] failures = {
                new SocketException("Connection reset"),
                new HttpClosedException("Connection was closed"),
                new IllegalStateException("handler blew up")
        };
        for (Throwable failure : failures) {
            WebSocketHandler handler = session -> Mono.error(failure);
            assertThatCode(() -> upgrade(handler).block(TIMEOUT))
                    .as("failure of type %s", failure.getClass().getName())
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void stillReportsAHandshakeThatCannotBeUpgraded() {
        HttpServerRequest nativeRequest = nativeRequest(false);
        WebSocketHandler handler = session -> Mono.empty();

        assertThatThrownBy(() -> upgrade(nativeRequest, handler).block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot upgrade to WebSocket");
    }

    @Test
    void treatsVertxAndNativeTransportTeardownsAsThePeerGoingAway() {
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(new HttpClosedException("Connection was closed"))).isTrue();
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(new SocketException("Connection reset"))).isTrue();
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(new ClosedChannelException())).isTrue();
        // epoll/kqueue report the same teardown as an IOException with the errno text appended.
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(
                new IOException("readAddress(..) failed: Connection reset"))).isTrue();
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(
                new RuntimeException("wrapper", new SocketException("Broken pipe")))).isTrue();
    }

    @Test
    void treatsHandlerFailuresAsGenuineErrors() {
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(new IllegalStateException("boom"))).isFalse();
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(new IOException("database unavailable"))).isFalse();
        assertThat(VertxRequestUpgradeStrategy.isPeerGone(
                new RuntimeException("wrapper", new IllegalStateException("boom")))).isFalse();
    }

    private Mono<Void> upgrade(WebSocketHandler handler) {
        return upgrade(nativeRequest(true), handler);
    }

    private Mono<Void> upgrade(HttpServerRequest nativeRequest, WebSocketHandler handler) {
        VertxServerHttpRequest springRequest = new VertxServerHttpRequest(nativeRequest);
        ServerWebExchange exchange = mock(ServerWebExchange.class);
        when(exchange.getRequest()).thenReturn(springRequest);
        return strategy.upgrade(exchange, handler, null, VertxRequestUpgradeStrategyTest::handshakeInfo);
    }

    private static HttpServerRequest nativeRequest(boolean upgradable) {
        HttpServerRequest request = mock(HttpServerRequest.class);
        when(request.method()).thenReturn(HttpMethod.GET);
        when(request.absoluteURI()).thenReturn(WS_URI);
        when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
        when(request.canUpgradeToWebSocket()).thenReturn(upgradable);
        if (upgradable) {
            HttpServerResponse response = mock(HttpServerResponse.class);
            ServerWebSocket webSocket = mock(ServerWebSocket.class);
            Future<ServerWebSocket> upgraded = Future.succeededFuture(webSocket);
            when(request.response()).thenReturn(response);
            when(request.toWebSocket()).thenReturn(upgraded);
        }
        return request;
    }

    private static HandshakeInfo handshakeInfo() {
        return new HandshakeInfo(URI.create(WS_URI), new HttpHeaders(), Mono.empty(), null);
    }
}
