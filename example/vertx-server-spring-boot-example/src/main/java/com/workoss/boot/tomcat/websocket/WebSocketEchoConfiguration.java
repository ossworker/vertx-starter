package com.workoss.boot.tomcat.websocket;

import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.Map;

/**
 * Standard WebFlux WebSocket endpoints, served by the Vert.x HTTP server on the very same
 * {@code server.port} as {@code @GetMapping} routes. Open {@code static/websocket.html} in a
 * browser to exercise them.
 * <p>
 * {@code /ws/cluster} additionally registers each session with {@link ClusterWebSocketBroadcaster},
 * so a message published through it reaches every connection across the cluster while HTTP and
 * WebSocket still share one port.
 */
@Configuration(proxyBeanMethods = false)
public class WebSocketEchoConfiguration {

    @Bean
    public SimpleUrlHandlerMapping webSocketHandlerMapping(ClusterWebSocketBroadcaster broadcaster) {
        return new SimpleUrlHandlerMapping(
                Map.of("/ws/echo", echoHandler(), "/ws/cluster", clusterHandler(broadcaster)), -1);
    }

    private WebSocketHandler echoHandler() {
        return session -> session.send(session.receive()
                .map(message -> session.textMessage("[" + session.getId() + "] " + message.getPayloadAsText())));
    }

    /**
     * Registers the session so cluster broadcasts can find it, then echoes anything the client
     * sends. The group is taken from the {@code ?group=} query parameter; anything sent by a client
     * is re-published so every node delivers it to its own sessions.
     */
    private WebSocketHandler clusterHandler(ClusterWebSocketBroadcaster broadcaster) {
        return session -> {
            String group = groupOf(session);
            broadcaster.register(session, group);

            return session.receive()
                    .doOnNext(message -> broadcaster.broadcast(
                            "[" + session.getId() + "] " + message.getPayloadAsText(), group))
                    .doFinally(signal -> broadcaster.unregister(session.getId()))
                    .then();
        };
    }

    private String groupOf(WebSocketSession session) {
        var query = session.getHandshakeInfo().getUri().getQuery();
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
}
