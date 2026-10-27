package com.workoss.boot.vertx.web.server;

import com.workoss.boot.vertx.web.server.websocket.VertxWebSocketProperties;
import io.vertx.core.Vertx;
import io.vertx.core.http.Http2Settings;
import io.vertx.core.http.HttpServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.reactive.server.AbstractReactiveWebServerFactory;
import org.springframework.boot.web.server.Compression;
import org.springframework.boot.web.server.Http2;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServer;
import org.springframework.http.server.reactive.HttpHandler;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * ReactiveWebServerFactory implementation that creates Vert.x-backed web servers.
 * <p>
 * Standard properties are read from Spring Boot's {@code server.*} configuration:
 * <ul>
 *   <li>{@code server.port} - listening port</li>
 *   <li>{@code server.address} - bind address</li>
 *   <li>{@code server.ssl.*} - SSL/TLS configuration</li>
 *   <li>{@code server.compression.enabled} - response compression</li>
 *   <li>{@code server.http2.enabled} - HTTP/2 support (auto-enables ALPN when SSL is on)</li>
 * </ul>
 * Vert.x-specific settings are read from {@code vertx.http-server.*} via {@link VertxHttpServerProperties}.
 */
public class VertxReactiveWebServerFactory extends AbstractReactiveWebServerFactory {

    private static final Logger logger = LoggerFactory.getLogger(VertxReactiveWebServerFactory.class);

    private final Vertx vertx;
    private final VertxHttpServerProperties vertxProperties;
    private final VertxWebSocketProperties webSocketProperties;
    private final ServerProperties serverProperties;

    public VertxReactiveWebServerFactory(Vertx vertx, VertxHttpServerProperties vertxProperties,
                                           VertxWebSocketProperties webSocketProperties,
                                           ServerProperties serverProperties) {
        this.vertx = vertx;
        this.vertxProperties = vertxProperties;
        this.webSocketProperties = webSocketProperties;
        this.serverProperties = serverProperties;
    }

    @Override
    public WebServer getWebServer(HttpHandler httpHandler) {
        HttpServerOptions options = buildHttpServerOptions();
        VertxHttpHandlerAdapter handlerAdapter =
                new VertxHttpHandlerAdapter(httpHandler, webSocketProperties.isEnabled());
        return new VertxWebServer(vertx, options, handlerAdapter);
    }

    private HttpServerOptions buildHttpServerOptions() {
        HttpServerOptions options = new HttpServerOptions();

        // --- Standard Spring Boot server.* properties ---

        // Port: from server.port (default 8080 via AbstractReactiveWebServerFactory)
        int port = getPort();
        options.setPort(port >= 0 ? port : 8080);

        // Address: from server.address
        InetAddress address = getAddress();
        options.setHost(address != null ? address.getHostAddress() : "0.0.0.0");

        // SSL: from server.ssl.*
        Ssl ssl = getSsl();
        boolean sslEnabled = ssl != null && ssl.isEnabled();
        if (sslEnabled) {
            options.setSsl(true);
        }

        // Compression: from server.compression.enabled
        Compression compression = getCompression();
        if (compression != null && compression.getEnabled()) {
            options.setCompressionSupported(true);
            options.setCompressionLevel(vertxProperties.getCompressionLevel());
        }

        // --- HTTP/2: from server.http2.enabled ---
        // When SSL is enabled and server.http2.enabled=true, auto-enable ALPN for HTTP/2 negotiation
        Http2 http2 = serverProperties.getHttp2();
        boolean http2Enabled = http2 != null && http2.isEnabled();
        if (http2Enabled) {
            options.setUseAlpn(true);
            Http2Settings http2Settings = new Http2Settings();
            options.setInitialSettings(http2Settings);
        }

        // --- Vert.x-specific properties (vertx.http-server.*) ---
        options.setIdleTimeout(vertxProperties.getIdleTimeout());
        options.setIdleTimeoutUnit(TimeUnit.SECONDS);
        options.setMaxHeaderSize(vertxProperties.getMaxHeaderSize());
        options.setMaxChunkSize(vertxProperties.getMaxChunkSize());
        options.setMaxInitialLineLength(vertxProperties.getMaxInitialLineLength());
        options.setAcceptBacklog(vertxProperties.getAcceptBacklog());

        // --- WebSocket properties (vertx.websocket.*) ---
        if (webSocketProperties.isEnabled()) {
            options.setMaxWebSocketFrameSize(webSocketProperties.getMaxFrameSize());
            options.setMaxWebSocketMessageSize(webSocketProperties.getMaxMessageSize());
            options.setPerMessageWebSocketCompressionSupported(webSocketProperties.isPerMessageCompression());
            options.setPerFrameWebSocketCompressionSupported(webSocketProperties.isPerFrameCompression());
            options.setWebSocketCompressionLevel(webSocketProperties.getCompressionLevel());
            options.setWebSocketAllowServerNoContext(webSocketProperties.isAllowServerNoContext());
            options.setWebSocketPreferredClientNoContext(webSocketProperties.isPreferredClientNoContext());
            options.setWebSocketClosingTimeout(webSocketProperties.getClosingTimeout());
            // Vert.x registers an event bus write handler per connection only when the socket bridge
            // is used; this starter routes messages through Spring's WebSocketHandler instead.
            options.setRegisterWebSocketWriteHandlers(false);
            for (String subProtocol : webSocketProperties.getSubProtocols()) {
                options.addWebSocketSubProtocol(subProtocol);
            }
            if (http2Enabled) {
                logger.warn("server.http2.enabled and vertx.websocket.enabled are both on: Vert.x can only "
                        + "upgrade HTTP/1.1 requests, so WebSocket handshakes negotiated over h2 will fail");
            }
        }

        return options;
    }
}
