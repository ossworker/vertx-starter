package com.workoss.boot.vertx.web.server;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import org.springframework.boot.web.reactive.server.AbstractReactiveWebServerFactory;
import org.springframework.boot.web.server.Compression;
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
 * </ul>
 * Vert.x-specific settings are read from {@code vertx.http-server.*} via {@link VertxHttpServerProperties}.
 */
public class VertxReactiveWebServerFactory extends AbstractReactiveWebServerFactory {

    private final Vertx vertx;
    private final VertxHttpServerProperties vertxProperties;

    public VertxReactiveWebServerFactory(Vertx vertx, VertxHttpServerProperties vertxProperties) {
        this.vertx = vertx;
        this.vertxProperties = vertxProperties;
    }

    @Override
    public WebServer getWebServer(HttpHandler httpHandler) {
        HttpServerOptions options = buildHttpServerOptions();
        VertxHttpHandlerAdapter handlerAdapter = new VertxHttpHandlerAdapter(httpHandler);
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
        if (ssl != null && ssl.isEnabled()) {
            options.setSsl(true);
        }

        // Compression: from server.compression.enabled
        Compression compression = getCompression();
        if (compression != null && compression.getEnabled()) {
            options.setCompressionSupported(true);
            options.setCompressionLevel(vertxProperties.getCompressionLevel());
        }

        // --- Vert.x-specific properties (vertx.http-server.*) ---
        options.setIdleTimeout(vertxProperties.getIdleTimeout());
        options.setIdleTimeoutUnit(TimeUnit.SECONDS);
        options.setMaxHeaderSize(vertxProperties.getMaxHeaderSize());
        options.setMaxChunkSize(vertxProperties.getMaxChunkSize());
        options.setMaxInitialLineLength(vertxProperties.getMaxInitialLineLength());
        options.setAcceptBacklog(vertxProperties.getAcceptBacklog());

        return options;
    }
}
