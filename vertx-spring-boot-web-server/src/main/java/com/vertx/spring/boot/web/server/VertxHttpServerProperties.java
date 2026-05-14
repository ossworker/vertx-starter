package com.vertx.spring.boot.web.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Vert.x-specific HTTP server configuration properties.
 * <p>
 * Standard server properties (port, address, ssl, compression enabled) are read from
 * Spring Boot's {@code server.*} namespace via {@code AbstractReactiveWebServerFactory}.
 * This class only contains Vert.x-specific settings that have no standard Spring Boot equivalent.
 */
@ConfigurationProperties(prefix = "vertx.http-server")
public class VertxHttpServerProperties {

    /**
     * Compression level (1-9) when compression is enabled via server.compression.enabled=true.
     */
    private int compressionLevel = 6;

    /**
     * Idle timeout in seconds for keep-alive connections.
     */
    private int idleTimeout = 60;

    /**
     * Maximum HTTP request header size in bytes.
     */
    private int maxHeaderSize = 8192;

    /**
     * Maximum HTTP chunk size in bytes.
     */
    private int maxChunkSize = 8192;

    /**
     * Maximum initial line length (request line) in bytes.
     */
    private int maxInitialLineLength = 4096;

    /**
     * TCP accept backlog size.
     */
    private int acceptBacklog = 1024;

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getMaxHeaderSize() {
        return maxHeaderSize;
    }

    public void setMaxHeaderSize(int maxHeaderSize) {
        this.maxHeaderSize = maxHeaderSize;
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public void setMaxChunkSize(int maxChunkSize) {
        this.maxChunkSize = maxChunkSize;
    }

    public int getMaxInitialLineLength() {
        return maxInitialLineLength;
    }

    public void setMaxInitialLineLength(int maxInitialLineLength) {
        this.maxInitialLineLength = maxInitialLineLength;
    }

    public int getAcceptBacklog() {
        return acceptBacklog;
    }

    public void setAcceptBacklog(int acceptBacklog) {
        this.acceptBacklog = acceptBacklog;
    }
}
