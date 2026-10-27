package com.workoss.boot.vertx.web.server.websocket;

import io.vertx.core.http.HttpServerOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * WebSocket settings for the Vert.x HTTP server, bound to {@code vertx.websocket.*}.
 * <p>
 * These map onto Vert.x's {@link HttpServerOptions} WebSocket section. The listening port,
 * bind address, SSL and HTTP/2 settings keep coming from Spring Boot's standard
 * {@code server.*} namespace, so HTTP and WebSocket share a single port by construction.
 *
 * @see VertxRequestUpgradeStrategy
 */
@ConfigurationProperties(prefix = "vertx.websocket")
public class VertxWebSocketProperties {

    /**
     * Whether WebSocket upgrade support is enabled. When disabled the HTTP request pipeline
     * keeps its original hot path (no upgrade probe per request).
     */
    private boolean enabled = true;

    /**
     * EventBus address carrying cluster-wide WebSocket fan-out. Every node subscribes to it and
     * pushes what it receives to the sessions it owns locally.
     */
    private String broadcastAddress = ClusterWebSocketBroadcaster.DEFAULT_ADDRESS;

    /**
     * Maximum size of a single WebSocket frame in bytes.
     */
    private int maxFrameSize = HttpServerOptions.DEFAULT_MAX_WEBSOCKET_FRAME_SIZE;

    /**
     * Maximum size of an aggregated WebSocket message in bytes. Frames of a fragmented
     * message are aggregated up to this limit before being delivered to the handler.
     */
    private int maxMessageSize = HttpServerOptions.DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE;

    /**
     * Sub-protocols the server is willing to speak. Netty negotiates the value echoed back
     * in the {@code Sec-WebSocket-Protocol} response header from this list.
     */
    private List<String> subProtocols = new ArrayList<>();

    /**
     * Enable the {@code permessage-deflate} WebSocket extension.
     */
    private boolean perMessageCompression = false;

    /**
     * Enable the {@code deflate-frame} WebSocket extension. Mutually exclusive with
     * per-message compression; when both are on Vert.x applies per-frame.
     */
    private boolean perFrameCompression = false;

    /**
     * Compression level (1-9) for WebSocket extensions. Independent from HTTP compression.
     */
    private int compressionLevel = HttpServerOptions.DEFAULT_WEBSOCKET_COMPRESSION_LEVEL;

    /**
     * Allow the server to use {@code no_context_takeover} when the client supports it.
     */
    private boolean allowServerNoContext = false;

    /**
     * Prefer the client to use {@code no_context_takeover}.
     */
    private boolean preferredClientNoContext = false;

    /**
     * Seconds the server waits for the peer close frame after sending its own, before
     * tearing down the TCP connection. {@code 0} closes immediately, a negative value disables it.
     */
    private int closingTimeout = HttpServerOptions.DEFAULT_WEBSOCKET_CLOSING_TIMEOUT;

    /**
     * Heartbeat interval (seconds). The server writes a {@code PING} frame every this many seconds
     * to a connected peer. A healthy browser (or any RFC 6455 client) auto-replies {@code PONG},
     * which resets the connection's idle timer and keeps an otherwise-idle socket alive.
     * <p>
     * Set to {@code 0} to disable heartbeats entirely. When disabled, an idle connection is only
     * reclaimed by {@link #idleTimeout} and will be dropped once it stops sending anything at all.
     * A value smaller than {@link #idleTimeout} gives the peer a few missed-heartbeat retries before
     * reclamation, which is the recommended posture.
     */
    private long pingInterval = 25;

    /**
     * Idle timeout (seconds) for a single WebSocket connection. If the server receives no frame
     * (application message, PONG, or anything else) within this window, it reclaims the connection:
     * the socket is closed and every layer listening on close (roster cleanup, async-map removal,
     * broadcaster unregister) runs. This is the "idle for some time, server reclaims" guarantee.
     * <p>
     * {@code 0} disables idle reclamation (the server then never drops a socket for being quiet).
     * Heartbeats keep healthy idle peers under this threshold; only a peer that truly stopped
     * answering PINGs reaches it.
     */
    private long idleTimeout = 60;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getBroadcastAddress() {
        return broadcastAddress;
    }

    public void setBroadcastAddress(String broadcastAddress) {
        this.broadcastAddress = broadcastAddress;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public List<String> getSubProtocols() {
        return subProtocols;
    }

    public void setSubProtocols(List<String> subProtocols) {
        this.subProtocols = subProtocols;
    }

    public boolean isPerMessageCompression() {
        return perMessageCompression;
    }

    public void setPerMessageCompression(boolean perMessageCompression) {
        this.perMessageCompression = perMessageCompression;
    }

    public boolean isPerFrameCompression() {
        return perFrameCompression;
    }

    public void setPerFrameCompression(boolean perFrameCompression) {
        this.perFrameCompression = perFrameCompression;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public boolean isAllowServerNoContext() {
        return allowServerNoContext;
    }

    public void setAllowServerNoContext(boolean allowServerNoContext) {
        this.allowServerNoContext = allowServerNoContext;
    }

    public boolean isPreferredClientNoContext() {
        return preferredClientNoContext;
    }

    public void setPreferredClientNoContext(boolean preferredClientNoContext) {
        this.preferredClientNoContext = preferredClientNoContext;
    }

    public int getClosingTimeout() {
        return closingTimeout;
    }

    public void setClosingTimeout(int closingTimeout) {
        this.closingTimeout = closingTimeout;
    }

    public long getPingInterval() {
        return pingInterval;
    }

    public void setPingInterval(long pingInterval) {
        this.pingInterval = pingInterval;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }
}
