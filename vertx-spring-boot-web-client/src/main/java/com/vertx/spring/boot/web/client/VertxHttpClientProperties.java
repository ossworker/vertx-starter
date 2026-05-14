package com.vertx.spring.boot.web.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Vert.x HTTP client.
 */
@ConfigurationProperties(prefix = "vertx.http-client")
public class VertxHttpClientProperties {

    /**
     * Maximum number of connections per host in the pool.
     */
    private int maxPoolSize = 5;

    /**
     * Connection timeout in milliseconds.
     */
    private int connectTimeout = 60000;

    /**
     * Idle timeout in seconds.
     */
    private int idleTimeout = 30;

    /**
     * Read timeout in milliseconds.
     */
    private int readTimeout = 30000;

    /**
     * Whether to use HTTP keep-alive.
     */
    private boolean keepAlive = true;

    /**
     * Whether to enable HTTP pipelining.
     */
    private boolean pipelining = false;

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isPipelining() {
        return pipelining;
    }

    public void setPipelining(boolean pipelining) {
        this.pipelining = pipelining;
    }
}
