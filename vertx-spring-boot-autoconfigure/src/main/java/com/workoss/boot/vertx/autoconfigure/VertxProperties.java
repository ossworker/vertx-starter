package com.workoss.boot.vertx.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Vert.x core options.
 */
@ConfigurationProperties(prefix = "vertx")
public class VertxProperties {

    /**
     * Number of event loop threads. 0 means use default (CPU cores * 2).
     */
    private int eventLoopPoolSize = 0;

    /**
     * Number of worker threads.
     */
    private int workerPoolSize = 20;

    /**
     * Number of internal blocking pool threads.
     */
    private int internalBlockingPoolSize = 20;

    /**
     * Whether to prefer native transport (epoll on Linux, kqueue on macOS).
     */
    private boolean preferNativeTransport = false;

    /**
     * Virtual thread settings.
     */
    private VirtualThreads virtualThreads = new VirtualThreads();

    /**
     * Graceful shutdown timeout in seconds.
     */
    private long gracefulShutdownTimeoutSeconds = 30;

    /**
     * Cluster configuration.
     */
    private Cluster cluster = new Cluster();

    public int getEventLoopPoolSize() {
        return eventLoopPoolSize;
    }

    public void setEventLoopPoolSize(int eventLoopPoolSize) {
        this.eventLoopPoolSize = eventLoopPoolSize;
    }

    public int getWorkerPoolSize() {
        return workerPoolSize;
    }

    public void setWorkerPoolSize(int workerPoolSize) {
        this.workerPoolSize = workerPoolSize;
    }

    public int getInternalBlockingPoolSize() {
        return internalBlockingPoolSize;
    }

    public void setInternalBlockingPoolSize(int internalBlockingPoolSize) {
        this.internalBlockingPoolSize = internalBlockingPoolSize;
    }

    public boolean isPreferNativeTransport() {
        return preferNativeTransport;
    }

    public void setPreferNativeTransport(boolean preferNativeTransport) {
        this.preferNativeTransport = preferNativeTransport;
    }

    public VirtualThreads getVirtualThreads() {
        return virtualThreads;
    }

    public void setVirtualThreads(VirtualThreads virtualThreads) {
        this.virtualThreads = virtualThreads;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public long getGracefulShutdownTimeoutSeconds() {
        return gracefulShutdownTimeoutSeconds;
    }

    public void setGracefulShutdownTimeoutSeconds(long gracefulShutdownTimeoutSeconds) {
        this.gracefulShutdownTimeoutSeconds = gracefulShutdownTimeoutSeconds;
    }

    public static class VirtualThreads {

        /**
         * Whether to use virtual threads for worker tasks.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Cluster {

        /**
         * Whether clustering is enabled.
         */
        private boolean enabled = false;

        /**
         * Bind address for the clustered event bus listener. This is a <em>separate</em> listener
         * from {@code server.port} — Vert.x creates it to carry node-to-node EventBus traffic.
         * <p>
         * Leave it unset (or {@code 0.0.0.0}) to bind all interfaces; in that case set
         * {@link #publicHost} to an address peers can actually dial, since the bind address is not
         * advertised as-is.
         */
        private String host = "0.0.0.0";

        /**
         * Port for the clustered event bus listener. {@code 0} lets Vert.x pick a free port, which
         * is fine when every node can reach every other node on any port; set a fixed port when a
         * firewall only permits a known one.
         */
        private int port = 0;

        /**
         * Address peers should use to reach this node's event bus listener. Required when
         * {@link #host} is a wildcard, and typically set when running behind NAT.
         */
        private String publicHost;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getPublicHost() {
            return publicHost;
        }

        public void setPublicHost(String publicHost) {
            this.publicHost = publicHost;
        }
    }
}
