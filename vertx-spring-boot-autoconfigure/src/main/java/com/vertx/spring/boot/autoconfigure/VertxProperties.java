package com.vertx.spring.boot.autoconfigure;

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
    private boolean preferNativeTransport = true;

    /**
     * Virtual thread settings.
     */
    private VirtualThreads virtualThreads = new VirtualThreads();

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
         * The host for cluster communication.
         */
        private String host = "0.0.0.0";

        /**
         * The port for cluster communication. 0 means auto-assign.
         */
        private int port = 0;

        /**
         * The public host for cluster communication (useful behind NAT).
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
