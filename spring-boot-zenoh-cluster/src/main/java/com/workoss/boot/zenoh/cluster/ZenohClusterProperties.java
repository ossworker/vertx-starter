package com.workoss.boot.zenoh.cluster;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for the pure Spring Boot Zenoh cluster component.
 * <p>
 * Prefix: {@code zenoh.cluster}. The component has no Vert.x dependency; every key it publishes
 * lives under {@code zenoh/{app-name}/{environment}/...} so it cannot collide with the
 * {@code vertx/{app}/{env}/...} keyspace used by {@code vertx-spring-boot-cluster-zenoh} when both
 * modules share one Zenoh network.
 */
@ConfigurationProperties(prefix = "zenoh.cluster")
public class ZenohClusterProperties {

    /**
     * Whether the cluster component is active. Enabled by default: putting this module on the
     * classpath is an explicit opt-in, so {@code zenoh.cluster.enabled=false} is the way to
     * switch it off again.
     */
    private boolean enabled = true;

    /**
     * Application name for multi-tenant isolation.
     */
    private String appName = "default";

    /**
     * Environment label for multi-tenant isolation (e.g., dev, staging, production).
     */
    private String environment = "default";

    /**
     * Stable node id. When unset a random UUID is generated per start, which is the right choice
     * for ephemeral nodes; set it when restarts must be recognised as the same node.
     */
    private String nodeId;

    /**
     * Zenoh session mode: peer, client, or router.
     */
    private String mode = "peer";

    /**
     * Zenoh connect endpoints.
     */
    private List<String> connect = new ArrayList<>();

    /**
     * Zenoh listen endpoints.
     */
    private List<String> listen = new ArrayList<>();

    /**
     * Path to a Zenoh JSON5 configuration file. When set, mode/connect/listen are applied on top
     * of it.
     */
    private String configFile;

    /**
     * Liveliness heartbeat interval in milliseconds. The heartbeat refreshes this node's
     * {@code nodes/} key and re-publishes its metadata, which is also how late joiners learn
     * about peers.
     */
    private long heartbeatInterval = 5000L;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public List<String> getConnect() {
        return connect;
    }

    public void setConnect(List<String> connect) {
        this.connect = connect;
    }

    public List<String> getListen() {
        return listen;
    }

    public void setListen(List<String> listen) {
        this.listen = listen;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }
}
