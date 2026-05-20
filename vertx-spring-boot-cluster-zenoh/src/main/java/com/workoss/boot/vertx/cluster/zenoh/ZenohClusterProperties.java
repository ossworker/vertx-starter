package com.workoss.boot.vertx.cluster.zenoh;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for Zenoh cluster manager.
 */
@ConfigurationProperties(prefix = "vertx.cluster.zenoh")
public class ZenohClusterProperties {

    /**
     * Application name for multi-tenant isolation.
     */
    private String appName = "default";

    /**
     * Environment label for multi-tenant isolation (e.g., dev, staging, production).
     */
    private String environment = "default";

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
     * Path to Zenoh JSON5 configuration file.
     */
    private String configFile;

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
}
