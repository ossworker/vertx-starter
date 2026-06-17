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

    /**
     * Storage backend type: filesystem, rocksdb, s3, or none.
     * <ul>
     *   <li>filesystem - uses libzenoh_backend_fs plugin (local file storage, default)</li>
     *   <li>rocksdb - uses libzenoh_backend_rocksdb plugin (RocksDB storage)</li>
     *   <li>s3 - uses libzenoh_backend_s3 plugin (S3-compatible object storage)</li>
     *   <li>none - in-memory mode, no plugin loaded (sharedData not persistent)</li>
     * </ul>
     */
    private String backend = "none";

    /**
     * Additional plugin search directories for backend plugins.
     * <p>
     * The default search directory {@code ~/.zenoh/lib} is always included automatically.
     * Add extra directories here if plugins are installed in non-standard locations.
     * <p>
     * Example: {@code plugin-dir: [/opt/zenoh/plugins, /usr/local/lib/zenoh]}
     */
    private List<String> pluginDir = new ArrayList<>();

    /**
     * Directory name for filesystem/rocksdb backends to store data.
     * <p>
     * The filesystem backend stores data relative to its root directory
     * ({@code ~/.zenoh/zenoh_backend_fs/}), so the actual path will be
     * {@code ~/.zenoh/zenoh_backend_fs/<storage-dir>}.
     * <p>
     * Defaults to "zenoh-storage".
     */
    private String storageDir = "zenoh-storage";

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

    public String getBackend() {
        return backend;
    }

    public void setBackend(String backend) {
        this.backend = backend;
    }

    public List<String> getPluginDir() {
        return pluginDir;
    }

    public void setPluginDir(List<String> pluginDir) {
        this.pluginDir = pluginDir;
    }

    public String getStorageDir() {
        return storageDir;
    }

    public void setStorageDir(String storageDir) {
        this.storageDir = storageDir;
    }
}
