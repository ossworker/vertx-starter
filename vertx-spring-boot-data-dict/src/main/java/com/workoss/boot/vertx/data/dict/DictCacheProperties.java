package com.workoss.boot.vertx.data.dict;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for dictionary data two-level cache.
 * <p>
 * Zenoh keys are automatically isolated by app-name and environment
 * to prevent data leakage across applications or environments.
 */
@ConfigurationProperties(prefix = "vertx.data.dict")
public class DictCacheProperties {

    /**
     * Whether to enable dictionary data cache.
     */
    private boolean enabled = true;

    /**
     * L1 Caffeine cache maximum size.
     */
    private int l1MaxSize = 10000;

    /**
     * L1 Caffeine cache expire time in seconds.
     */
    private int l1ExpireSeconds = 300;

    /**
     * Application name for multi-tenant Zenoh key isolation.
     * Defaults to the value configured in vertx.cluster.zenoh.app-name.
     */
    private String appName;

    /**
     * Environment label for multi-tenant Zenoh key isolation.
     * Defaults to the value configured in vertx.cluster.zenoh.environment.
     */
    private String environment;

    /**
     * L2 Zenoh cache key prefix (relative to the app/env root).
     * Full key: vertx/{app}/{env}/{prefix}
     */
    private String zenohKeyPrefix = "dict";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getL1MaxSize() {
        return l1MaxSize;
    }

    public void setL1MaxSize(int l1MaxSize) {
        this.l1MaxSize = l1MaxSize;
    }

    public int getL1ExpireSeconds() {
        return l1ExpireSeconds;
    }

    public void setL1ExpireSeconds(int l1ExpireSeconds) {
        this.l1ExpireSeconds = l1ExpireSeconds;
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

    public String getZenohKeyPrefix() {
        return zenohKeyPrefix;
    }

    public void setZenohKeyPrefix(String zenohKeyPrefix) {
        this.zenohKeyPrefix = zenohKeyPrefix;
    }

    /**
     * Build the fully qualified Zenoh key prefix with app/env isolation.
     * Format: vertx/{app}/{env}/{prefix}/
     */
    public String resolveZenohPrefix() {
        String app = sanitize(appName != null ? appName : "default");
        String env = sanitize(environment != null ? environment : "default");
        String prefix = sanitize(zenohKeyPrefix);
        return "vertx/" + app + "/" + env + "/" + prefix + "/";
    }

    private static String sanitize(String input) {
        return input.replaceAll("[/*?#\\[\\]]", "_");
    }
}
