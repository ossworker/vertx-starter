package com.vertx.spring.boot.cluster.zenoh;

/**
 * Builds key prefixes for multi-tenant isolation in Zenoh.
 * All keys used by the cluster manager follow the pattern:
 * vertx/{app-name}/{environment}/cluster/...
 */
public class KeyPrefixStrategy {

    private final String prefix;

    public KeyPrefixStrategy(String appName, String environment) {
        this.prefix = "vertx/" + sanitize(appName) + "/" + sanitize(environment) + "/cluster";
    }

    /**
     * Get the key for node liveliness.
     */
    public String nodeKey(String nodeId) {
        return prefix + "/nodes/" + nodeId;
    }

    /**
     * Get the key expression pattern for all nodes.
     */
    public String allNodesPattern() {
        return prefix + "/nodes/*";
    }

    /**
     * Get the key for node info.
     */
    public String nodeInfoKey(String nodeId) {
        return prefix + "/nodeinfo/" + nodeId;
    }

    /**
     * Get the key for an async map entry.
     */
    public String mapKey(String mapName, String entryKey) {
        return prefix + "/maps/" + sanitize(mapName) + "/" + entryKey;
    }

    /**
     * Get the key expression pattern for all entries in a map.
     */
    public String mapPattern(String mapName) {
        return prefix + "/maps/" + sanitize(mapName) + "/**";
    }

    /**
     * Get the key for a counter.
     */
    public String counterKey(String counterName) {
        return prefix + "/counters/" + sanitize(counterName);
    }

    /**
     * Get the key for a lock.
     */
    public String lockKey(String lockName) {
        return prefix + "/locks/" + sanitize(lockName);
    }

    /**
     * Get the key for EventBus handler registration.
     */
    public String registrationKey(String address, String nodeId) {
        return prefix + "/registrations/" + sanitize(address) + "/" + nodeId;
    }

    /**
     * Get the key expression pattern for all registrations of an address.
     */
    public String registrationPattern(String address) {
        return prefix + "/registrations/" + sanitize(address) + "/*";
    }

    /**
     * Get the key expression pattern for all registrations.
     */
    public String allRegistrationsPattern() {
        return prefix + "/registrations/**";
    }

    /**
     * Get the base prefix.
     */
    public String getPrefix() {
        return prefix;
    }

    private static String sanitize(String input) {
        if (input == null || input.isEmpty()) {
            return "default";
        }
        // Replace characters that could interfere with Zenoh key expressions
        return input.replaceAll("[/*?#\\[\\]]", "_");
    }
}
