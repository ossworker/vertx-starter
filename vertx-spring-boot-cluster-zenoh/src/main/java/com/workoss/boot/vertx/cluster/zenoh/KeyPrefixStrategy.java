package com.workoss.boot.vertx.cluster.zenoh;

/**
 * Builds key prefixes for multi-tenant isolation in Zenoh.
 * All keys used by the cluster manager follow the pattern:
 * zenoh/{app-name}/{environment}/...
 *
 * Cluster keys: zenoh/{app}/{env}/cluster/...
 * EventBus keys: zenoh/{app}/{env}/eventbus/...
 */
public class KeyPrefixStrategy {

    private final String root;
    private final String clusterPrefix;

    public KeyPrefixStrategy(String appName, String environment) {
        this.root = "zenoh/" + sanitize(appName) + "/" + sanitize(environment);
        this.clusterPrefix = root + "/cluster";
    }

    // --- Cluster keys ---

    /**
     * Get the key for node liveliness.
     */
    public String nodeKey(String nodeId) {
        return root + "/nodes/" + nodeId;
    }

    /**
     * Get the key expression pattern for all nodes.
     */
    public String allNodesPattern() {
        return root + "/nodes/*";
    }

    /**
     * Get the key for node info.
     */
    public String nodeInfoKey(String nodeId) {
        return root + "/nodeinfo/" + nodeId;
    }

    /**
     * Get the key for an async map entry.
     */
    public String mapKey(String mapName, String entryKey) {
        return clusterPrefix + "/maps/" + sanitize(mapName) + "/" + entryKey;
    }

    /**
     * Get the key expression pattern for all entries in a map.
     */
    public String mapPattern(String mapName) {
        return clusterPrefix + "/maps/" + sanitize(mapName) + "/**";
    }

    /**
     * Get the key for a counter.
     */
    public String counterKey(String counterName) {
        return clusterPrefix + "/counters/" + sanitize(counterName);
    }

    /**
     * Get the key for a lock.
     */
    public String lockKey(String lockName) {
        return clusterPrefix + "/locks/" + sanitize(lockName);
    }

    /**
     * Get the key for EventBus handler registration.
     */
    public String registrationKey(String address, String nodeId) {
        return root + "/registrations/" + sanitize(address) + "/" + nodeId;
    }

    /**
     * Get the key expression pattern for all registrations of an address.
     */
    public String registrationPattern(String address) {
        return root + "/registrations/" + sanitize(address) + "/*";
    }

    /**
     * Get the key expression pattern for all registrations.
     */
    public String allRegistrationsPattern() {
        return root + "/registrations/**";
    }

    // --- EventBus keys ---

    /**
     * Get the inbox key for point-to-point messaging to a specific node.
     */
    public String eventBusInboxKey(String nodeId) {
        return root + "/eventbus/inbox/" + nodeId;
    }

    /**
     * Get the pub/sub key for broadcasting messages to an address.
     */
    public String eventBusPubKey(String address) {
        return root + "/eventbus/pub/" + sanitize(address);
    }

    /**
     * Get the reply key for request/reply patterns.
     */
    public String eventBusReplyKey(String replyId) {
        return root + "/eventbus/reply/" + replyId;
    }

    /**
     * Get the key expression pattern for all pub/sub addresses.
     */
    public String eventBusPubPattern() {
        return root + "/eventbus/pub/*";
    }

    /**
     * Get the key expression pattern for all registrations.
     */
    public String registrationsPattern() {
        return root + "/registrations/**";
    }

    // --- Accessors ---

    /**
     * Get the root prefix (zenoh/{app}/{env}).
     */
    public String getRoot() {
        return root;
    }

    /**
     * Get the cluster prefix (zenoh/{app}/{env}/cluster).
     */
    public String getPrefix() {
        return clusterPrefix;
    }

    private static String sanitize(String input) {
        if (input == null || input.isEmpty()) {
            return "default";
        }
        // Replace characters that could interfere with Zenoh key expressions
        return input.replaceAll("[/*?#\\[\\]]", "_");
    }
}
