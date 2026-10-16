package com.workoss.boot.zenoh.cluster;

/**
 * Builds key prefixes for multi-tenant isolation in Zenoh.
 * All keys used by this component follow the pattern:
 * {@code zenoh/{app-name}/{environment}/...}
 *
 * <ul>
 *   <li>Cluster keys: {@code zenoh/{app}/{env}/nodes/...}, {@code zenoh/{app}/{env}/nodeinfo/...}</li>
 *   <li>Store keys: {@code zenoh/{app}/{env}/store/maps/...}, {@code .../counters/...}, {@code .../locks/...}</li>
 *   <li>Message keys: {@code zenoh/{app}/{env}/bus/broadcast/...}, {@code zenoh/{app}/{env}/bus/inbox/...}</li>
 * </ul>
 * Different apps/environments sharing the same Zenoh network are completely isolated.
 */
public class KeyPrefixStrategy {

    private final String root;
    private final String storePrefix;

    public KeyPrefixStrategy(String appName, String environment) {
        this.root = "zenoh/" + sanitize(appName) + "/" + sanitize(environment);
        this.storePrefix = root + "/store";
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
        return root + "/nodes/**";
    }

    /**
     * Get the key for node metadata.
     */
    public String nodeInfoKey(String nodeId) {
        return root + "/nodeinfo/" + nodeId;
    }

    /**
     * Get the key expression pattern for all node metadata.
     */
    public String allNodeInfoPattern() {
        return root + "/nodeinfo/**";
    }

    // --- Store keys (shared maps, counters, locks) ---

    /**
     * Get the key for a shared map entry.
     */
    public String mapKey(String mapName, String entryKey) {
        return storePrefix + "/maps/" + sanitize(mapName) + "/" + entryKey;
    }

    /**
     * Get the key for a counter.
     */
    public String counterKey(String counterName) {
        return storePrefix + "/counters/" + sanitize(counterName);
    }

    /**
     * Get the key for a lock.
     */
    public String lockKey(String lockName) {
        return storePrefix + "/locks/" + sanitize(lockName);
    }

    // --- Message keys ---

    /**
     * Get the key for broadcasting a message to an address.
     */
    public String busBroadcastKey(String address) {
        return root + "/bus/broadcast/" + sanitize(address);
    }

    /**
     * Get the key expression pattern for all broadcast addresses.
     */
    public String busBroadcastPattern() {
        return root + "/bus/broadcast/**";
    }

    /**
     * Get the inbox key for point-to-point messaging to a specific node.
     */
    public String busInboxKey(String nodeId) {
        return root + "/bus/inbox/" + nodeId;
    }

    // --- Accessors ---

    /**
     * Get the root prefix ({@code zenoh/{app}/{env}}).
     */
    public String getRoot() {
        return root;
    }

    /**
     * Get the store prefix ({@code zenoh/{app}/{env}/store}).
     */
    public String getStorePrefix() {
        return storePrefix;
    }

    /**
     * Get the key expression pattern covering the whole replicated store.
     */
    public String storePattern() {
        return storePrefix + "/**";
    }

    private static String sanitize(String input) {
        if (input == null || input.isEmpty()) {
            return "default";
        }
        // Replace characters that could interfere with Zenoh key expressions
        return input.replaceAll("[/*?#\\[\\]]", "_");
    }
}
