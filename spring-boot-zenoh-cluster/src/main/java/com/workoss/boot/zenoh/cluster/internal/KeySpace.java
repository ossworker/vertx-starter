package com.workoss.boot.zenoh.cluster.internal;

/**
 * Zenoh keyspace of one cluster: {@code zenoh/{app}/{env}/...}. Names (app, env, node id,
 * broadcast address, map / counter / lock name) keep {@code /} as a chunk separator and have each
 * segment {@link KeyEscaper#escapePath escaped}, so they can never inject wildcards. Data keys
 * inside the store (map entry keys, counter slots) stay a single escaped chunk, because the store
 * splits {@code namespace/entry} at the last {@code /}.
 * <pre>
 * members/{nodeId...}              heartbeat carrying the node's {@code NodeInfo}
 * store/{namespace...}/{entry}     replicated store; namespace is maps/{name...}, counters/{name...}
 *                                  or locks/{name...} (whose only entry is {@value #LOCK_ENTRY})
 * bus/broadcast/{address...}       broadcast messages
 * bus/inbox/{nodeId...}            direct messages
 * </pre>
 */
public final class KeySpace {

    public static final String LOCK_ENTRY = "holder";

    private final String root;
    private final String storePrefix;
    private final String membersPrefix;
    private final String broadcastPrefix;

    public KeySpace(String appName, String environment) {
        this.root = "zenoh/" + KeyEscaper.escapePath(orDefault(appName)) + "/"
                + KeyEscaper.escapePath(orDefault(environment));
        this.storePrefix = root + "/store";
        this.membersPrefix = root + "/members/";
        this.broadcastPrefix = root + "/bus/broadcast/";
    }

    public String root() {
        return root;
    }

    public String memberKey(String nodeId) {
        return membersPrefix + KeyEscaper.escapePath(nodeId);
    }

    public String membersPattern() {
        return membersPrefix + "**";
    }

    /**
     * Node id carried by a {@link #memberKey member key}, or {@code null} for any other key.
     */
    public String memberId(String key) {
        return suffix(membersPrefix, key);
    }

    /**
     * {@code zenoh/{app}/{env}/store}, without a trailing slash.
     */
    public String storePrefix() {
        return storePrefix;
    }

    public String storePattern() {
        return storePrefix + "/**";
    }

    public String broadcastKey(String address) {
        return broadcastPrefix + KeyEscaper.escapePath(address);
    }

    public String broadcastPattern() {
        return broadcastPrefix + "**";
    }

    /**
     * Address carried by a {@link #broadcastKey broadcast key}, or {@code null} for any other key.
     */
    public String broadcastAddress(String key) {
        return suffix(broadcastPrefix, key);
    }

    public String inboxKey(String nodeId) {
        return root + "/bus/inbox/" + KeyEscaper.escapePath(nodeId);
    }

    public static String mapNamespace(String mapName) {
        return "maps/" + KeyEscaper.escapePath(mapName);
    }

    public static String counterNamespace(String counterName) {
        return "counters/" + KeyEscaper.escapePath(counterName);
    }

    public static String lockNamespace(String lockName) {
        return "locks/" + KeyEscaper.escapePath(lockName);
    }

    private static String suffix(String prefix, String key) {
        if (key == null || !key.startsWith(prefix) || key.length() == prefix.length()) {
            return null;
        }
        return KeyEscaper.unescapePath(key.substring(prefix.length()));
    }

    private static String orDefault(String value) {
        return value == null || value.isBlank() ? "default" : value;
    }
}
