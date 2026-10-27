package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.internal.ClusterSettings;
import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscovery;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import com.workoss.zenoh.link.LinkProtocol;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ConfigurationProperties(prefix = "zenoh.cluster")
public class ZenohClusterProperties {

    /**
     * Whether to join the cluster. Adding the module to the classpath is an explicit opt-in, so
     * this defaults to true.
     */
    private boolean enabled = true;

    /**
     * Application name; nodes only see each other within the same application and environment.
     * Defaults to {@code spring.application.name}, or {@code default} when that is not set either.
     */
    private String appName;

    /**
     * Cluster environment; isolates nodes of different environments. An explicit value wins;
     * otherwise it defaults to the first active Spring profile
     * ({@code spring.profiles.active}), or {@code default} when none is active.
     */
    private String environment;

    /**
     * Stable node id; must be unique in the cluster. Defaults to a random UUID per application
     * context.
     */
    private String nodeId;

    /**
     * Zenoh session mode. {@code router} also starts an embedded router listening on {@link #listen}.
     */
    private ZenohTransport.Mode mode = ZenohTransport.Mode.PEER;

    /**
     * Link protocol for every {@link #connect}/{@link #listen} endpoint written without one, so
     * switching the whole cluster is a one-line change: {@code tcp}, {@code udp}, {@code tls}
     * (TLS over TCP), {@code quic} (TLS 1.3 over UDP), {@code ws} (plain WebSocket; Zenoh has no
     * wss) or {@code unixsock-stream}. {@code tls} and {@code quic} need {@link #tls} material.
     * Endpoints that carry an explicit {@code <protocol>/} prefix keep it.
     */
    private LinkProtocol protocol = LinkProtocol.TCP;

    /**
     * Endpoints to connect to, either bare ({@code 10.0.0.1:7447}, bound to {@link #protocol}) or
     * explicit ({@code quic/10.0.0.1:7447}). Endpoint parameters pass through to Zenoh, e.g.
     * {@code quic/10.0.0.1:7447?rel=0#initial_mtu=1200}.
     */
    private List<String> connect = new ArrayList<>();

    /**
     * Endpoints to listen on; same forms as {@link #connect}. In {@code router} mode it defaults
     * to {@code [::]:7447} over {@link #protocol}.
     */
    private List<String> listen = new ArrayList<>();

    /**
     * Optional Zenoh JSON5 configuration file; mode and endpoints above are applied on top of it.
     */
    private String configFile;

    /**
     * Arbitrary Zenoh configuration applied on top of {@link #configFile} as JSON5 values. Keys
     * are Zenoh config paths; explicit {@link #mode}, {@link #connect} and {@link #listen} win
     * over entries targeting the same underlying keys, and {@link #scouting} and {@link #tls}
     * entries are added first so anything here overrides them. For advanced tuning only: because
     * Spring Boot canonicalises map keys, a path containing {@code /} MUST be written in bracket
     * form, e.g.
     * {@code "[transport/link/tls/listen_certificate]"}, or it is silently mangled. Prefer
     * {@link #tls} for the common TLS material. Example for restricting link schemes:
     * <pre>
     * zenoh:
     *   cluster:
     *     config:
     *       "[transport/link/protocols]": '["quic"]'
     * </pre>
     */
    private Map<String, String> config = new LinkedHashMap<>();

    /**
     * Convenience TLS material for {@code quic} and {@code tls} links. Zenoh 1.x refuses such a
     * listener without a certificate and private key, and a connector without a root CA cannot
     * validate the peer.
     * Values are plain file paths and are quoted into JSON5 automatically, so no manual quoting
     * or bracket syntax is needed. Entries here are added to {@link #config} first, so the raw
     * {@code config} map can still override them for full control.
     */
    private final Tls tls = new Tls();

    /**
     * Zenoh scouting (peer discovery). Zenoh enables both multicast and gossip by default; gossip
     * propagates known peers along established links (notably through a shared router) and is what
     * discovery relies on where multicast is unavailable (Kubernetes, WAN). Typical cloud setup:
     * every node connects to the router and multicast is switched off.
     */
    private final Scouting scouting = new Scouting();
    private final Dns dns = new Dns();

    private Duration heartbeatInterval = Duration.ofSeconds(5);

    /**
     * How long a peer may stay silent before it is declared gone and its locks are reclaimed.
     * Defaults to three heartbeat intervals.
     */
    private Duration failureTimeout;

    /**
     * How long deletes are remembered so that late writes cannot resurrect entries. Should exceed
     * the longest partition you expect to heal from.
     */
    private Duration tombstoneRetention = Duration.ofMinutes(10);

    /**
     * Interval of the periodic full resync with peers; zero disables it.
     */
    private Duration antiEntropyInterval = Duration.ofMinutes(5);

    ClusterSettings toSettings(Environment springEnvironment) {
        String id = nodeId == null || nodeId.isBlank() ? UUID.randomUUID().toString() : nodeId;
        String app = appName == null || appName.isBlank()
                ? springEnvironment.getProperty("spring.application.name", "default")
                : appName;
        String env = environment;
        if (env == null || env.isBlank()) {
            String[] activeProfiles = springEnvironment.getActiveProfiles();
            env = activeProfiles.length > 0 ? activeProfiles[0] : "default";
        }
        long heartbeat = heartbeatInterval.toMillis();
        long timeout = failureTimeout == null ? heartbeat * 3 : failureTimeout.toMillis();
        return new ClusterSettings(id, app, env,
                new ZenohTransport.Settings(mode, protocol, connect, listen, configFile, buildConfig()),
                heartbeat, timeout, tombstoneRetention.toMillis(), antiEntropyInterval.toMillis(),
                new DnsPeerDiscovery.Settings(dns.getName(), dns.getPort(), dns.getRefreshInterval().toMillis(),
                        dns.getSelfAddress(), dns.getServer(), dns.getTimeout().toMillis()));
    }

    /**
     * Merges the {@link #tls} convenience entries (as quoted JSON5 strings) with the raw
     * {@link #config} map. The raw map is applied last so it can override the convenience entries.
     */
    private Map<String, String> buildConfig() {
        Map<String, String> merged = new LinkedHashMap<>();
        addScoutingEntries(merged);
        addTlsEntry(merged, "transport/link/tls/listen_certificate", tls.getListenCertificate());
        addTlsEntry(merged, "transport/link/tls/listen_private_key", tls.getListenPrivateKey());
        addTlsEntry(merged, "transport/link/tls/root_ca_certificate", tls.getRootCaCertificate());
        addTlsEntry(merged, "transport/link/tls/connect_certificate", tls.getConnectCertificate());
        addTlsEntry(merged, "transport/link/tls/connect_private_key", tls.getConnectPrivateKey());
        if (tls.getEnableMtls() != null) {
            merged.put("transport/link/tls/enable_mtls", tls.getEnableMtls() ? "true" : "false");
        }
        if (config != null) {
            merged.putAll(config);
        }
        return Map.copyOf(merged);
    }

    private void addScoutingEntries(Map<String, String> target) {
        addBooleanEntry(target, "scouting/multicast/enabled", scouting.getMulticastEnabled());
        addBooleanEntry(target, "scouting/gossip/enabled", scouting.getGossipEnabled());
        addBooleanEntry(target, "scouting/gossip/multihop", scouting.getGossipMultihop());
        String autoconnect = scouting.getGossipAutoconnect();
        if (autoconnect != null && !autoconnect.isBlank()) {
            String matcher = "\"" + autoconnect.trim() + "\"";
            // Routers do not autoconnect by design; only peer and client honour the matcher.
            target.put("scouting/gossip/autoconnect/peer", matcher);
            target.put("scouting/gossip/autoconnect/client", matcher);
        }
    }

    private static void addBooleanEntry(Map<String, String> target, String key, Boolean value) {
        if (value != null) {
            target.put(key, value ? "true" : "false");
        }
    }

    private static void addTlsEntry(Map<String, String> target, String key, String path) {
        if (path != null && !path.isBlank()) {
            target.put(key, "\"" + path.trim() + "\"");
        }
    }

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

    public ZenohTransport.Mode getMode() {
        return mode;
    }

    public void setMode(ZenohTransport.Mode mode) {
        this.mode = mode;
    }

    public LinkProtocol getProtocol() {
        return protocol;
    }

    public void setProtocol(LinkProtocol protocol) {
        this.protocol = protocol;
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

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public Duration getFailureTimeout() {
        return failureTimeout;
    }

    public void setFailureTimeout(Duration failureTimeout) {
        this.failureTimeout = failureTimeout;
    }

    public Duration getTombstoneRetention() {
        return tombstoneRetention;
    }

    public void setTombstoneRetention(Duration tombstoneRetention) {
        this.tombstoneRetention = tombstoneRetention;
    }

    public Duration getAntiEntropyInterval() {
        return antiEntropyInterval;
    }

    public void setAntiEntropyInterval(Duration antiEntropyInterval) {
        this.antiEntropyInterval = antiEntropyInterval;
    }

    public Tls getTls() {
        return tls;
    }

    public void setTls(Tls tls) {
        if (tls != null) {
            this.tls.copyFrom(tls);
        }
    }

    public Scouting getScouting() {
        return scouting;
    }

    public void setScouting(Scouting scouting) {
        if (scouting != null) {
            this.scouting.copyFrom(scouting);
        }
    }

    public Dns getDns() {
        return dns;
    }

    public static class Dns {
        private String name;
        private int port = 7447;
        private Duration refreshInterval = Duration.ofSeconds(5);
        private String selfAddress;
        private String server;
        private Duration timeout = Duration.ofSeconds(2);

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public Duration getRefreshInterval() {
            return refreshInterval;
        }

        public void setRefreshInterval(Duration refreshInterval) {
            this.refreshInterval = refreshInterval;
        }

        public String getSelfAddress() {
            return selfAddress;
        }

        public void setSelfAddress(String selfAddress) {
            this.selfAddress = selfAddress;
        }

        public String getServer() {
            return server;
        }

        public void setServer(String server) {
            this.server = server;
        }

        public Duration getTimeout() {
            return timeout;
        }

        public void setTimeout(Duration timeout) {
            this.timeout = timeout;
        }
    }

    /**
     * Zenoh scouting switches; unset values keep the Zenoh defaults (multicast and gossip both
     * on, gossip single-hop, autoconnect to router|peer|client). See {@link #scouting}.
     */
    public static class Scouting {

        private Boolean multicastEnabled;
        private Boolean gossipEnabled;
        private Boolean gossipMultihop;
        private String gossipAutoconnect;

        void copyFrom(Scouting other) {
            this.multicastEnabled = other.multicastEnabled;
            this.gossipEnabled = other.gossipEnabled;
            this.gossipMultihop = other.gossipMultihop;
            this.gossipAutoconnect = other.gossipAutoconnect;
        }

        public Boolean getMulticastEnabled() {
            return multicastEnabled;
        }

        public void setMulticastEnabled(Boolean multicastEnabled) {
            this.multicastEnabled = multicastEnabled;
        }

        public Boolean getGossipEnabled() {
            return gossipEnabled;
        }

        public void setGossipEnabled(Boolean gossipEnabled) {
            this.gossipEnabled = gossipEnabled;
        }

        public Boolean getGossipMultihop() {
            return gossipMultihop;
        }

        public void setGossipMultihop(Boolean gossipMultihop) {
            this.gossipMultihop = gossipMultihop;
        }

        /**
         * WhatAmI matcher of peers a node autoconnects to once gossip reveals them, e.g.
         * {@code router|peer} (Zenoh default) or {@code router} to only follow routers.
         */
        public String getGossipAutoconnect() {
            return gossipAutoconnect;
        }

        public void setGossipAutoconnect(String gossipAutoconnect) {
            this.gossipAutoconnect = gossipAutoconnect;
        }
    }

    /**
     * TLS material for QUIC/TLS links, expressed as plain file paths. See {@link #tls}.
     */
    public static class Tls {

        private String listenCertificate;
        private String listenPrivateKey;
        private String rootCaCertificate;
        private String connectCertificate;
        private String connectPrivateKey;
        private Boolean enableMtls;

        void copyFrom(Tls other) {
            this.listenCertificate = other.listenCertificate;
            this.listenPrivateKey = other.listenPrivateKey;
            this.rootCaCertificate = other.rootCaCertificate;
            this.connectCertificate = other.connectCertificate;
            this.connectPrivateKey = other.connectPrivateKey;
            this.enableMtls = other.enableMtls;
        }

        public String getListenCertificate() {
            return listenCertificate;
        }

        public void setListenCertificate(String listenCertificate) {
            this.listenCertificate = listenCertificate;
        }

        public String getListenPrivateKey() {
            return listenPrivateKey;
        }

        public void setListenPrivateKey(String listenPrivateKey) {
            this.listenPrivateKey = listenPrivateKey;
        }

        public String getRootCaCertificate() {
            return rootCaCertificate;
        }

        public void setRootCaCertificate(String rootCaCertificate) {
            this.rootCaCertificate = rootCaCertificate;
        }

        public String getConnectCertificate() {
            return connectCertificate;
        }

        public void setConnectCertificate(String connectCertificate) {
            this.connectCertificate = connectCertificate;
        }

        public String getConnectPrivateKey() {
            return connectPrivateKey;
        }

        public void setConnectPrivateKey(String connectPrivateKey) {
            this.connectPrivateKey = connectPrivateKey;
        }

        public Boolean getEnableMtls() {
            return enableMtls;
        }

        public void setEnableMtls(Boolean enableMtls) {
            this.enableMtls = enableMtls;
        }
    }
}
