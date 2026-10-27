package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscovery;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport;
import com.workoss.zenoh.link.LinkProtocol;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class ClusterTestSupport {

    static final long HEARTBEAT_MILLIS = 200L;
    static final long FAILURE_TIMEOUT_MILLIS = 1_000L;

    private ClusterTestSupport() {
    }

    static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    static String endpoint(int port) {
        return "tcp/127.0.0.1:" + port;
    }

    static String quicEndpoint(int port) {
        return "quic/127.0.0.1:" + port;
    }

    static ClusterSettings peer(String app, String nodeId, int port) {
        return settings(app, nodeId, transport(ZenohTransport.Mode.PEER, List.of(),
                List.of(endpoint(port)), Map.of()));
    }

    static ClusterSettings client(String app, String nodeId, int port) {
        return settings(app, nodeId, transport(ZenohTransport.Mode.CLIENT,
                List.of(endpoint(port)), List.of(), Map.of()));
    }

    /**
     * A node listening on the bare address {@code 127.0.0.1:port}, bound to {@code protocol}
     * through {@code zenoh.cluster.protocol}; carries listener TLS material when the protocol
     * needs it.
     */
    static ClusterSettings listener(String app, String nodeId, ZenohTransport.Mode mode,
                                    LinkProtocol protocol, int port) {
        return settings(app, nodeId, new ZenohTransport.Settings(mode, protocol, List.of(),
                List.of("127.0.0.1:" + port), null, protocol.requiresTls() ? quicListenTlsConfig() : Map.of()));
    }

    /** A client connecting to the bare address {@code 127.0.0.1:port} over {@code protocol}. */
    static ClusterSettings connector(String app, String nodeId, LinkProtocol protocol, int port) {
        return settings(app, nodeId, new ZenohTransport.Settings(ZenohTransport.Mode.CLIENT, protocol,
                List.of("127.0.0.1:" + port), List.of(), null,
                protocol.requiresTls() ? quicConnectTlsConfig() : Map.of()));
    }

    /**
     * Listener material for QUIC and TLS links: Zenoh 1.x refuses either listener without a
     * certificate and private key. The fixtures under src/test/resources/quic are a CA-signed
     * pair for 127.0.0.1/localhost, valid for a century.
     */
    static Map<String, String> quicListenTlsConfig() {
        return Map.of("transport/link/tls/listen_certificate", pem("quic/server-cert.pem"),
                "transport/link/tls/listen_private_key", pem("quic/server-key.pem"));
    }

    /** QUIC/TLS connector material: the CA that signed the listener's certificate. */
    static Map<String, String> quicConnectTlsConfig() {
        return Map.of("transport/link/tls/root_ca_certificate", pem("quic/ca.pem"));
    }

    static ClusterSettings quicPeer(String app, String nodeId, int port) {
        return settings(app, nodeId, transport(ZenohTransport.Mode.PEER, List.of(),
                List.of(quicEndpoint(port)), Map.of()));
    }

    static ClusterSettings quicPeerWithCert(String app, String nodeId, int port) {
        return settings(app, nodeId, transport(ZenohTransport.Mode.PEER, List.of(),
                List.of(quicEndpoint(port)), quicListenTlsConfig()));
    }

    static ClusterSettings quicClientWithCert(String app, String nodeId, int port) {
        return settings(app, nodeId, transport(ZenohTransport.Mode.CLIENT,
                List.of(quicEndpoint(port)), List.of(), quicConnectTlsConfig()));
    }

    static ClusterSettings quicClientWithCert(String app, String nodeId, int port,
                                              Map<String, String> extraConfig) {
        Map<String, String> config = new LinkedHashMap<>(quicConnectTlsConfig());
        config.putAll(extraConfig);
        return settings(app, nodeId, transport(ZenohTransport.Mode.CLIENT,
                List.of(quicEndpoint(port)), List.of(), config));
    }

    private static ZenohTransport.Settings transport(ZenohTransport.Mode mode, List<String> connect,
                                                     List<String> listen, Map<String, String> config) {
        return new ZenohTransport.Settings(mode, LinkProtocol.TCP, connect, listen, null, config);
    }

    private static String pem(String resource) {
        try {
            Path path = Path.of(ClusterTestSupport.class.getResource("/" + resource).toURI());
            // Zenoh reads the value as JSON5, so the path itself must be a quoted JSON string.
            return "\"" + path.toAbsolutePath() + "\"";
        } catch (URISyntaxException e) {
            throw new IllegalStateException("test resource not found: " + resource, e);
        }
    }

    private static ClusterSettings settings(String app, String nodeId, ZenohTransport.Settings transport) {
        return new ClusterSettings(nodeId, app, "test", transport, HEARTBEAT_MILLIS, FAILURE_TIMEOUT_MILLIS,
                60_000L, 0L, new DnsPeerDiscovery.Settings(null, 7447, 5_000L, null, null, 2_000L));
    }

    static void await(BooleanSupplier condition, long timeoutMillis, String description) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Not satisfied within " + timeoutMillis + " ms: " + description);
    }
}
