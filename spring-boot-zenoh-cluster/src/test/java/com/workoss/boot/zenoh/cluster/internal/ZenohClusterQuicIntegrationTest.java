package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.workoss.boot.zenoh.cluster.internal.ClusterTestSupport.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Zenoh sessions over QUIC (TLS-encrypted UDP): node A is a peer listening on a free port
 * with a certificate, node B connects to it as a client validating against the CA. Zenoh 1.x
 * refuses QUIC listeners without TLS material, so both sides carry it through
 * {@code zenoh.cluster.config}. Every test uses its own app name so leftover sessions cannot
 * interfere.
 */
@DisplayName("Zenoh cluster over QUIC links")
class ZenohClusterQuicIntegrationTest {

    private static final long WAIT = 10_000L;

    private final List<DefaultZenohCluster> started = new ArrayList<>();
    private String app;
    private int port;
    private DefaultZenohCluster a;
    private DefaultZenohCluster b;

    @BeforeEach
    void setUp() throws Exception {
        app = "it-quic-" + UUID.randomUUID().toString().substring(0, 8);
        port = ClusterTestSupport.freePort();
        a = start(new DefaultZenohCluster(ClusterTestSupport.quicPeerWithCert(app, "node-a", port)));
        b = start(new DefaultZenohCluster(ClusterTestSupport.quicClientWithCert(app, "node-b", port)));
        awaitMembers(a, "node-a", "node-b");
        awaitMembers(b, "node-a", "node-b");
    }

    @AfterEach
    void tearDown() {
        Collections.reverse(started);
        started.forEach(DefaultZenohCluster::stop);
    }

    @Test
    @DisplayName("membership and map replication work over QUIC in both directions")
    void dataPlaneReplicatesOverQuic() {
        SharedMap<String, String> mapA = a.sharedMap("shared", ValueCodec.utf8());
        SharedMap<String, String> mapB = b.sharedMap("shared", ValueCodec.utf8());
        mapA.put("x", "1");
        await(() -> "1".equals(mapB.get("x")), WAIT, "B sees x=1 over QUIC");
        mapB.put("y", "2");
        await(() -> "2".equals(mapA.get("y")), WAIT, "A sees y=2 over QUIC");
    }

    @Test
    @DisplayName("members of a QUIC cluster report each other's node ids")
    void membershipIsBidirectional() {
        Set<String> seenByA = Set.copyOf(a.members().stream().map(NodeInfo::nodeId).toList());
        assertEquals(Set.of("node-a", "node-b"), seenByA);
    }

    @Test
    @DisplayName("a QUIC client carrying extra zenoh.cluster.config joins the cluster")
    void configPassthroughAcceptedOnRealSession() {
        DefaultZenohCluster c = start(new DefaultZenohCluster(ClusterTestSupport.quicClientWithCert(
                app, "node-c", port, Map.of("transport/link/tx/keep_alive", "4",
                        "transport/link/protocols", "[\"tcp\",\"udp\",\"quic\",\"tls\"]"))));
        awaitMembers(a, "node-a", "node-b", "node-c");
        c.sharedMap("shared", ValueCodec.utf8()).put("from-c", "v");
        await(() -> "v".equals(a.sharedMap("shared", ValueCodec.utf8()).get("from-c")), WAIT,
                "A sees C's write made over QUIC with config passthrough");
    }

    @Test
    @DisplayName("a config entry with a wrong-typed value fails session open (config really reaches Zenoh)")
    void configPassthroughReachesNativeConfig() {
        // Falsification: if zenoh.cluster.config were NOT applied, this node would start happily.
        // keep_alive must be a number; a string value makes Zenoh reject the whole config at open.
        ClusterSettings settings = ClusterTestSupport.quicClientWithCert(app, "node-bad", port,
                Map.of("transport/link/tx/keep_alive", "\"not-a-number\""));
        DefaultZenohCluster bad = new DefaultZenohCluster(settings);
        RuntimeException e = assertThrows(RuntimeException.class, bad::start);
        assertTrue(e.getMessage() != null && !e.getMessage().isBlank(), e.toString());
    }

    @Test
    @DisplayName("a QUIC listener without TLS material fails naming the missing key")
    void quicListenerWithoutCertificateFails() {
        DefaultZenohCluster noCert = new DefaultZenohCluster(
                ClusterTestSupport.quicPeer(app, "node-nocert", port));
        RuntimeException e = assertThrows(RuntimeException.class, noCert::start);
        assertTrue(e.getMessage().contains("Missing TLS private key"),
                "expected the native TLS error, got: " + e.getMessage());
    }

    // --- Helpers ---

    private DefaultZenohCluster start(DefaultZenohCluster cluster) {
        cluster.start();
        started.add(cluster);
        return cluster;
    }

    private static void awaitMembers(DefaultZenohCluster cluster, String... ids) {
        Set<String> expected = Set.of(ids);
        await(() -> expected.equals(Set.copyOf(cluster.members().stream().map(NodeInfo::nodeId).toList())),
                WAIT, cluster.nodeId() + " sees members " + expected);
    }
}
