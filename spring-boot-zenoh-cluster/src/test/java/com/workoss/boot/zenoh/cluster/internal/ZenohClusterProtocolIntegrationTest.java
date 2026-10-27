package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.NodeInfo;
import com.workoss.boot.zenoh.cluster.SharedMap;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport.Mode;
import com.workoss.zenoh.link.LinkProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.workoss.boot.zenoh.cluster.internal.ClusterTestSupport.await;

/**
 * Real sessions where only {@code zenoh.cluster.protocol} changes: both nodes use bare
 * {@code 127.0.0.1:port} endpoints, so each row proves the protocol switch alone moves the cluster
 * onto another link type. Router rows also cover the embedded router sharing its session with
 * the node, including QUIC/TLS listeners for which the node carries no client TLS material.
 */
@DisplayName("Zenoh cluster switches link protocol through configuration")
class ZenohClusterProtocolIntegrationTest {

    private static final long WAIT = 10_000L;

    private final List<DefaultZenohCluster> started = new ArrayList<>();

    @AfterEach
    void tearDown() {
        Collections.reverse(started);
        started.forEach(DefaultZenohCluster::stop);
    }

    @ParameterizedTest(name = "{0} listener in {1} mode")
    @CsvSource({
            "TCP,  PEER", "WS,   PEER", "TLS,  PEER", "QUIC, PEER",
            "TCP,  ROUTER", "WS,   ROUTER", "TLS,  ROUTER", "QUIC, ROUTER"})
    @DisplayName("membership and map replication over the configured protocol")
    void replicatesOverConfiguredProtocol(LinkProtocol protocol, Mode mode) throws Exception {
        String app = "it-proto-" + protocol.scheme() + "-" + UUID.randomUUID().toString().substring(0, 8);
        int port = ClusterTestSupport.freePort();
        DefaultZenohCluster a = start(ClusterTestSupport.listener(app, "node-a", mode, protocol, port));
        DefaultZenohCluster b = start(ClusterTestSupport.connector(app, "node-b", protocol, port));
        awaitMembers(a, "node-a", "node-b");
        awaitMembers(b, "node-a", "node-b");

        SharedMap<String, String> mapA = a.sharedMap("shared", ValueCodec.utf8());
        SharedMap<String, String> mapB = b.sharedMap("shared", ValueCodec.utf8());
        mapA.put("x", "1");
        await(() -> "1".equals(mapB.get("x")), WAIT, "B sees x=1 over " + protocol);
        mapB.put("y", "2");
        await(() -> "2".equals(mapA.get("y")), WAIT, "A sees y=2 over " + protocol);
    }

    private DefaultZenohCluster start(ClusterSettings settings) {
        DefaultZenohCluster cluster = new DefaultZenohCluster(settings);
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
