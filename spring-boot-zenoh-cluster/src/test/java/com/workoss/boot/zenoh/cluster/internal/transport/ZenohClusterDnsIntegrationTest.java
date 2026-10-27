package com.workoss.boot.zenoh.cluster.internal.transport;

import com.workoss.boot.zenoh.cluster.ClusterMessage;
import com.workoss.boot.zenoh.cluster.ValueCodec;
import com.workoss.boot.zenoh.cluster.internal.ClusterSettings;
import com.workoss.boot.zenoh.cluster.internal.DefaultZenohCluster;
import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscoveryTest.Answer;
import com.workoss.boot.zenoh.cluster.internal.transport.DnsPeerDiscoveryTest.UdpDnsServer;
import com.workoss.zenoh.link.LinkProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

@Timeout(40)
class ZenohClusterDnsIntegrationTest {

    @Test
    void realDnsChangesRegroupPeersWithoutLosingStateOrApplicationIdentity() throws Exception {
        String app = "dns-it-" + UUID.randomUUID();
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        try (UdpDnsServer dns = new UdpDnsServer()) {
            DefaultZenohCluster a = new DefaultZenohCluster(settings(app, "node-a", "127.0.0.1", port, dns));
            DefaultZenohCluster b = new DefaultZenohCluster(settings(app, "node-b", "::1", port, dns));
            try {
                a.start();
                b.start();
                var selfA = a.members().getFirst();
                var selfB = b.members().getFirst();
                var mapA = a.sharedMap("shared", ValueCodec.utf8());
                var mapB = b.sharedMap("shared", ValueCodec.utf8());
                mapA.put("before-connect", "retained");
                AtomicReference<ClusterMessage> message = new AtomicReference<>();
                b.messaging().subscribe("dns/test", message::set);

                dns.answer.set(new Answer(List.of("127.0.0.1"), List.of("::1"), 0, 0, false));
                await(() -> a.members().size() == 2 && b.members().size() == 2);
                await(() -> "retained".equals(mapB.get("before-connect")));
                await(() -> {
                    a.messaging().broadcast("dns/test", "connected".getBytes(StandardCharsets.UTF_8));
                    return message.get() != null;
                });
                assertSame(mapA, a.sharedMap("shared", ValueCodec.utf8()));
                assertEquals(selfA, a.members().stream().filter(node -> node.nodeId().equals("node-a"))
                        .findFirst().orElseThrow());
                assertEquals(selfB, b.members().stream().filter(node -> node.nodeId().equals("node-b"))
                        .findFirst().orElseThrow());

                String connectedA = a.sessionId().orElseThrow();
                String connectedB = b.sessionId().orElseThrow();
                int beforeFailure = dns.queries.size();
                dns.answer.set(new Answer(List.of(), List.of(), 2, 2, false));
                await(() -> dns.queries.size() >= beforeFailure + 6);
                assertEquals(connectedA, a.sessionId().orElseThrow());
                assertEquals(connectedB, b.sessionId().orElseThrow());
                mapB.put("during-dns-failure", "delivered");
                await(() -> "delivered".equals(mapA.get("during-dns-failure")));

                dns.answer.set(Answer.empty());
                await(() -> !connectedA.equals(a.sessionId().orElseThrow())
                        && !connectedB.equals(b.sessionId().orElseThrow()));
                await(() -> a.members().size() == 1 && b.members().size() == 1);
                assertEquals("retained", mapA.get("before-connect"));
                mapA.put("while-disconnected", "repaired");
                dns.answer.set(new Answer(List.of("127.0.0.1"), List.of("::1"), 0, 0, false));
                await(() -> a.members().size() == 2 && b.members().size() == 2);
                await(() -> "repaired".equals(mapB.get("while-disconnected")));
                b.stop();
                dns.answer.set(new Answer(List.of("127.0.0.1"), List.of(), 0, 0, false));
                await(() -> a.members().size() == 1);
                assertFalse(b.isRunning());
            } finally {
                b.stop();
                a.stop();
            }
        }
    }

    private static ClusterSettings settings(String app, String nodeId, String ip, int port, UdpDnsServer dns) {
        String listen = (ip.contains(":") ? "[" + ip + "]" : ip) + ":" + port;
        var transport = new ZenohTransport.Settings(ZenohTransport.Mode.PEER, LinkProtocol.TCP,
                List.of(), List.of(listen), null,
                Map.of("scouting/multicast/enabled", "false", "queries_default_timeout", "1000"));
        var discovery = new DnsPeerDiscovery.Settings("peers.ns.svc.cluster.local.", port, 100L,
                ip, dns.server(), 200L);
        return new ClusterSettings(nodeId, app, "test", transport, 200L, 2_000L,
                60_000L, 0L, discovery);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("DNS peers did not converge within 10 seconds");
    }
}
