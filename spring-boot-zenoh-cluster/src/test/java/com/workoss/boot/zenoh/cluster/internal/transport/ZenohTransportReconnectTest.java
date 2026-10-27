package com.workoss.boot.zenoh.cluster.internal.transport;

import com.workoss.zenoh.link.LinkProtocol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZenohTransportReconnectTest {

    @ParameterizedTest
    @EnumSource(value = LinkProtocol.class, names = {"TCP", "WS", "QUIC", "TLS"})
    void endpointChangesRestoreSubscribersAndQueryables(LinkProtocol protocol) throws Exception {
        int portA = freePort();
        int portB = freePort();
        String root = "reconnect/" + UUID.randomUUID();
        AtomicReference<String> receivedA = new AtomicReference<>();
        AtomicReference<String> receivedB = new AtomicReference<>();
        try (ZenohTransport a = ZenohTransport.open(settings(protocol, portA), "reconnect-a");
             ZenohTransport b = ZenohTransport.open(settings(protocol, portB), "reconnect-b")) {
            a.subscribe(root + "/a", sample -> receivedA.set(text(sample.payload())), "a-sub");
            AutoCloseable subscription = b.subscribe(root + "/b",
                    sample -> receivedB.set(text(sample.payload())), "b-sub");
            b.serve(root + "/query", query -> query.reply(root + "/query", bytes("reply")), "b-query");
            String initialId = b.sessionId();
            b.updateConnectEndpoints(List.of("127.0.0.1:" + portA));
            assertNotEquals(initialId, b.sessionId());
            await(() -> {
                a.put(root + "/b", bytes("first"));
                b.put(root + "/a", bytes("first"));
                return "first".equals(receivedA.get()) && "first".equals(receivedB.get());
            });
            await(() -> a.get(root + "/query").stream()
                    .anyMatch(reply -> reply.samplePayload().map(payload -> "reply".equals(text(payload)))
                            .orElse(false)));

            String connectedId = b.sessionId();
            b.updateConnectEndpoints(List.of("127.0.0.1:" + portA,
                    protocol.scheme() + "/127.0.0.1:" + portA));
            assertEquals(connectedId, b.sessionId());
            assertThrows(IllegalArgumentException.class, () -> b.updateConnectEndpoints(List.of("bad/")));
            assertEquals(connectedId, b.sessionId());

            b.updateConnectEndpoints(List.of());
            assertNotEquals(connectedId, b.sessionId());
            assertEquals("reply", text(b.get(root + "/query").getFirst().samplePayload().orElseThrow()));
            b.updateConnectEndpoints(List.of("127.0.0.1:" + portA));
            await(() -> {
                a.put(root + "/b", bytes("second"));
                return "second".equals(receivedB.get());
            });
            subscription.close();
            b.updateConnectEndpoints(List.of());
            assertEquals("reply", text(b.get(root + "/query").getFirst().samplePayload().orElseThrow()));
        }
    }

    private static ZenohTransport.Settings settings(LinkProtocol protocol, int port) throws Exception {
        Map<String, String> config = new HashMap<>(Map.of(
                "scouting/multicast/enabled", "false", "queries_default_timeout", "1000"));
        if (protocol.requiresTls()) {
            config.put("transport/link/tls/listen_certificate", pem("server-cert.pem"));
            config.put("transport/link/tls/listen_private_key", pem("server-key.pem"));
            config.put("transport/link/tls/root_ca_certificate", pem("ca.pem"));
        }
        return new ZenohTransport.Settings(ZenohTransport.Mode.PEER, protocol, List.of(),
                List.of("127.0.0.1:" + port), null, config);
    }

    private static String pem(String name) throws Exception {
        return "\"" + Path.of(ZenohTransportReconnectTest.class.getResource("/quic/" + name).toURI()) + "\"";
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Transport did not reconnect within 10 seconds");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
