package com.workoss.zenoh.link;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.EmbeddedRouter;
import com.workoss.zenoh.Sample;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Link protocols, endpoints and router-shared sessions")
class LinkTransportTest {

    @Test
    @DisplayName("native library reports the transports it was compiled with")
    void reportsCompiledTransports() {
        Set<String> supported = LinkProtocol.supportedSchemes();
        for (String scheme : new String[]{"tcp", "udp", "tls", "quic", "ws", "unixsock-stream"}) {
            assertTrue(supported.contains(scheme), scheme + " missing from " + supported);
        }
        assertFalse(LinkProtocol.SERIAL.isSupported());
    }

    @Test
    @DisplayName("bare addresses take the default protocol, explicit schemes win")
    void resolvesBareAndExplicitEndpoints() {
        assertEquals("quic/10.0.0.1:7447", Endpoint.resolve("10.0.0.1:7447", LinkProtocol.QUIC).toString());
        assertEquals("ws/[::]:7447", Endpoint.resolve("[::]:7447", LinkProtocol.WS).toString());
        assertEquals("unixsock-stream//tmp/z.sock",
                Endpoint.resolve("/tmp/z.sock", LinkProtocol.UNIXSOCK_STREAM).toString());
        Endpoint explicit = Endpoint.resolve("tcp/10.0.0.1:7447?rel=1", LinkProtocol.QUIC);
        assertEquals(LinkProtocol.TCP, explicit.protocol());
        assertEquals("10.0.0.1:7447?rel=1", explicit.address());
    }

    @Test
    @DisplayName("malformed endpoints fail with a precise message")
    void rejectsMalformedEndpoints() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("10.0.0.1:7447"))
                .getMessage().contains("expected <protocol>/<address>"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("tcps/h:1"))
                .getMessage().contains("Unknown Zenoh link protocol 'tcps'"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("quic/"))
                .getMessage().contains("missing address after 'quic/'"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("serial/ttyS0").requireSupported())
                .getMessage().contains("does not include"));
    }

    @Test
    @DisplayName("constructor failures carry the native error instead of 'Constructor failed'")
    void constructorErrorsAreDescriptive() throws IOException {
        try (Config config = Config.defaultConfig()) {
            config.setMode("router");
            config.setListenEndpoints(Collections.singletonList("quic/127.0.0.1:" + freePort()));
            RuntimeException e = assertThrows(RuntimeException.class, () -> new EmbeddedRouter(config));
            assertTrue(e.getMessage().contains("Missing TLS private key"), e.getMessage());
        }
    }

    @Test
    @DisplayName("a router-shared session publishes to a client connected over WebSocket")
    void routerSharedSessionOverWebSocket() throws Exception {
        String endpoint = "ws/127.0.0.1:" + freePort();
        try (Config routerConfig = Config.defaultConfig()) {
            routerConfig.setMode("router");
            routerConfig.setListenEndpoints(Collections.singletonList(endpoint));
            try (EmbeddedRouter router = new EmbeddedRouter(routerConfig);
                 Session shared = new Session(router);
                 Config clientConfig = Config.defaultConfig()) {
                assertEquals(router.zid(), shared.zid());
                clientConfig.setMode("client");
                clientConfig.setConnectEndpoints(Collections.singletonList(endpoint));
                try (Session client = new Session(clientConfig);
                     Subscriber subscriber = new Subscriber(client, "link/test/**")) {
                    Optional<Sample> received = Optional.empty();
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (!received.isPresent() && System.currentTimeMillis() < deadline) {
                        shared.put("link/test/ws", "over-ws".getBytes(StandardCharsets.UTF_8));
                        Thread.sleep(100);
                        received = subscriber.tryRecv();
                    }
                    assertTrue(received.isPresent(), "client never received the router's put over ws");
                    assertEquals("over-ws", new String(received.get().payload(), StandardCharsets.UTF_8));
                    subscriber.undeclare();
                }
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
