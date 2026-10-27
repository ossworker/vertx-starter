package com.workoss.boot.zenoh.cluster.internal.transport;

import com.workoss.boot.zenoh.cluster.internal.transport.ZenohTransport.Mode;
import com.workoss.zenoh.link.Endpoint;
import com.workoss.zenoh.link.LinkProtocol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Zenoh transport endpoint resolution and config passthrough")
class ZenohTransportEndpointTest {

    @Test
    @DisplayName("explicit endpoints of every linked protocol pass through unchanged, with parameters")
    void acceptsExplicitEndpoints() {
        List<String> endpoints = List.of(
                "tcp/127.0.0.1:7447",
                "tcp/[::]:7447",
                "udp/127.0.0.1:7448",
                "quic/zenoh.example:7447?rel=0#initial_mtu=1200",
                "tls/zenoh.example:7447",
                "ws/zenoh.example:7447",
                "unixsock-stream//tmp/zenoh.sock");
        assertEquals(endpoints, strings(ZenohTransport.resolveEndpoints(endpoints, LinkProtocol.TCP, "listen")));
    }

    @Test
    @DisplayName("bare addresses take zenoh.cluster.protocol, explicit ones keep their own")
    void bareAddressesFollowTheConfiguredProtocol() {
        List<String> endpoints = List.of("10.0.0.1:7447", "[::]:7447", "tcp/10.0.0.2:7447");
        assertEquals(List.of("quic/10.0.0.1:7447", "quic/[::]:7447", "tcp/10.0.0.2:7447"),
                strings(ZenohTransport.resolveEndpoints(endpoints, LinkProtocol.QUIC, "connect")));
        assertEquals(List.of("ws/10.0.0.1:7447", "ws/[::]:7447", "tcp/10.0.0.2:7447"),
                strings(ZenohTransport.resolveEndpoints(endpoints, LinkProtocol.WS, "connect")));
        assertEquals(List.of("unixsock-stream//tmp/zenoh.sock"), strings(ZenohTransport.resolveEndpoints(
                List.of("/tmp/zenoh.sock"), LinkProtocol.UNIXSOCK_STREAM, "listen")));
    }

    @Test
    @DisplayName("null and empty endpoint lists are accepted (scouting fallback)")
    void acceptsEmptyConfiguration() {
        assertTrue(ZenohTransport.resolveEndpoints(null, LinkProtocol.TCP, "connect").isEmpty());
        assertTrue(ZenohTransport.resolveEndpoints(List.of(), LinkProtocol.TCP, "listen").isEmpty());
    }

    @Test
    @DisplayName("unknown protocol is rejected, naming the property and the known protocols")
    void rejectsUnknownProtocol() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ZenohTransport.resolveEndpoints(List.of("tcps/127.0.0.1:7447"), LinkProtocol.TCP, "listen"));
        assertTrue(e.getMessage().contains("zenoh.cluster.listen"), e.getMessage());
        assertTrue(e.getMessage().contains("Unknown Zenoh link protocol 'tcps'"), e.getMessage());
        assertTrue(e.getMessage().contains("quic"), e.getMessage());
    }

    @Test
    @DisplayName("a protocol missing from the native build is rejected before Zenoh sees it")
    void rejectsProtocolNotCompiledIn() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ZenohTransport.resolveEndpoints(List.of("serial//dev/ttyUSB0"), LinkProtocol.TCP, "connect"));
        assertTrue(e.getMessage().contains("zenoh.cluster.connect"), e.getMessage());
        assertTrue(e.getMessage().contains("does not include"), e.getMessage());
    }

    @Test
    @DisplayName("protocol without an address is rejected")
    void rejectsMissingAddress() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ZenohTransport.resolveEndpoints(List.of("quic/"), LinkProtocol.TCP, "listen"));
        assertTrue(e.getMessage().contains("missing address after 'quic/'"), e.getMessage());
    }

    @Test
    @DisplayName("blank endpoint inside the list is rejected")
    void rejectsBlankEntry() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ZenohTransport.resolveEndpoints(Arrays.asList("tcp/127.0.0.1:7447", "  "),
                        LinkProtocol.TCP, "connect"));
        assertTrue(e.getMessage().contains("zenoh.cluster.connect"), e.getMessage());
        assertTrue(e.getMessage().contains("must not be blank"), e.getMessage());
    }

    @Test
    @DisplayName("router mode needs an explicit listen endpoint for non-IP protocols")
    void routerWithoutListenOnUnixSocketFails() {
        ZenohTransport.Settings settings = new ZenohTransport.Settings(Mode.ROUTER,
                LinkProtocol.UNIXSOCK_STREAM, List.of(), List.of(), null, Map.of());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ZenohTransport.open(settings, "test"));
        assertTrue(e.getMessage().contains("zenoh.cluster.listen is required"), e.getMessage());
    }

    @Test
    @DisplayName("a config entry whose value is not JSON5 fails fast, naming the key")
    void invalidConfigValueFailsFastWithKey() {
        ZenohTransport.Settings settings = new ZenohTransport.Settings(Mode.PEER, LinkProtocol.TCP, List.of(),
                List.of(), null, Map.of("transport/link/tx/keep_alive", "}{"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ZenohTransport.open(settings, "test"));
        assertTrue(e.getMessage().contains("transport/link/tx/keep_alive"), e.getMessage());
        assertTrue(e.getMessage().contains("valid JSON5"), e.getMessage());
    }

    private static List<String> strings(List<Endpoint> endpoints) {
        return endpoints.stream().map(Endpoint::toString).toList();
    }
}
