package com.workoss.zenoh.link;

import com.workoss.zenoh.Config;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Zenoh unicast link protocols. Which ones actually work depends on the transports compiled into
 * the native library; see {@link #isSupported()}.
 */
public enum LinkProtocol {

    TCP("tcp", false),
    UDP("udp", false),
    /** TLS over TCP. */
    TLS("tls", true),
    /** QUIC: TLS 1.3 over UDP; Zenoh always requires certificates on it. */
    QUIC("quic", true),
    /** Plain WebSocket; Zenoh 1.x has no {@code wss}, put a TLS terminator in front instead. */
    WS("ws", false),
    UNIXSOCK_STREAM("unixsock-stream", false),
    UNIXPIPE("unixpipe", false),
    SERIAL("serial", false),
    VSOCK("vsock", false);

    private static volatile Set<String> supported;

    private final String scheme;
    private final boolean tls;

    LinkProtocol(String scheme, boolean tls) {
        this.scheme = scheme;
        this.tls = tls;
    }

    /** Endpoint scheme, e.g. {@code quic} in {@code quic/10.0.0.1:7447}. */
    public String scheme() {
        return scheme;
    }

    /** Whether Zenoh needs TLS material (certificate/key on listen, root CA on connect). */
    public boolean requiresTls() {
        return tls;
    }

    public boolean isSupported() {
        return supportedSchemes().contains(scheme);
    }

    /**
     * @throws IllegalArgumentException if {@code scheme} is not a Zenoh link protocol
     */
    public static LinkProtocol fromScheme(String scheme) {
        String normalized = scheme == null ? "" : scheme.trim().toLowerCase(Locale.ROOT);
        for (LinkProtocol protocol : values()) {
            if (protocol.scheme.equals(normalized)) {
                return protocol;
            }
        }
        throw new IllegalArgumentException("Unknown Zenoh link protocol '" + scheme
                + "'; known protocols are " + allSchemes());
    }

    /** Schemes compiled into the loaded native library, as reported by Zenoh itself. */
    public static Set<String> supportedSchemes() {
        Set<String> result = supported;
        if (result == null) {
            result = Collections.unmodifiableSet(new LinkedHashSet<String>(Config.supportedLinkProtocols()));
            supported = result;
        }
        return result;
    }

    private static Set<String> allSchemes() {
        Set<String> schemes = new LinkedHashSet<String>();
        for (LinkProtocol protocol : values()) {
            schemes.add(protocol.scheme);
        }
        return schemes;
    }
}
