package com.workoss.zenoh.link;

/**
 * A Zenoh endpoint, {@code <protocol>/<address>[?<metadata>][#<config>]}, e.g.
 * {@code quic/10.0.0.1:7447?rel=0#initial_mtu=1200}. Only the protocol is interpreted here;
 * address syntax and parameters are validated by Zenoh when the session opens.
 */
public final class Endpoint {

    private final LinkProtocol protocol;
    private final String address;

    private Endpoint(LinkProtocol protocol, String address) {
        this.protocol = protocol;
        this.address = address;
    }

    /**
     * Parses a full endpoint.
     *
     * @throws IllegalArgumentException if the scheme is missing or unknown, or the address is empty
     */
    public static Endpoint parse(String endpoint) {
        String value = requireText(endpoint);
        int slash = value.indexOf('/');
        if (slash <= 0) {
            throw new IllegalArgumentException("Invalid endpoint '" + value
                    + "': expected <protocol>/<address>, e.g. tcp/127.0.0.1:7447");
        }
        return of(LinkProtocol.fromScheme(value.substring(0, slash)), value.substring(slash + 1), value);
    }

    /**
     * Parses {@code value} as a full endpoint, or, when it carries no protocol prefix (e.g.
     * {@code 10.0.0.1:7447}, {@code [::]:7447}, {@code /tmp/zenoh.sock}), binds it to
     * {@code defaultProtocol}. This lets one setting switch every bare address between protocols.
     */
    public static Endpoint resolve(String value, LinkProtocol defaultProtocol) {
        String text = requireText(value);
        if (text.indexOf('/') > 0) {
            return parse(text);
        }
        if (defaultProtocol == null) {
            throw new IllegalArgumentException("Endpoint '" + text + "' has no protocol and no default is set");
        }
        return of(defaultProtocol, text, defaultProtocol.scheme() + "/" + text);
    }

    public LinkProtocol protocol() {
        return protocol;
    }

    public String address() {
        return address;
    }

    /**
     * @throws IllegalArgumentException if the native library was built without this protocol
     */
    public Endpoint requireSupported() {
        if (!protocol.isSupported()) {
            throw new IllegalArgumentException("Endpoint '" + this + "' uses '" + protocol.scheme()
                    + "', which this zenoh-jni build does not include; supported protocols are "
                    + LinkProtocol.supportedSchemes());
        }
        return this;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Endpoint && toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return protocol.scheme() + "/" + address;
    }

    private static Endpoint of(LinkProtocol protocol, String address, String original) {
        if (address.isEmpty()) {
            throw new IllegalArgumentException("Invalid endpoint '" + original
                    + "': missing address after '" + protocol.scheme() + "/'");
        }
        return new Endpoint(protocol, address);
    }

    private static String requireText(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Endpoint must not be blank");
        }
        return value.trim();
    }
}
