package com.workoss.boot.zenoh.cluster;

import com.workoss.boot.zenoh.cluster.internal.BuiltinCodecs;

/**
 * Wire codec for shared-map values.
 * <p>
 * {@link #jdk()} deserializes whatever peers send, so it is only appropriate when every node on
 * the Zenoh network is trusted; prefer a data format codec otherwise.
 *
 * @param <V> value type
 */
public interface ValueCodec<V> {

    byte[] encode(V value);

    /**
     * @return the value, or {@code null} for an unusable payload
     */
    V decode(byte[] data);

    /**
     * JDK serialization; values must be {@link java.io.Serializable}. Always the same instance.
     */
    @SuppressWarnings("unchecked")
    static <V> ValueCodec<V> jdk() {
        return (ValueCodec<V>) BuiltinCodecs.JDK;
    }

    static ValueCodec<String> utf8() {
        return BuiltinCodecs.UTF8;
    }
}
