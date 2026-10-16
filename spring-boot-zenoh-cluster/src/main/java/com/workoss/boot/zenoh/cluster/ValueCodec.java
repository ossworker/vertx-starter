package com.workoss.boot.zenoh.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Pluggable codec for shared-map values on the wire. The default is JDK serialization, matching
 * the reference implementation; supply a custom codec (JSON, protobuf, ...) when values must be
 * readable across languages or must not be Java-serialized.
 *
 * @param <V> the value type
 */
public interface ValueCodec<V> {

    /**
     * Encode a value for replication.
     */
    byte[] encode(V value);

    /**
     * Decode a replicated value; may return {@code null} when the payload is unusable.
     */
    V decode(byte[] data);

    /**
     * Default JDK-serialization codec. Values must implement {@link java.io.Serializable}.
     */
    static <V> ValueCodec<V> jdk() {
        return new ValueCodec<>() {
            @Override
            public byte[] encode(V value) {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                     ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(value);
                    oos.flush();
                    return bos.toByteArray();
                } catch (IOException e) {
                    throw new IllegalArgumentException("Cannot serialize shared-map value", e);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public V decode(byte[] data) {
                if (data == null || data.length == 0) {
                    return null;
                }
                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                    return (V) ois.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    return null;
                }
            }
        };
    }
}
