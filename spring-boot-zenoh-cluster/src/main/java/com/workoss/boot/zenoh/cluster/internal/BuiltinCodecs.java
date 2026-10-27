package com.workoss.boot.zenoh.cluster.internal;

import com.workoss.boot.zenoh.cluster.ValueCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

public final class BuiltinCodecs {

    public static final ValueCodec<Object> JDK = new ValueCodec<>() {
        @Override
        public byte[] encode(Object value) {
            return serialize(value);
        }

        @Override
        public Object decode(byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                return deserialize(data);
            } catch (IOException | ClassNotFoundException e) {
                return null;
            }
        }
    };

    public static final ValueCodec<String> UTF8 = new ValueCodec<>() {
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] data) {
            return data == null ? null : new String(data, StandardCharsets.UTF_8);
        }
    };

    private BuiltinCodecs() {
    }

    public static byte[] serialize(Object value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(value);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Not serializable: "
                    + (value == null ? "null" : value.getClass().getName()), e);
        }
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return ois.readObject();
        }
    }
}
