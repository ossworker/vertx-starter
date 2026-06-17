package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Zenoh publisher for a specific key expression.
 * Equivalent to zenoh-java: Publisher
 */
public final class Publisher implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Publisher(long handle) {
        this.handle = handle;
    }

    /**
     * Declare a publisher on the given session.
     * Equivalent to zenoh-java: session.declarePublisher(keyExpr)
     *
     * Note: lives in Publisher's `#[export]` block (returning `Result<Self, _>`)
     * because boltffi 0.25 only allows `#[export]` methods to return `Self`
     * for non-`#[data]` opaque types.
     */
    public Publisher(Session session, String keyExpr, java.util.Optional<Byte> congestionControl, java.util.Optional<Byte> priority, java.util.Optional<Boolean> express, java.util.Optional<Byte> reliability) {
        this(Publisher.createHandle0(session, keyExpr, congestionControl, priority, express, reliability));
    }

    private static long createHandle0(Session session, String keyExpr, java.util.Optional<Byte> congestionControl, java.util.Optional<Byte> priority, java.util.Optional<Boolean> express, java.util.Optional<Byte> reliability) {
        try (
            WireWriter _wire_congestion_control = new WireWriter((1 + ((congestionControl).isPresent() ? (1) : 0)));
            WireWriter _wire_priority = new WireWriter((1 + ((priority).isPresent() ? (1) : 0)));
            WireWriter _wire_express = new WireWriter((1 + ((express).isPresent() ? (1) : 0)));
            WireWriter _wire_reliability = new WireWriter((1 + ((reliability).isPresent() ? (1) : 0)))
        ) {
            if ((congestionControl).isPresent()) { _wire_congestion_control.writeI8((byte)1); _wire_congestion_control.writeI8((congestionControl).get()); } else { _wire_congestion_control.writeI8((byte)0); };
            if ((priority).isPresent()) { _wire_priority.writeI8((byte)1); _wire_priority.writeI8((priority).get()); } else { _wire_priority.writeI8((byte)0); };
            if ((express).isPresent()) { _wire_express.writeI8((byte)1); _wire_express.writeBool((express).get()); } else { _wire_express.writeI8((byte)0); };
            if ((reliability).isPresent()) { _wire_reliability.writeI8((byte)1); _wire_reliability.writeI8((reliability).get()); } else { _wire_reliability.writeI8((byte)0); };
            long _handle = Native.boltffi_publisher_declare(session.rawHandle(), keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8), _wire_congestion_control.toBuffer(), _wire_priority.toBuffer(), _wire_express.toBuffer(), _wire_reliability.toBuffer());
            if (_handle == 0L) throw new RuntimeException("Constructor failed");
            return _handle;
        }
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_publisher_free(handle);
    }

    /**
     * Put data through this publisher.
     * Equivalent to zenoh-java: publisher.put(payload)
     */
    public void put(byte[] payload) {
        byte[] _buf = Native.boltffi_publisher_put(handle, payload);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Put data with a specific encoding string (e.g. "text/plain", "application/json").
     */
    public void putWithEncoding(byte[] payload, String encoding) {
        byte[] _buf = Native.boltffi_publisher_put_with_encoding(handle, payload, encoding.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Delete data through this publisher.
     * Equivalent to zenoh-java: publisher.delete()
     */
    public void delete() {
        byte[] _buf = Native.boltffi_publisher_delete(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Undeclare this publisher.
     * Note: real undeclare requires consuming the inner publisher; the FFI
     * model exposes only `&self`, so the publisher is effectively undeclared
     * when its underlying handle is dropped.
     */
    public void undeclare() {
        byte[] _buf = Native.boltffi_publisher_undeclare(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Get the key expression this publisher is associated with.
     */
    public String getKeyExpr() {
        byte[] _buf = Native.boltffi_publisher_get_key_expr(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }
}