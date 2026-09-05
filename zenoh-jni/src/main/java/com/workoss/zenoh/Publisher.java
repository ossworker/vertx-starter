package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

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
        this(Publisher.__boltffiCreateHandle0(session, keyExpr, congestionControl, priority, express, reliability));
    }

    private static long __boltffiCreateHandle0(Session session, String keyExpr, java.util.Optional<Byte> congestionControl, java.util.Optional<Byte> priority, java.util.Optional<Boolean> express, java.util.Optional<Byte> reliability) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        WireLease __boltffi_congestionControl_wire = WireWriterPool.acquire((1 + (congestionControl.isPresent() ? 1 : 0)));
        WireLease __boltffi_priority_wire = WireWriterPool.acquire((1 + (priority.isPresent() ? 1 : 0)));
        WireLease __boltffi_express_wire = WireWriterPool.acquire((1 + (express.isPresent() ? 1 : 0)));
        WireLease __boltffi_reliability_wire = WireWriterPool.acquire((1 + (reliability.isPresent() ? 1 : 0)));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    WireWriter __boltffi_congestionControl_writer = __boltffi_congestionControl_wire.writer();
    __boltffi_congestionControl_writer.writeOptional(congestionControl, (__boltffi_congestionControl_value) -> { __boltffi_congestionControl_writer.writeByte(__boltffi_congestionControl_value); });
    WireWriter __boltffi_priority_writer = __boltffi_priority_wire.writer();
    __boltffi_priority_writer.writeOptional(priority, (__boltffi_priority_value) -> { __boltffi_priority_writer.writeByte(__boltffi_priority_value); });
    WireWriter __boltffi_express_writer = __boltffi_express_wire.writer();
    __boltffi_express_writer.writeOptional(express, (__boltffi_express_value) -> { __boltffi_express_writer.writeBoolean(__boltffi_express_value); });
    WireWriter __boltffi_reliability_writer = __boltffi_reliability_wire.writer();
    __boltffi_reliability_writer.writeOptional(reliability, (__boltffi_reliability_value) -> { __boltffi_reliability_writer.writeByte(__boltffi_reliability_value); });
    try {
        return Native.boltffi_init_class_zenoh_ffi_publisher_publisher_declare(session.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size(), __boltffi_congestionControl_wire.directBuffer(), __boltffi_congestionControl_wire.size(), __boltffi_priority_wire.directBuffer(), __boltffi_priority_wire.size(), __boltffi_express_wire.directBuffer(), __boltffi_express_wire.size(), __boltffi_reliability_wire.directBuffer(), __boltffi_reliability_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException("Constructor failed");
    }
} finally {
    __boltffi_keyExpr_wire.close();
    __boltffi_congestionControl_wire.close();
    __boltffi_priority_wire.close();
    __boltffi_express_wire.close();
    __boltffi_reliability_wire.close();
}
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("Publisher is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_publisher_publisher(this.handle);
    }

    /**
     * Put data through this publisher.
     * Equivalent to zenoh-java: publisher.put(payload)
     */
    public void put(byte[] payload) {
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        try {
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    try {
        Native.boltffi_method_class_zenoh_ffi_publisher_publisher_put(this.rawHandle(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_payload_wire.close();
}
    }

    /**
     * Put data with a specific encoding string (e.g. "text/plain", "application/json").
     */
    public void putWithEncoding(byte[] payload, String encoding) {
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        WireLease __boltffi_encoding_wire = WireWriterPool.acquire(WireSizes.string(encoding));
        try {
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    WireWriter __boltffi_encoding_writer = __boltffi_encoding_wire.writer();
    __boltffi_encoding_writer.writeString(encoding);
    try {
        Native.boltffi_method_class_zenoh_ffi_publisher_publisher_put_with_encoding(this.rawHandle(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size(), __boltffi_encoding_wire.directBuffer(), __boltffi_encoding_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_payload_wire.close();
    __boltffi_encoding_wire.close();
}
    }

    /**
     * Delete data through this publisher.
     * Equivalent to zenoh-java: publisher.delete()
     */
    public void delete() {
        try {
    Native.boltffi_method_class_zenoh_ffi_publisher_publisher_delete(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Undeclare this publisher.
     * Note: real undeclare requires consuming the inner publisher; the FFI
     * model exposes only `&self`, so the publisher is effectively undeclared
     * when its underlying handle is dropped.
     */
    public void undeclare() {
        try {
    Native.boltffi_method_class_zenoh_ffi_publisher_publisher_undeclare(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Get the key expression this publisher is associated with.
     */
    public String getKeyExpr() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_publisher_publisher_get_key_expr(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }
}