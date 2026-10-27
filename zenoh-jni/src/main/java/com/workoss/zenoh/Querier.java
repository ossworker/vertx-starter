package com.workoss.zenoh;

public final class Querier implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private Querier(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("Querier handle must not be zero");
        this.handle = handle;
    }

    static Querier __boltffiFromHandle(long handle) {
        return new Querier(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Declare a querier on the given session.
     * Equivalent to zenoh-java: session.declareQuerier(keyExpr)
     */
    public Querier(Session session, String keyExpr) {
        this(new java.util.concurrent.atomic.AtomicLong(Querier.__boltffiCreateHandle0(session, keyExpr)));
    }


    private static long __boltffiCreateHandle0(Session session, String keyExpr) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    try {
        return Native.boltffi_init_class_zenoh_ffi_querier_querier_declare(session.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("Querier is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("Querier is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_zenoh_ffi_querier_querier(__boltffi_handle);
    }

    /**
     * Perform a get query and collect all replies.
     * Equivalent to zenoh-java: querier.get()
     */
    public java.util.List<Reply> get() {
        try {
    byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_querier_querier_get(this.rawHandle());
    WireReader __boltffi_reader = new WireReader(__boltffi_result);
    return __boltffi_reader.readSequence(() -> Reply.fromReader(__boltffi_reader));
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Perform a get query with a specific payload.
     */
    public java.util.List<Reply> getWithPayload(byte[] payload) {
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        try {
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    try {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_querier_querier_get_with_payload(this.rawHandle(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readSequence(() -> Reply.fromReader(__boltffi_reader));
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_payload_wire.close();
}
    }

    /**
     * Get the key expression for this querier.
     */
    public String getKeyExpr() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_querier_querier_get_key_expr(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }
}