package com.workoss.zenoh;

    /**
     * Key expression for pub/sub addressing
     */
public final class KeyExpr implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private KeyExpr(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("KeyExpr handle must not be zero");
        this.handle = handle;
    }

    static KeyExpr __boltffiFromHandle(long handle) {
        return new KeyExpr(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Create a new key expression
     */
    public KeyExpr(String expr) {
        this(new java.util.concurrent.atomic.AtomicLong(KeyExpr.__boltffiCreateHandle0(expr)));
    }


    private static long __boltffiCreateHandle0(String expr) {
        WireLease __boltffi_expr_wire = WireWriterPool.acquire(WireSizes.string(expr));
        try {
    WireWriter __boltffi_expr_writer = __boltffi_expr_wire.writer();
    __boltffi_expr_writer.writeString(expr);
    try {
        return Native.boltffi_init_class_9zenoh_ffi7KeyExpr3new(__boltffi_expr_wire.directBuffer(), __boltffi_expr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
    }
} finally {
    __boltffi_expr_wire.close();
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("KeyExpr is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("KeyExpr is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_9zenoh_ffi7KeyExpr(__boltffi_handle);
    }

    /**
     * Get string representation
     */
    public String asStr() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi7KeyExpr6as_str(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Check if this key expression intersects with another
     */
    public boolean intersects(KeyExpr other) {
        return Native.boltffi_method_class_9zenoh_ffi7KeyExpr10intersects(this.rawHandle(), other.rawHandle());
    }

    /**
     * Check if this key expression includes another
     */
    public boolean includes(KeyExpr other) {
        return Native.boltffi_method_class_9zenoh_ffi7KeyExpr8includes(this.rawHandle(), other.rawHandle());
    }
}