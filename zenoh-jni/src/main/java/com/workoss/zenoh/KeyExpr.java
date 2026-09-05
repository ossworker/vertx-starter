package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

public final class KeyExpr implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    KeyExpr(long handle) {
        this.handle = handle;
    }

    /**
     * Create a new key expression
     */
    public KeyExpr(String expr) {
        this(KeyExpr.__boltffiCreateHandle0(expr));
    }

    private static long __boltffiCreateHandle0(String expr) {
        WireLease __boltffi_expr_wire = WireWriterPool.acquire(WireSizes.string(expr));
        try {
    WireWriter __boltffi_expr_writer = __boltffi_expr_wire.writer();
    __boltffi_expr_writer.writeString(expr);
    try {
        return Native.boltffi_init_class_zenoh_ffi_key_expr_key_expr_new(__boltffi_expr_wire.directBuffer(), __boltffi_expr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException("Constructor failed");
    }
} finally {
    __boltffi_expr_wire.close();
}
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("KeyExpr is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_key_expr_key_expr(this.handle);
    }

    /**
     * Get string representation
     */
    public String asStr() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_key_expr_key_expr_as_str(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Check if this key expression intersects with another
     */
    public boolean intersects(KeyExpr other) {
        return Native.boltffi_method_class_zenoh_ffi_key_expr_key_expr_intersects(this.rawHandle(), other.rawHandle());
    }

    /**
     * Check if this key expression includes another
     */
    public boolean includes(KeyExpr other) {
        return Native.boltffi_method_class_zenoh_ffi_key_expr_key_expr_includes(this.rawHandle(), other.rawHandle());
    }
}