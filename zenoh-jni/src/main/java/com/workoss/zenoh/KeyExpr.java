package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Key expression for pub/sub addressing
 */
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
        this(KeyExpr.createHandle0(expr));
    }

    private static long createHandle0(String expr) {
        long _handle = Native.boltffi_key_expr_new(expr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_key_expr_free(handle);
    }

    /**
     * Get string representation
     */
    public String toString() {
        byte[] _buf = Native.boltffi_key_expr_to_string(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Check if this key expression intersects with another
     */
    public boolean intersects(KeyExpr other) {
        return Native.boltffi_key_expr_intersects(handle, other.rawHandle());
    }

    /**
     * Check if this key expression includes another
     */
    public boolean includes(KeyExpr other) {
        return Native.boltffi_key_expr_includes(handle, other.rawHandle());
    }
}