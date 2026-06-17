package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Zenoh queryable that answers queries on a key expression.
 * Equivalent to zenoh-java: Queryable
 */
public final class Queryable implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Queryable(long handle) {
        this.handle = handle;
    }

    /**
     * Declare a queryable on the given session.
     * Equivalent to zenoh-java: session.declareQueryable(keyExpr)
     */
    public Queryable(Session session, String keyExpr) {
        this(Queryable.createHandle0(session, keyExpr));
    }

    private static long createHandle0(Session session, String keyExpr) {
        long _handle = Native.boltffi_queryable_declare(session.rawHandle(), keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_queryable_free(handle);
    }

    /**
     * Get the key expression this queryable is registered on.
     */
    public String getKeyExpr() {
        byte[] _buf = Native.boltffi_queryable_get_key_expr(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Check if the queryable has been undeclared.
     */
    public boolean isUndeclared() {
        return Native.boltffi_queryable_is_undeclared(handle);
    }

    /**
     * Undeclare this queryable.
     */
    public void undeclare() {
        byte[] _buf = Native.boltffi_queryable_undeclare(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }
}