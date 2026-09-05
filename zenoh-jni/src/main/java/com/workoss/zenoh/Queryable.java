package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

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
        this(Queryable.__boltffiCreateHandle0(session, keyExpr));
    }

    private static long __boltffiCreateHandle0(Session session, String keyExpr) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    try {
        return Native.boltffi_init_class_zenoh_ffi_queryable_queryable_declare(session.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException("Constructor failed");
    }
} finally {
    __boltffi_keyExpr_wire.close();
}
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("Queryable is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_queryable_queryable(this.handle);
    }

    /**
     * Get the key expression this queryable is registered on.
     */
    public String getKeyExpr() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_queryable_queryable_get_key_expr(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Check if the queryable has been undeclared.
     */
    public boolean isUndeclared() {
        return Native.boltffi_method_class_zenoh_ffi_queryable_queryable_is_undeclared(this.rawHandle());
    }

    /**
     * Undeclare this queryable.
     */
    public void undeclare() {
        try {
    Native.boltffi_method_class_zenoh_ffi_queryable_queryable_undeclare(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }
}