package com.workoss.zenoh;

public final class Queryable implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private Queryable(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("Queryable handle must not be zero");
        this.handle = handle;
    }

    static Queryable __boltffiFromHandle(long handle) {
        return new Queryable(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Declare a queryable on the given session.
     * Equivalent to zenoh-java: session.declareQueryable(keyExpr)
     */
    public Queryable(Session session, String keyExpr) {
        this(new java.util.concurrent.atomic.AtomicLong(Queryable.__boltffiCreateHandle0(session, keyExpr)));
    }


    private static long __boltffiCreateHandle0(Session session, String keyExpr) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    try {
        return Native.boltffi_init_class_zenoh_ffi_queryable_queryable_declare(session.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("Queryable is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("Queryable is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_zenoh_ffi_queryable_queryable(__boltffi_handle);
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