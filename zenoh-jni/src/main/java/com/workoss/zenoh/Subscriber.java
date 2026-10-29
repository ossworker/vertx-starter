package com.workoss.zenoh;

    /**
     * A Zenoh subscriber that receives data on a key expression.
     * Uses a callback-fed channel so Java/Kotlin side can poll samples via
     * `recv()` / `try_recv()`.
     */
public final class Subscriber implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private Subscriber(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("Subscriber handle must not be zero");
        this.handle = handle;
    }

    static Subscriber __boltffiFromHandle(long handle) {
        return new Subscriber(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Declare a subscriber on the given session.
     * Equivalent to zenoh-java: session.declareSubscriber(keyExpr)
     * 
     * Note: lives in Subscriber's `#[export]` block (returning `Result<Self, _>`)
     * because boltffi 0.25 only allows `#[export]` methods to return `Self`
     * for non-`#[data]` opaque types.
     */
    public Subscriber(Session session, String keyExpr) {
        this(new java.util.concurrent.atomic.AtomicLong(Subscriber.__boltffiCreateHandle0(session, keyExpr)));
    }


    private static long __boltffiCreateHandle0(Session session, String keyExpr) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    try {
        return Native.boltffi_init_class_9zenoh_ffi10Subscriber7declare(session.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("Subscriber is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("Subscriber is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_9zenoh_ffi10Subscriber(__boltffi_handle);
    }

    /**
     * Get the key expression this subscriber is listening on.
     */
    public String getKeyExpr() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi10Subscriber12get_key_expr(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Check if the subscriber has been undeclared.
     */
    public boolean isUndeclared() {
        return Native.boltffi_method_class_9zenoh_ffi10Subscriber13is_undeclared(this.rawHandle());
    }

    /**
     * Undeclare this subscriber.
     * Equivalent to zenoh-java: subscriber.undeclare()
     */
    public void undeclare() {
        try {
    Native.boltffi_method_class_9zenoh_ffi10Subscriber9undeclare(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Block waiting for the next sample.
     * Returns None if the subscriber has been undeclared or the channel closed.
     */
    public java.util.Optional<Sample> recv() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi10Subscriber4recv(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readOptional(() -> Sample.fromReader(__boltffi_reader));
    }

    /**
     * Try to receive a sample without blocking.
     * Returns None if no sample is currently available.
     */
    public java.util.Optional<Sample> tryRecv() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi10Subscriber8try_recv(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readOptional(() -> Sample.fromReader(__boltffi_reader));
    }
}