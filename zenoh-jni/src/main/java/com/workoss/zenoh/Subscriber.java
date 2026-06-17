package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Zenoh subscriber that receives data on a key expression.
 * Uses a callback-fed channel so Java/Kotlin side can poll samples via
 * `recv()` / `try_recv()`.
 */
public final class Subscriber implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Subscriber(long handle) {
        this.handle = handle;
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
        this(Subscriber.createHandle0(session, keyExpr));
    }

    private static long createHandle0(Session session, String keyExpr) {
        long _handle = Native.boltffi_subscriber_declare(session.rawHandle(), keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_subscriber_free(handle);
    }

    /**
     * Get the key expression this subscriber is listening on.
     */
    public String getKeyExpr() {
        byte[] _buf = Native.boltffi_subscriber_get_key_expr(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Check if the subscriber has been undeclared.
     */
    public boolean isUndeclared() {
        return Native.boltffi_subscriber_is_undeclared(handle);
    }

    /**
     * Undeclare this subscriber.
     * Equivalent to zenoh-java: subscriber.undeclare()
     */
    public void undeclare() {
        byte[] _buf = Native.boltffi_subscriber_undeclare(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Block waiting for the next sample.
     * Returns None if the subscriber has been undeclared or the channel closed.
     */
    public java.util.Optional<Sample> recv() {
        byte[] _buf = Native.boltffi_subscriber_recv(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readI8() == 0 ? java.util.Optional.empty() : java.util.Optional.ofNullable(Sample.decode(reader));
    }

    /**
     * Try to receive a sample without blocking.
     * Returns None if no sample is currently available.
     */
    public java.util.Optional<Sample> tryRecv() {
        byte[] _buf = Native.boltffi_subscriber_try_recv(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readI8() == 0 ? java.util.Optional.empty() : java.util.Optional.ofNullable(Sample.decode(reader));
    }
}