package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Zenoh querier for performing get operations.
 * Equivalent to zenoh-java: Querier
 */
public final class Querier implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Querier(long handle) {
        this.handle = handle;
    }

    /**
     * Declare a querier on the given session.
     * Equivalent to zenoh-java: session.declareQuerier(keyExpr)
     */
    public Querier(Session session, String keyExpr) {
        this(Querier.createHandle0(session, keyExpr));
    }

    private static long createHandle0(Session session, String keyExpr) {
        long _handle = Native.boltffi_querier_declare(session.rawHandle(), keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_querier_free(handle);
    }

    /**
     * Perform a get query and collect all replies.
     * Equivalent to zenoh-java: querier.get()
     */
    public java.util.List<Reply> get() {
        byte[] _buf = Native.boltffi_querier_get(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
        return reader.readList(readIndex0 -> Reply.decode(reader));
    }

    /**
     * Perform a get query with a specific payload.
     */
    public java.util.List<Reply> getWithPayload(byte[] payload) {
        byte[] _buf = Native.boltffi_querier_get_with_payload(handle, payload);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
        return reader.readList(readIndex0 -> Reply.decode(reader));
    }

    /**
     * Get the key expression for this querier.
     */
    public String getKeyExpr() {
        byte[] _buf = Native.boltffi_querier_get_key_expr(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }
}