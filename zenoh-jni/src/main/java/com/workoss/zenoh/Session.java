package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A zenoh session - the main entry point for pub/sub and queries.
 * Wraps zenoh::Session which is internally Arc-based and thread-safe.
 */
public final class Session implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Session(long handle) {
        this.handle = handle;
    }

    /**
     * Open a new session with the given configuration.
     * Equivalent to zenoh-java: Zenoh.open(config)
     *
     * Note: This is synchronous because boltffi 0.25 does not export
     * C symbols for async fn returning Result<Self, String>.
     */
    public Session(Config config) {
        this(Session.createHandle0(config));
    }

    private static long createHandle0(Config config) {
        long _handle = Native.boltffi_session_open(config.rawHandle());
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_session_free(handle);
    }

    /**
     * Put data on a key expression (shortcut without declaring a publisher).
     * Equivalent to zenoh-java: session.put(keyExpr, payload)
     */
    public void put(String keyExpr, byte[] payload) {
        byte[] _buf = Native.boltffi_session_put(handle, keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8), payload);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Delete data on a key expression.
     * Equivalent to zenoh-java: session.delete(keyExpr)
     */
    public void delete(String keyExpr) {
        byte[] _buf = Native.boltffi_session_delete(handle, keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Close the session.
     * Equivalent to zenoh-java: session.close()
     */
    public void closeSession() {
        byte[] _buf = Native.boltffi_session_close(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Check if the session is closed.
     */
    public boolean isClosed() {
        return Native.boltffi_session_is_closed(handle);
    }

    /**
     * Get the session's ZID (Zenoh ID) as a string.
     */
    public String zid() {
        byte[] _buf = Native.boltffi_session_zid(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Perform a get query on a selector and collect replies.
     * Equivalent to zenoh-java: session.get(selector)
     */
    public java.util.List<Reply> get(String selector) {
        byte[] _buf = Native.boltffi_session_get(handle, selector.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
        return reader.readList(readIndex0 -> Reply.decode(reader));
    }
}