package com.workoss.zenoh;

    /**
     * A zenoh session - the main entry point for pub/sub and queries.
     * Wraps zenoh::Session which is internally Arc-based and thread-safe.
     */
public final class Session implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private Session(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("Session handle must not be zero");
        this.handle = handle;
    }

    static Session __boltffiFromHandle(long handle) {
        return new Session(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Open a new session with the given configuration.
     * Equivalent to zenoh-java: Zenoh.open(config)
     * 
     * Note: This is synchronous because boltffi 0.25 does not export
     * C symbols for async fn returning Result<Self, String>.
     */
    public Session(Config config) {
        this(new java.util.concurrent.atomic.AtomicLong(Session.__boltffiCreateHandle0(config)));
    }


    private static long __boltffiCreateHandle0(Config config) {
        try {
    return Native.boltffi_init_class_9zenoh_ffi7Session4open(config.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
}
    }

    /**
     * Share an embedded router's own session: same runtime, listeners and connections, so no
     * loopback link (and no TLS material for it) is needed. Closing this session closes the
     * router; release the handle and close the router instead.
     */
    public Session(EmbeddedRouter router) {
        this(new java.util.concurrent.atomic.AtomicLong(Session.__boltffiCreateHandle1(router)));
    }


    private static long __boltffiCreateHandle1(EmbeddedRouter router) {
        try {
    return Native.boltffi_init_class_9zenoh_ffi7Session11from_router(router.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("Session is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("Session is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_9zenoh_ffi7Session(__boltffi_handle);
    }

    /**
     * Put data on a key expression (shortcut without declaring a publisher).
     * Equivalent to zenoh-java: session.put(keyExpr, payload)
     */
    public void put(String keyExpr, byte[] payload) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    try {
        Native.boltffi_method_class_9zenoh_ffi7Session3put(this.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
    __boltffi_payload_wire.close();
}
    }

    /**
     * Delete data on a key expression.
     * Equivalent to zenoh-java: session.delete(keyExpr)
     */
    public void delete(String keyExpr) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    try {
        Native.boltffi_method_class_9zenoh_ffi7Session6delete(this.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
}
    }

    /**
     * Close the session.
     * Equivalent to zenoh-java: session.close()
     */
    public void closeSession() {
        try {
    Native.boltffi_method_class_9zenoh_ffi7Session13close_session(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Check if the session is closed.
     */
    public boolean isClosed() {
        return Native.boltffi_method_class_9zenoh_ffi7Session9is_closed(this.rawHandle());
    }

    /**
     * Get the session's ZID (Zenoh ID) as a string.
     */
    public String zid() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi7Session3zid(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Perform a get query on a selector and collect replies.
     * Equivalent to zenoh-java: session.get(selector)
     */
    public java.util.List<Reply> get(String selector) {
        WireLease __boltffi_selector_wire = WireWriterPool.acquire(WireSizes.string(selector));
        try {
    WireWriter __boltffi_selector_writer = __boltffi_selector_wire.writer();
    __boltffi_selector_writer.writeString(selector);
    try {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi7Session3get(this.rawHandle(), __boltffi_selector_wire.directBuffer(), __boltffi_selector_wire.size());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readSequence(() -> Reply.fromReader(__boltffi_reader));
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_selector_wire.close();
}
    }
}