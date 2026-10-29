package com.workoss.zenoh;

    /**
     * Represents an incoming query received by a Queryable.
     * Provides methods to inspect the query and send replies.
     */
public final class Query implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private Query(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("Query handle must not be zero");
        this.handle = handle;
    }

    static Query __boltffiFromHandle(long handle) {
        return new Query(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Block waiting for the next query on the given queryable.
     * Returns Err if the queryable has been undeclared or the channel is closed.
     * Equivalent to zenoh-java: queryable.receiver().recv()
     */
    public Query(Queryable queryable) {
        this(new java.util.concurrent.atomic.AtomicLong(Query.__boltffiCreateHandle0(queryable)));
    }


    private static long __boltffiCreateHandle0(Queryable queryable) {
        try {
    return Native.boltffi_init_class_9zenoh_ffi5Query4recv(queryable.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("Query is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("Query is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_9zenoh_ffi5Query(__boltffi_handle);
    }

    /**
     * Try to receive the next query without blocking.
     * Returns Err if no query is currently available.
     */
    public static Query tryRecv(Queryable queryable) {
        try {
    return Query.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi5Query8try_recv(queryable.rawHandle()));
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException("Factory constructor failed");
}
    }

    /**
     * Get the key expression of this query.
     */
    public String getKeyExpr() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi5Query12get_key_expr(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Get the query parameters (selector part after '?').
     */
    public String getParameters() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi5Query14get_parameters(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Get the query payload if present.
     */
    public java.util.Optional<byte[]> getPayload() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi5Query11get_payload(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readOptional(() -> __boltffi_reader.readBytes());
    }

    /**
     * Reply to this query with a key expression and payload.
     * Equivalent to zenoh-java: query.reply(keyExpr, payload)
     */
    public void reply(String keyExpr, byte[] payload) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    try {
        Native.boltffi_method_class_9zenoh_ffi5Query5reply(this.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size());
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
     * Reply to this query with an error payload.
     * Equivalent to zenoh-java: query.replyErr(payload)
     */
    public void replyErr(byte[] payload) {
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        try {
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    try {
        Native.boltffi_method_class_9zenoh_ffi5Query9reply_err(this.rawHandle(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_payload_wire.close();
}
    }
}