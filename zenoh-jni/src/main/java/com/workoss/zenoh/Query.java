package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents an incoming query received by a Queryable.
 * Provides methods to inspect the query and send replies.
 */
public final class Query implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Query(long handle) {
        this.handle = handle;
    }

    /**
     * Block waiting for the next query on the given queryable.
     * Returns Err if the queryable has been undeclared or the channel is closed.
     * Equivalent to zenoh-java: queryable.receiver().recv()
     */
    public Query(Queryable queryable) {
        this(Query.createHandle0(queryable));
    }

    private static long createHandle0(Queryable queryable) {
        long _handle = Native.boltffi_query_recv(queryable.rawHandle());
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_query_free(handle);
    }

    /**
     * Try to receive the next query without blocking.
     * Returns Err if no query is currently available.
     */
    public static Query tryRecv(Queryable queryable) {
        long _handle = Native.boltffi_query_try_recv(queryable.rawHandle());
        if (_handle == 0L) throw new RuntimeException("Factory constructor failed");
        return new Query(_handle);
    }

    /**
     * Get the key expression of this query.
     */
    public String getKeyExpr() {
        byte[] _buf = Native.boltffi_query_get_key_expr(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Get the query parameters (selector part after '?').
     */
    public String getParameters() {
        byte[] _buf = Native.boltffi_query_get_parameters(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Get the query payload if present.
     */
    public java.util.Optional<byte[]> getPayload() {
        byte[] _buf = Native.boltffi_query_get_payload(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readI8() == 0 ? java.util.Optional.empty() : java.util.Optional.ofNullable(reader.readByteArray());
    }

    /**
     * Reply to this query with a key expression and payload.
     * Equivalent to zenoh-java: query.reply(keyExpr, payload)
     */
    public void reply(String keyExpr, byte[] payload) {
        byte[] _buf = Native.boltffi_query_reply(handle, keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8), payload);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Reply to this query with an error payload.
     * Equivalent to zenoh-java: query.replyErr(payload)
     */
    public void replyErr(byte[] payload) {
        byte[] _buf = Native.boltffi_query_reply_err(handle, payload);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }
}