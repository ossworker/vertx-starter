package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Zero-copy bytes wrapper for FFI
 */
public final class ZBytes implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ZBytes(long handle) {
        this.handle = handle;
    }

    /**
     * Create from byte array
     */
    public ZBytes(byte[] bytes) {
        this(ZBytes.createHandle0(bytes));
    }

    private static long createHandle0(byte[] bytes) {
        return Native.boltffi_z_bytes_from_bytes(bytes);
    }

    /**
     * Create from string
     */
    public ZBytes(String s) {
        this(ZBytes.createHandle1(s));
    }

    private static long createHandle1(String s) {
        return Native.boltffi_z_bytes_from_string(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_z_bytes_free(handle);
    }

    /**
     * Get as byte array
     */
    public byte[] toBytes() {
        byte[] _buf = Native.boltffi_z_bytes_to_bytes(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return _buf != null ? _buf : new byte[0];
    }

    /**
     * Get as UTF-8 string (returns empty string if invalid UTF-8)
     */
    public String toString() {
        byte[] _buf = Native.boltffi_z_bytes_to_string(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }

    /**
     * Get the length in bytes
     */
    public long len() {
        return Native.boltffi_z_bytes_len(handle);
    }

    /**
     * Check if empty
     */
    public boolean isEmpty() {
        return Native.boltffi_z_bytes_is_empty(handle);
    }
}