package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

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
        this(ZBytes.__boltffiCreateHandle0(bytes));
    }

    private static long __boltffiCreateHandle0(byte[] bytes) {
        WireLease __boltffi_bytes_wire = WireWriterPool.acquire((4 + bytes.length));
        try {
    WireWriter __boltffi_bytes_writer = __boltffi_bytes_wire.writer();
    __boltffi_bytes_writer.writeBytes(bytes);
    return Native.boltffi_init_class_zenoh_ffi_bytes_z_bytes_from_bytes(__boltffi_bytes_wire.directBuffer(), __boltffi_bytes_wire.size());
} finally {
    __boltffi_bytes_wire.close();
}
    }

    /**
     * Create from string
     */
    public ZBytes(String s) {
        this(ZBytes.__boltffiCreateHandle1(s));
    }

    private static long __boltffiCreateHandle1(String s) {
        WireLease __boltffi_s_wire = WireWriterPool.acquire(WireSizes.string(s));
        try {
    WireWriter __boltffi_s_writer = __boltffi_s_wire.writer();
    __boltffi_s_writer.writeString(s);
    return Native.boltffi_init_class_zenoh_ffi_bytes_z_bytes_from_string(__boltffi_s_wire.directBuffer(), __boltffi_s_wire.size());
} finally {
    __boltffi_s_wire.close();
}
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("ZBytes is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_bytes_z_bytes(this.handle);
    }

    /**
     * Get as byte array
     */
    public byte[] toBytes() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_bytes_z_bytes_to_bytes(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readBytes();
    }

    /**
     * Get as UTF-8 string (returns empty string if invalid UTF-8)
     */
    public String toUtf8String() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_bytes_z_bytes_to_utf8_string(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Get the length in bytes
     */
    public long len() {
        return Native.boltffi_method_class_zenoh_ffi_bytes_z_bytes_len(this.rawHandle());
    }

    /**
     * Check if empty
     */
    public boolean isEmpty() {
        return Native.boltffi_method_class_zenoh_ffi_bytes_z_bytes_is_empty(this.rawHandle());
    }
}