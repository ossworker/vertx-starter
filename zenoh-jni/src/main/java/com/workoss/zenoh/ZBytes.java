package com.workoss.zenoh;

    /**
     * Zero-copy bytes wrapper for FFI
     */
public final class ZBytes implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private ZBytes(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("ZBytes handle must not be zero");
        this.handle = handle;
    }

    static ZBytes __boltffiFromHandle(long handle) {
        return new ZBytes(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Create from byte array
     */
    public ZBytes(byte[] bytes) {
        this(new java.util.concurrent.atomic.AtomicLong(ZBytes.__boltffiCreateHandle0(bytes)));
    }


    private static long __boltffiCreateHandle0(byte[] bytes) {
        WireLease __boltffi_bytes_wire = WireWriterPool.acquire((4 + bytes.length));
        try {
    WireWriter __boltffi_bytes_writer = __boltffi_bytes_wire.writer();
    __boltffi_bytes_writer.writeBytes(bytes);
    return Native.boltffi_init_class_9zenoh_ffi6ZBytes10from_bytes(__boltffi_bytes_wire.directBuffer(), __boltffi_bytes_wire.size());
} finally {
    __boltffi_bytes_wire.close();
}
    }

    /**
     * Create from string
     */
    public ZBytes(String s) {
        this(new java.util.concurrent.atomic.AtomicLong(ZBytes.__boltffiCreateHandle1(s)));
    }


    private static long __boltffiCreateHandle1(String s) {
        WireLease __boltffi_s_wire = WireWriterPool.acquire(WireSizes.string(s));
        try {
    WireWriter __boltffi_s_writer = __boltffi_s_wire.writer();
    __boltffi_s_writer.writeString(s);
    return Native.boltffi_init_class_9zenoh_ffi6ZBytes11from_string(__boltffi_s_wire.directBuffer(), __boltffi_s_wire.size());
} finally {
    __boltffi_s_wire.close();
}
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("ZBytes is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("ZBytes is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_9zenoh_ffi6ZBytes(__boltffi_handle);
    }

    /**
     * Get as byte array
     */
    public byte[] toBytes() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi6ZBytes8to_bytes(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readBytes();
    }

    /**
     * Get as UTF-8 string (returns empty string if invalid UTF-8)
     */
    public String toUtf8String() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi6ZBytes14to_utf8_string(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }

    /**
     * Get the length in bytes
     */
    public long len() {
        return Native.boltffi_method_class_9zenoh_ffi6ZBytes3len(this.rawHandle());
    }

    /**
     * Check if empty
     */
    public boolean isEmpty() {
        return Native.boltffi_method_class_9zenoh_ffi6ZBytes8is_empty(this.rawHandle());
    }
}