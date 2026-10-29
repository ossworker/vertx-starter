package com.workoss.zenoh;

    /**
     * Encoding descriptor for payload data
     */
public final class Encoding implements AutoCloseable {
    private final java.util.concurrent.atomic.AtomicLong handle;

    private Encoding(java.util.concurrent.atomic.AtomicLong handle) {
        if (handle.get() == 0) throw new IllegalArgumentException("Encoding handle must not be zero");
        this.handle = handle;
    }

    static Encoding __boltffiFromHandle(long handle) {
        return new Encoding(new java.util.concurrent.atomic.AtomicLong(handle));
    }

    /**
     * Create encoding from id and optional schema
     */
    public Encoding(short id, java.util.Optional<String> schema) {
        this(new java.util.concurrent.atomic.AtomicLong(Encoding.__boltffiCreateHandle0(id, schema)));
    }


    private static long __boltffiCreateHandle0(short id, java.util.Optional<String> schema) {
        WireLease __boltffi_schema_wire = WireWriterPool.acquire(WireSizes.optional(schema, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0)));
        try {
    WireWriter __boltffi_schema_writer = __boltffi_schema_wire.writer();
    __boltffi_schema_writer.writeOptional(schema, (__boltffi_value_0) -> { __boltffi_schema_writer.writeString(__boltffi_value_0); });
    return Native.boltffi_init_class_9zenoh_ffi8Encoding3new(id, __boltffi_schema_wire.directBuffer(), __boltffi_schema_wire.size());
} finally {
    __boltffi_schema_wire.close();
}
    }

    public Encoding() {
        this(new java.util.concurrent.atomic.AtomicLong(Encoding.__boltffiCreateHandle1()));
    }


    private static long __boltffiCreateHandle1() {
        return Native.boltffi_init_class_9zenoh_ffi8Encoding10text_plain();
    }

    long __boltffiTakeHandle() {
        long value = handle.getAndSet(0);
        if (value == 0) throw new IllegalStateException("Encoding is closed");
        return value;
    }

    long rawHandle() {
        long value = handle.get();
        if (value == 0) throw new IllegalStateException("Encoding is closed");
        return value;
    }

    @Override
    public void close() {
        long __boltffi_handle = handle.getAndSet(0);
        if (__boltffi_handle == 0) return;
        Native.boltffi_release_class_9zenoh_ffi8Encoding(__boltffi_handle);
    }

    public static Encoding applicationOctetStream() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding24application_octet_stream());
    }

    public static Encoding applicationJson() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding16application_json());
    }

    public static Encoding textJson() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding9text_json());
    }

    public static Encoding applicationInteger() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding19application_integer());
    }

    public static Encoding applicationFloat() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding17application_float());
    }

    public static Encoding applicationXml() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding15application_xml());
    }

    public static Encoding applicationYaml() {
        return Encoding.__boltffiFromHandle(Native.boltffi_init_class_9zenoh_ffi8Encoding16application_yaml());
    }

    /**
     * Get the encoding ID
     */
    public short getId() {
        return Native.boltffi_method_class_9zenoh_ffi8Encoding6get_id(this.rawHandle());
    }

    /**
     * Get the optional schema
     */
    public java.util.Optional<String> getSchema() {
        byte[] __boltffi_result = Native.boltffi_method_class_9zenoh_ffi8Encoding10get_schema(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readOptional(() -> __boltffi_reader.readString());
    }
}