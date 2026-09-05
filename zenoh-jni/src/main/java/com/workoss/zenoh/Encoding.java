package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

public final class Encoding implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Encoding(long handle) {
        this.handle = handle;
    }

    /**
     * Create encoding from id and optional schema
     */
    public Encoding(short id, java.util.Optional<String> schema) {
        this(Encoding.__boltffiCreateHandle0(id, schema));
    }

    private static long __boltffiCreateHandle0(short id, java.util.Optional<String> schema) {
        WireLease __boltffi_schema_wire = WireWriterPool.acquire(WireSizes.optional(schema, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0)));
        try {
    WireWriter __boltffi_schema_writer = __boltffi_schema_wire.writer();
    __boltffi_schema_writer.writeOptional(schema, (__boltffi_value_0) -> { __boltffi_schema_writer.writeString(__boltffi_value_0); });
    return Native.boltffi_init_class_zenoh_ffi_encoding_encoding_new(id, __boltffi_schema_wire.directBuffer(), __boltffi_schema_wire.size());
} finally {
    __boltffi_schema_wire.close();
}
    }

    public Encoding() {
        this(Encoding.__boltffiCreateHandle1());
    }

    private static long __boltffiCreateHandle1() {
        return Native.boltffi_init_class_zenoh_ffi_encoding_encoding_text_plain();
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("Encoding is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_encoding_encoding(this.handle);
    }

    public static Encoding applicationOctetStream() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_application_octet_stream());
    }

    public static Encoding applicationJson() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_application_json());
    }

    public static Encoding textJson() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_text_json());
    }

    public static Encoding applicationInteger() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_application_integer());
    }

    public static Encoding applicationFloat() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_application_float());
    }

    public static Encoding applicationXml() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_application_xml());
    }

    public static Encoding applicationYaml() {
        return new Encoding(Native.boltffi_init_class_zenoh_ffi_encoding_encoding_application_yaml());
    }

    /**
     * Get the encoding ID
     */
    public short getId() {
        return Native.boltffi_method_class_zenoh_ffi_encoding_encoding_get_id(this.rawHandle());
    }

    /**
     * Get the optional schema
     */
    public java.util.Optional<String> getSchema() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_encoding_encoding_get_schema(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readOptional(() -> __boltffi_reader.readString());
    }
}