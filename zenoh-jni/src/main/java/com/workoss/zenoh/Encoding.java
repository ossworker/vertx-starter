package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encoding descriptor for payload data
 */
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
        this(Encoding.createHandle0(id, schema));
    }

    private static long createHandle0(short id, java.util.Optional<String> schema) {
        try (
            WireWriter _wire_schema = new WireWriter((1 + ((schema).isPresent() ? ((4 + (4 + ((schema).get()).length() * 3))) : 0)))
        ) {
            if ((schema).isPresent()) { _wire_schema.writeI8((byte)1); _wire_schema.writeString((schema).get()); } else { _wire_schema.writeI8((byte)0); };
            return Native.boltffi_encoding_new(id, _wire_schema.toBuffer());
        }
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_encoding_free(handle);
    }

    public static Encoding textPlain() {
        return new Encoding(Native.boltffi_encoding_text_plain());
    }

    public static Encoding applicationOctetStream() {
        return new Encoding(Native.boltffi_encoding_application_octet_stream());
    }

    public static Encoding applicationJson() {
        return new Encoding(Native.boltffi_encoding_application_json());
    }

    public static Encoding textJson() {
        return new Encoding(Native.boltffi_encoding_text_json());
    }

    public static Encoding applicationInteger() {
        return new Encoding(Native.boltffi_encoding_application_integer());
    }

    public static Encoding applicationFloat() {
        return new Encoding(Native.boltffi_encoding_application_float());
    }

    public static Encoding applicationXml() {
        return new Encoding(Native.boltffi_encoding_application_xml());
    }

    public static Encoding applicationYaml() {
        return new Encoding(Native.boltffi_encoding_application_yaml());
    }

    /**
     * Get the encoding ID
     */
    public short getId() {
        return Native.boltffi_encoding_get_id(handle);
    }

    /**
     * Get the optional schema
     */
    public java.util.Optional<String> getSchema() {
        byte[] _buf = Native.boltffi_encoding_get_schema(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readI8() == 0 ? java.util.Optional.empty() : java.util.Optional.ofNullable(reader.readString());
    }
}