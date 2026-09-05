package com.workoss.zenoh;

final class Native {
    static {
        String virtualMachineName = System.getProperty("java.vm.name", "")
            .toLowerCase(java.util.Locale.ROOT);
        boolean androidRuntime = virtualMachineName.contains("dalvik")
            || virtualMachineName.contains("art");
        if (androidRuntime) {
            System.loadLibrary("zenoh_ffi");
        } else {
            BoltFFINativeRuntime.load(
                Native.class,
                "zenoh_ffi_jni",
                "zenoh_ffi"
            );
        }
    }

    private Native() {}

    static native void boltffi_release_class_zenoh_ffi_key_expr_key_expr(long handle);

    static native long boltffi_init_class_zenoh_ffi_key_expr_key_expr_new(java.nio.ByteBuffer expr, int __boltffi_expr_len);

    static native byte[] boltffi_method_class_zenoh_ffi_key_expr_key_expr_as_str(long receiver);

    static native boolean boltffi_method_class_zenoh_ffi_key_expr_key_expr_intersects(long receiver, long other);

    static native boolean boltffi_method_class_zenoh_ffi_key_expr_key_expr_includes(long receiver, long other);

    static native void boltffi_release_class_zenoh_ffi_encoding_encoding(long handle);

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_new(short id, java.nio.ByteBuffer schema, int __boltffi_schema_len);

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_text_plain();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_application_octet_stream();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_application_json();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_text_json();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_application_integer();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_application_float();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_application_xml();

    static native long boltffi_init_class_zenoh_ffi_encoding_encoding_application_yaml();

    static native short boltffi_method_class_zenoh_ffi_encoding_encoding_get_id(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_encoding_encoding_get_schema(long receiver);

    static native void boltffi_release_class_zenoh_ffi_bytes_z_bytes(long handle);

    static native long boltffi_init_class_zenoh_ffi_bytes_z_bytes_from_bytes(java.nio.ByteBuffer bytes, int __boltffi_bytes_len);

    static native long boltffi_init_class_zenoh_ffi_bytes_z_bytes_from_string(java.nio.ByteBuffer s, int __boltffi_s_len);

    static native byte[] boltffi_method_class_zenoh_ffi_bytes_z_bytes_to_bytes(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_bytes_z_bytes_to_utf8_string(long receiver);

    static native long boltffi_method_class_zenoh_ffi_bytes_z_bytes_len(long receiver);

    static native boolean boltffi_method_class_zenoh_ffi_bytes_z_bytes_is_empty(long receiver);

    static native void boltffi_release_class_zenoh_ffi_config_config(long handle);

    static native long boltffi_init_class_zenoh_ffi_config_config_new();

    static native long boltffi_init_class_zenoh_ffi_config_config_from_file(java.nio.ByteBuffer path, int __boltffi_path_len);

    static native long boltffi_init_class_zenoh_ffi_config_config_default_config();

    static native long boltffi_init_class_zenoh_ffi_config_config_from_json5(java.nio.ByteBuffer json5, int __boltffi_json5_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_insert_json5(long receiver, java.nio.ByteBuffer key, int __boltffi_key_len, java.nio.ByteBuffer value, int __boltffi_value_len);

    static native byte[] boltffi_method_class_zenoh_ffi_config_config_get_json(long receiver, java.nio.ByteBuffer key, int __boltffi_key_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_set_mode(long receiver, java.nio.ByteBuffer mode, int __boltffi_mode_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_set_listen_endpoints(long receiver, java.nio.ByteBuffer endpoints, int __boltffi_endpoints_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_set_connect_endpoints(long receiver, java.nio.ByteBuffer endpoints, int __boltffi_endpoints_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_set_plugins_loading_enabled(long receiver, boolean enabled);

    static native void boltffi_method_class_zenoh_ffi_config_config_set_plugins_search_dirs(long receiver, java.nio.ByteBuffer dirs, int __boltffi_dirs_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_set_default_plugins_search_dirs(long receiver);

    static native void boltffi_method_class_zenoh_ffi_config_config_add_plugins_search_dir(long receiver, java.nio.ByteBuffer dir, int __boltffi_dir_len);

    static native void boltffi_method_class_zenoh_ffi_config_config_ensure_default_plugins_search_dirs(long receiver);

    static native void boltffi_release_class_zenoh_ffi_session_session(long handle);

    static native long boltffi_init_class_zenoh_ffi_session_session_open(long config);

    static native void boltffi_method_class_zenoh_ffi_session_session_put(long receiver, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len, java.nio.ByteBuffer payload, int __boltffi_payload_len);

    static native void boltffi_method_class_zenoh_ffi_session_session_delete(long receiver, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len);

    static native void boltffi_method_class_zenoh_ffi_session_session_close_session(long receiver);

    static native boolean boltffi_method_class_zenoh_ffi_session_session_is_closed(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_session_session_zid(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_session_session_get(long receiver, java.nio.ByteBuffer selector, int __boltffi_selector_len);

    static native void boltffi_release_class_zenoh_ffi_publisher_publisher(long handle);

    static native long boltffi_init_class_zenoh_ffi_publisher_publisher_declare(long session, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len, java.nio.ByteBuffer congestion_control, int __boltffi_congestion_control_len, java.nio.ByteBuffer priority, int __boltffi_priority_len, java.nio.ByteBuffer express, int __boltffi_express_len, java.nio.ByteBuffer reliability, int __boltffi_reliability_len);

    static native void boltffi_method_class_zenoh_ffi_publisher_publisher_put(long receiver, java.nio.ByteBuffer payload, int __boltffi_payload_len);

    static native void boltffi_method_class_zenoh_ffi_publisher_publisher_put_with_encoding(long receiver, java.nio.ByteBuffer payload, int __boltffi_payload_len, java.nio.ByteBuffer encoding, int __boltffi_encoding_len);

    static native void boltffi_method_class_zenoh_ffi_publisher_publisher_delete(long receiver);

    static native void boltffi_method_class_zenoh_ffi_publisher_publisher_undeclare(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_publisher_publisher_get_key_expr(long receiver);

    static native void boltffi_release_class_zenoh_ffi_subscriber_subscriber(long handle);

    static native long boltffi_init_class_zenoh_ffi_subscriber_subscriber_declare(long session, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len);

    static native byte[] boltffi_method_class_zenoh_ffi_subscriber_subscriber_get_key_expr(long receiver);

    static native boolean boltffi_method_class_zenoh_ffi_subscriber_subscriber_is_undeclared(long receiver);

    static native void boltffi_method_class_zenoh_ffi_subscriber_subscriber_undeclare(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_subscriber_subscriber_recv(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_subscriber_subscriber_try_recv(long receiver);

    static native void boltffi_release_class_zenoh_ffi_queryable_queryable(long handle);

    static native long boltffi_init_class_zenoh_ffi_queryable_queryable_declare(long session, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len);

    static native byte[] boltffi_method_class_zenoh_ffi_queryable_queryable_get_key_expr(long receiver);

    static native boolean boltffi_method_class_zenoh_ffi_queryable_queryable_is_undeclared(long receiver);

    static native void boltffi_method_class_zenoh_ffi_queryable_queryable_undeclare(long receiver);

    static native void boltffi_release_class_zenoh_ffi_query_query(long handle);

    static native long boltffi_init_class_zenoh_ffi_query_query_recv(long queryable);

    static native long boltffi_init_class_zenoh_ffi_query_query_try_recv(long queryable);

    static native byte[] boltffi_method_class_zenoh_ffi_query_query_get_key_expr(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_query_query_get_parameters(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_query_query_get_payload(long receiver);

    static native void boltffi_method_class_zenoh_ffi_query_query_reply(long receiver, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len, java.nio.ByteBuffer payload, int __boltffi_payload_len);

    static native void boltffi_method_class_zenoh_ffi_query_query_reply_err(long receiver, java.nio.ByteBuffer payload, int __boltffi_payload_len);

    static native void boltffi_release_class_zenoh_ffi_querier_querier(long handle);

    static native long boltffi_init_class_zenoh_ffi_querier_querier_declare(long session, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len);

    static native byte[] boltffi_method_class_zenoh_ffi_querier_querier_get(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_querier_querier_get_with_payload(long receiver, java.nio.ByteBuffer payload, int __boltffi_payload_len);

    static native byte[] boltffi_method_class_zenoh_ffi_querier_querier_get_key_expr(long receiver);

    static native void boltffi_release_class_zenoh_ffi_router_embedded_router(long handle);

    static native long boltffi_init_class_zenoh_ffi_router_embedded_router_new();

    static native long boltffi_init_class_zenoh_ffi_router_embedded_router_start(long config);

    static native long boltffi_init_class_zenoh_ffi_router_embedded_router_start_with_json5(java.nio.ByteBuffer json5_config, int __boltffi_json5_config_len);

    static native long boltffi_init_class_zenoh_ffi_router_embedded_router_start_default();

    static native long boltffi_init_class_zenoh_ffi_router_embedded_router_start_peer(long config);

    static native void boltffi_method_class_zenoh_ffi_router_embedded_router_put(long receiver, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len, java.nio.ByteBuffer payload, int __boltffi_payload_len);

    static native void boltffi_method_class_zenoh_ffi_router_embedded_router_delete(long receiver, java.nio.ByteBuffer key_expr, int __boltffi_key_expr_len);

    static native void boltffi_method_class_zenoh_ffi_router_embedded_router_close_router(long receiver);

    static native boolean boltffi_method_class_zenoh_ffi_router_embedded_router_is_running(long receiver);

    static native byte[] boltffi_method_class_zenoh_ffi_router_embedded_router_zid(long receiver);
}

final class BoltFFIValueIdentity {
    private BoltFFIValueIdentity() {}

    static <T> boolean optionalEquals(
        java.util.Optional<T> left,
        java.util.Optional<T> right,
        java.util.function.BiPredicate<T, T> equals
    ) {
        if (left == right) return true;
        if (left == null || right == null) return false;
        if (left.isPresent() != right.isPresent()) return false;
        return !left.isPresent() || equals.test(left.get(), right.get());
    }

    static <T> int optionalHash(
        java.util.Optional<T> value,
        java.util.function.ToIntFunction<T> hash
    ) {
        if (value == null || !value.isPresent()) return 0;
        return 31 + hash.applyAsInt(value.get());
    }

    static <T> boolean sequenceEquals(
        java.util.List<T> left,
        java.util.List<T> right,
        java.util.function.BiPredicate<T, T> equals
    ) {
        if (left == right) return true;
        if (left == null || right == null || left.size() != right.size()) return false;
        int index = 0;
        while (index < left.size()) {
            if (!equals.test(left.get(index), right.get(index))) return false;
            index += 1;
        }
        return true;
    }

    static <T> int sequenceHash(
        java.util.List<T> values,
        java.util.function.ToIntFunction<T> hash
    ) {
        if (values == null) return 0;
        int result = 1;
        int index = 0;
        while (index < values.size()) {
            result = 31 * result + hash.applyAsInt(values.get(index));
            index += 1;
        }
        return result;
    }
}

@FunctionalInterface
interface WireRead<T> {
    T read();
}

@FunctionalInterface
interface WireWrite<T> {
    void write(T value);
}

@FunctionalInterface
interface WireSize<T> {
    int size(T value);
}

final class WireReader {
    private final java.nio.ByteBuffer buffer;

    WireReader(byte[] bytes) {
        this.buffer = java.nio.ByteBuffer
            .wrap(java.util.Objects.requireNonNull(bytes, "null buffer returned"))
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    boolean readBoolean() { return buffer.get() != 0; }
    byte readByte() { return buffer.get(); }
    short readShort() { return buffer.getShort(); }
    int readInt() { return buffer.getInt(); }
    long readLong() { return buffer.getLong(); }
    float readFloat() { return buffer.getFloat(); }
    double readDouble() { return buffer.getDouble(); }

    java.time.Duration readDuration() {
        long seconds = readLong();
        int nanos = readInt();
        if (seconds < 0 || nanos < 0) {
            throw new IllegalArgumentException("duration out of range");
        }
        return java.time.Duration.ofSeconds(seconds, nanos);
    }

    java.time.Instant readInstant() {
        long seconds = readLong();
        int nanos = readInt();
        if (nanos < 0) {
            throw new IllegalArgumentException("instant nanos out of range");
        }
        return java.time.Instant.ofEpochSecond(seconds, nanos);
    }

    java.util.UUID readUuid() {
        return new java.util.UUID(readLong(), readLong());
    }

    java.net.URI readUri() { return java.net.URI.create(readString()); }

    String readString() {
        int length = readLength();
        String value = new String(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            length,
            java.nio.charset.StandardCharsets.UTF_8
        );
        buffer.position(buffer.position() + length);
        return value;
    }

    byte[] readBytes() {
        int length = readLength();
        byte[] value = new byte[length];
        buffer.get(value);
        return value;
    }

    <T> java.util.Optional<T> readOptional(WireRead<T> read) {
        return readBoolean()
            ? java.util.Optional.ofNullable(read.read())
            : java.util.Optional.empty();
    }

    <T> java.util.List<T> readSequence(WireRead<T> read) {
        int length = readCount();
        java.util.ArrayList<T> values = new java.util.ArrayList<>(length);
        int index = 0;
        while (index < length) {
            values.add(read.read());
            index += 1;
        }
        return values;
    }

    <K, V> java.util.Map<K, V> readMap(WireRead<K> readKey, WireRead<V> readValue) {
        int length = readCount();
        java.util.HashMap<K, V> values = new java.util.HashMap<>(mapCapacity(length));
        int index = 0;
        while (index < length) {
            K key = readKey.read();
            if (values.containsKey(key)) {
                throw new IllegalArgumentException("duplicate wire map key");
            }
            values.put(key, readValue.read());
            index += 1;
        }
        return values;
    }

    java.util.List<String> readStringSequence() {
        int length = readCount();
        java.util.ArrayList<String> values = new java.util.ArrayList<>(length);
        int index = 0;
        while (index < length) {
            values.add(readString());
            index += 1;
        }
        return values;
    }

    boolean[] readBooleanArray() {
        int length = readCount();
        boolean[] values = new boolean[length];
        int index = 0;
        while (index < length) {
            values[index] = readBoolean();
            index += 1;
        }
        return values;
    }

    byte[] readByteArray() { return readBytes(); }

    short[] readShortArray() {
        int length = readArrayLength(2);
        short[] values = new short[length];
        int byteCount = length * 2;
        buffer.asShortBuffer().get(values);
        buffer.position(buffer.position() + byteCount);
        return values;
    }

    int[] readIntArray() {
        int length = readArrayLength(4);
        int[] values = new int[length];
        int byteCount = length * 4;
        buffer.asIntBuffer().get(values);
        buffer.position(buffer.position() + byteCount);
        return values;
    }

    long[] readLongArray() {
        int length = readArrayLength(8);
        long[] values = new long[length];
        int byteCount = length * 8;
        buffer.asLongBuffer().get(values);
        buffer.position(buffer.position() + byteCount);
        return values;
    }

    float[] readFloatArray() {
        int length = readArrayLength(4);
        float[] values = new float[length];
        int byteCount = length * 4;
        buffer.asFloatBuffer().get(values);
        buffer.position(buffer.position() + byteCount);
        return values;
    }

    double[] readDoubleArray() {
        int length = readArrayLength(8);
        double[] values = new double[length];
        int byteCount = length * 8;
        buffer.asDoubleBuffer().get(values);
        buffer.position(buffer.position() + byteCount);
        return values;
    }

    private int readLength() {
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("invalid wire length");
        }
        return length;
    }

    private int readCount() {
        int count = buffer.getInt();
        if (count < 0) {
            throw new IllegalArgumentException("invalid wire count");
        }
        return count;
    }

    private int readArrayLength(int width) {
        int length = readCount();
        if (length > buffer.remaining() / width) {
            throw new IllegalArgumentException("invalid wire array length");
        }
        return length;
    }

    private int mapCapacity(int count) {
        return count < 3 ? count + 1 : Math.min(1 << 30, (int) Math.ceil(count / 0.75d));
    }
}

final class BoltFfiErrorBufferException extends RuntimeException {
    private final byte[] bytes;

    BoltFfiErrorBufferException(byte[] bytes) {
        super("BoltFFI call failed");
        this.bytes = bytes;
    }

    byte[] bytes() { return bytes; }
}

final class WireWriter {
    private final java.nio.ByteBuffer buffer;

    WireWriter(java.nio.ByteBuffer buffer) {
        this.buffer = buffer;
    }

    int size() { return buffer.position(); }
    void writeBoolean(boolean value) { buffer.put(value ? (byte) 1 : (byte) 0); }
    void writeByte(byte value) { buffer.put(value); }
    void writeShort(short value) { buffer.putShort(value); }
    void writeInt(int value) { buffer.putInt(value); }
    void writeLong(long value) { buffer.putLong(value); }
    void writeFloat(float value) { buffer.putFloat(value); }
    void writeDouble(double value) { buffer.putDouble(value); }

    void writeDuration(java.time.Duration value) {
        if (value.isNegative()) {
            throw new IllegalArgumentException("duration must be non-negative");
        }
        writeLong(value.getSeconds());
        writeInt(value.getNano());
    }

    void writeInstant(java.time.Instant value) {
        writeLong(value.getEpochSecond());
        writeInt(value.getNano());
    }

    void writeUuid(java.util.UUID value) {
        writeLong(value.getMostSignificantBits());
        writeLong(value.getLeastSignificantBits());
    }

    void writeUri(java.net.URI value) { writeString(value.toString()); }

    void writeString(String value) {
        writeBytes(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    void writeBytes(byte[] value) {
        writeInt(value.length);
        buffer.put(value);
    }

    <T> void writeOptional(java.util.Optional<T> value, WireWrite<T> write) {
        writeBoolean(value.isPresent());
        if (value.isPresent()) {
            write.write(value.get());
        }
    }

    <T> void writeSequence(java.util.List<T> values, WireWrite<T> write) {
        writeInt(values.size());
        int index = 0;
        while (index < values.size()) {
            write.write(values.get(index));
            index += 1;
        }
    }

    <K, V> void writeMap(
        java.util.Map<K, V> values,
        WireWrite<K> writeKey,
        WireWrite<V> writeValue
    ) {
        writeInt(values.size());
        java.util.Iterator<java.util.Map.Entry<K, V>> entries = values.entrySet().iterator();
        while (entries.hasNext()) {
            java.util.Map.Entry<K, V> entry = entries.next();
            writeKey.write(entry.getKey());
            writeValue.write(entry.getValue());
        }
    }

    void writeStringSequence(java.util.List<String> values) {
        writeInt(values.size());
        int index = 0;
        while (index < values.size()) {
            writeString(values.get(index));
            index += 1;
        }
    }

    void writeBooleanArray(boolean[] values) {
        writeInt(values.length);
        int index = 0;
        while (index < values.length) {
            writeBoolean(values[index]);
            index += 1;
        }
    }

    void writeByteArray(byte[] values) { writeBytes(values); }

    void writeShortArray(short[] values) {
        writeInt(values.length);
        int byteCount = Math.multiplyExact(values.length, 2);
        buffer.asShortBuffer().put(values);
        buffer.position(buffer.position() + byteCount);
    }

    void writeIntArray(int[] values) {
        writeInt(values.length);
        int byteCount = Math.multiplyExact(values.length, 4);
        buffer.asIntBuffer().put(values);
        buffer.position(buffer.position() + byteCount);
    }

    void writeLongArray(long[] values) {
        writeInt(values.length);
        int byteCount = Math.multiplyExact(values.length, 8);
        buffer.asLongBuffer().put(values);
        buffer.position(buffer.position() + byteCount);
    }

    void writeFloatArray(float[] values) {
        writeInt(values.length);
        int byteCount = Math.multiplyExact(values.length, 4);
        buffer.asFloatBuffer().put(values);
        buffer.position(buffer.position() + byteCount);
    }

    void writeDoubleArray(double[] values) {
        writeInt(values.length);
        int byteCount = Math.multiplyExact(values.length, 8);
        buffer.asDoubleBuffer().put(values);
        buffer.position(buffer.position() + byteCount);
    }
}

final class WireSizes {
    private WireSizes() {}

    static int string(String value) {
        return Math.addExact(4, Math.multiplyExact(value.length(), 3));
    }

    static <T> int optional(java.util.Optional<T> value, WireSize<T> size) {
        return value.isPresent() ? Math.addExact(1, size.size(value.get())) : 1;
    }

    static <T> int sequence(java.util.List<T> values, WireSize<T> size) {
        int total = 4;
        int index = 0;
        while (index < values.size()) {
            total = Math.addExact(total, size.size(values.get(index)));
            index += 1;
        }
        return total;
    }

    static <K, V> int map(
        java.util.Map<K, V> values,
        WireSize<K> keySize,
        WireSize<V> valueSize
    ) {
        int total = 4;
        java.util.Iterator<java.util.Map.Entry<K, V>> entries = values.entrySet().iterator();
        while (entries.hasNext()) {
            java.util.Map.Entry<K, V> entry = entries.next();
            total = Math.addExact(total, keySize.size(entry.getKey()));
            total = Math.addExact(total, valueSize.size(entry.getValue()));
        }
        return total;
    }

    static int stringSequence(java.util.List<String> values) {
        int total = 4;
        int index = 0;
        while (index < values.size()) {
            total = Math.addExact(total, string(values.get(index)));
            index += 1;
        }
        return total;
    }

}

final class WireLease implements AutoCloseable {
    private final WireWriterPoolState owner;
    private final java.nio.ByteBuffer buffer;
    private final WireWriter writer;
    private boolean closed;

    WireLease(WireWriterPoolState owner, java.nio.ByteBuffer buffer) {
        this.owner = owner;
        this.buffer = buffer;
        this.writer = new WireWriter(buffer);
        reopen();
    }

    WireWriter writer() { return writer; }
    java.nio.ByteBuffer directBuffer() { return buffer; }
    int size() { return writer.size(); }
    int capacity() { return buffer.capacity(); }

    WireLease reopen() {
        buffer.clear();
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        closed = false;
        return this;
    }

    byte[] bytes() {
        java.nio.ByteBuffer source = buffer.duplicate();
        source.flip();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            owner.release(this);
        }
    }
}

final class WireWriterPoolState {
    private static final int CACHE_SIZE = 4;
    private final java.util.ArrayDeque<WireLease> leases = new java.util.ArrayDeque<>(CACHE_SIZE);

    WireLease acquire(int capacity) {
        int required = Math.max(capacity, 1);
        int remaining = leases.size();
        while (remaining > 0) {
            WireLease candidate = leases.pollFirst();
            if (candidate.capacity() >= required) return candidate.reopen();
            leases.offerLast(candidate);
            remaining -= 1;
        }
        return new WireLease(this, java.nio.ByteBuffer.allocateDirect(required));
    }

    void release(WireLease lease) {
        if (leases.size() < CACHE_SIZE) {
            leases.addFirst(lease);
        }
    }
}

final class WireWriterPool {
    private static final ThreadLocal<WireWriterPoolState> STATE =
        ThreadLocal.withInitial(WireWriterPoolState::new);

    private WireWriterPool() {}

    static WireLease acquire(int capacity) {
        return STATE.get().acquire(capacity);
    }
}

public final class ZenohFfi {
    private ZenohFfi() {}
}