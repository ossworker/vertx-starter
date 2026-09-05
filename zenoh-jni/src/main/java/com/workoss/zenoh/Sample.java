package com.workoss.zenoh;

    /**
     * A data sample received from zenoh.
     * Pure `#[data]` type — fields are wire-encoded and accessible through
     * generated bindings. No `#[export]` so boltffi treats it as a value type.
     */
public final class Sample {
    public final String keyExpr;
    public final byte[] payload;
    public final String encoding;
    public final byte kind;
    public final java.util.Optional<String> timestamp;
    public final byte qosPriority;
    public final byte qosCongestionControl;
    public final boolean qosExpress;

    public Sample(String keyExpr, byte[] payload, String encoding, byte kind, java.util.Optional<String> timestamp, byte qosPriority, byte qosCongestionControl, boolean qosExpress) {
        this.keyExpr = keyExpr;
        this.payload = payload;
        this.encoding = encoding;
        this.kind = kind;
        this.timestamp = timestamp;
        this.qosPriority = qosPriority;
        this.qosCongestionControl = qosCongestionControl;
        this.qosExpress = qosExpress;
    }


    public String keyExpr() {
        return keyExpr;
    }

    public byte[] payload() {
        return payload;
    }

    public String encoding() {
        return encoding;
    }

    public byte kind() {
        return kind;
    }

    public java.util.Optional<String> timestamp() {
        return timestamp;
    }

    public byte qosPriority() {
        return qosPriority;
    }

    public byte qosCongestionControl() {
        return qosCongestionControl;
    }

    public boolean qosExpress() {
        return qosExpress;
    }


    @Override
    public boolean equals(Object value) {
        if (this == value) return true;
        if (value == null || getClass() != value.getClass()) return false;
        Sample other = (Sample) value;
        return java.util.Objects.equals(this.keyExpr, other.keyExpr) && java.util.Arrays.equals(this.payload, other.payload) && java.util.Objects.equals(this.encoding, other.encoding) && this.kind == other.kind && BoltFFIValueIdentity.optionalEquals(this.timestamp, other.timestamp, (leftValue0, rightValue0) -> java.util.Objects.equals(leftValue0, rightValue0)) && this.qosPriority == other.qosPriority && this.qosCongestionControl == other.qosCongestionControl && this.qosExpress == other.qosExpress;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + java.util.Objects.hashCode(this.keyExpr);
        result = 31 * result + java.util.Arrays.hashCode(this.payload);
        result = 31 * result + java.util.Objects.hashCode(this.encoding);
        result = 31 * result + Byte.hashCode(this.kind);
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.timestamp, (itemValue0) -> java.util.Objects.hashCode(itemValue0));
        result = 31 * result + Byte.hashCode(this.qosPriority);
        result = 31 * result + Byte.hashCode(this.qosCongestionControl);
        result = 31 * result + Boolean.hashCode(this.qosExpress);
        return result;
    }

    @Override
    public String toString() {
        return "Sample{" +
            "keyExpr=" + keyExpr +
            ", payload=" + payload +
            ", encoding=" + encoding +
            ", kind=" + kind +
            ", timestamp=" + timestamp +
            ", qosPriority=" + qosPriority +
            ", qosCongestionControl=" + qosCongestionControl +
            ", qosExpress=" + qosExpress
            + '}';
    }


    int wireSize() {
        return (((((((WireSizes.string(this.keyExpr) + (4 + this.payload.length)) + WireSizes.string(this.encoding)) + 1) + WireSizes.optional(this.timestamp, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0))) + 1) + 1) + 1);
    }

    void writeTo(WireWriter writer) {
        writer.writeString(this.keyExpr);
        writer.writeBytes(this.payload);
        writer.writeString(this.encoding);
        writer.writeByte(this.kind);
        writer.writeOptional(this.timestamp, (__boltffi_value_0) -> { writer.writeString(__boltffi_value_0); });
        writer.writeByte(this.qosPriority);
        writer.writeByte(this.qosCongestionControl);
        writer.writeBoolean(this.qosExpress);
    }

    byte[] toByteArray() {
        WireLease lease = WireWriterPool.acquire(wireSize());
        try {
            writeTo(lease.writer());
            return lease.bytes();
        } finally {
            lease.close();
        }
    }

    static Sample fromReader(WireReader reader) {
        return new Sample(reader.readString(), reader.readBytes(), reader.readString(), reader.readByte(), reader.readOptional(() -> reader.readString()), reader.readByte(), reader.readByte(), reader.readBoolean());
    }

    static Sample fromByteArray(byte[] bytes) {
        return fromReader(new WireReader(bytes));
    }
}