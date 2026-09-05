package com.workoss.zenoh;

    /**
     * A reply received from a query (get operation).
     * Can be either a success (contains Sample) or an error.
     * Pure `#[data]` type — fields are wire-encoded and accessible through
     * generated bindings. No `#[export]` so boltffi treats it as a value type.
     */
public final class Reply {
    public final boolean success;
    public final java.util.Optional<String> sampleKeyExpr;
    public final java.util.Optional<byte[]> samplePayload;
    public final java.util.Optional<String> sampleEncoding;
    public final java.util.Optional<Byte> sampleKind;
    public final java.util.Optional<String> sampleTimestamp;
    public final java.util.Optional<byte[]> errorPayload;
    public final java.util.Optional<String> errorEncoding;

    public Reply(boolean success, java.util.Optional<String> sampleKeyExpr, java.util.Optional<byte[]> samplePayload, java.util.Optional<String> sampleEncoding, java.util.Optional<Byte> sampleKind, java.util.Optional<String> sampleTimestamp, java.util.Optional<byte[]> errorPayload, java.util.Optional<String> errorEncoding) {
        this.success = success;
        this.sampleKeyExpr = sampleKeyExpr;
        this.samplePayload = samplePayload;
        this.sampleEncoding = sampleEncoding;
        this.sampleKind = sampleKind;
        this.sampleTimestamp = sampleTimestamp;
        this.errorPayload = errorPayload;
        this.errorEncoding = errorEncoding;
    }


    public boolean success() {
        return success;
    }

    public java.util.Optional<String> sampleKeyExpr() {
        return sampleKeyExpr;
    }

    public java.util.Optional<byte[]> samplePayload() {
        return samplePayload;
    }

    public java.util.Optional<String> sampleEncoding() {
        return sampleEncoding;
    }

    public java.util.Optional<Byte> sampleKind() {
        return sampleKind;
    }

    public java.util.Optional<String> sampleTimestamp() {
        return sampleTimestamp;
    }

    public java.util.Optional<byte[]> errorPayload() {
        return errorPayload;
    }

    public java.util.Optional<String> errorEncoding() {
        return errorEncoding;
    }


    @Override
    public boolean equals(Object value) {
        if (this == value) return true;
        if (value == null || getClass() != value.getClass()) return false;
        Reply other = (Reply) value;
        return this.success == other.success && BoltFFIValueIdentity.optionalEquals(this.sampleKeyExpr, other.sampleKeyExpr, (leftValue0, rightValue0) -> java.util.Objects.equals(leftValue0, rightValue0)) && BoltFFIValueIdentity.optionalEquals(this.samplePayload, other.samplePayload, (leftValue0, rightValue0) -> java.util.Arrays.equals(leftValue0, rightValue0)) && BoltFFIValueIdentity.optionalEquals(this.sampleEncoding, other.sampleEncoding, (leftValue0, rightValue0) -> java.util.Objects.equals(leftValue0, rightValue0)) && BoltFFIValueIdentity.optionalEquals(this.sampleKind, other.sampleKind, (leftValue0, rightValue0) -> leftValue0 == rightValue0) && BoltFFIValueIdentity.optionalEquals(this.sampleTimestamp, other.sampleTimestamp, (leftValue0, rightValue0) -> java.util.Objects.equals(leftValue0, rightValue0)) && BoltFFIValueIdentity.optionalEquals(this.errorPayload, other.errorPayload, (leftValue0, rightValue0) -> java.util.Arrays.equals(leftValue0, rightValue0)) && BoltFFIValueIdentity.optionalEquals(this.errorEncoding, other.errorEncoding, (leftValue0, rightValue0) -> java.util.Objects.equals(leftValue0, rightValue0));
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + Boolean.hashCode(this.success);
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.sampleKeyExpr, (itemValue0) -> java.util.Objects.hashCode(itemValue0));
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.samplePayload, (itemValue0) -> java.util.Arrays.hashCode(itemValue0));
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.sampleEncoding, (itemValue0) -> java.util.Objects.hashCode(itemValue0));
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.sampleKind, (itemValue0) -> Byte.hashCode(itemValue0));
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.sampleTimestamp, (itemValue0) -> java.util.Objects.hashCode(itemValue0));
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.errorPayload, (itemValue0) -> java.util.Arrays.hashCode(itemValue0));
        result = 31 * result + BoltFFIValueIdentity.optionalHash(this.errorEncoding, (itemValue0) -> java.util.Objects.hashCode(itemValue0));
        return result;
    }

    @Override
    public String toString() {
        return "Reply{" +
            "success=" + success +
            ", sampleKeyExpr=" + sampleKeyExpr +
            ", samplePayload=" + samplePayload +
            ", sampleEncoding=" + sampleEncoding +
            ", sampleKind=" + sampleKind +
            ", sampleTimestamp=" + sampleTimestamp +
            ", errorPayload=" + errorPayload +
            ", errorEncoding=" + errorEncoding
            + '}';
    }


    int wireSize() {
        return (((((((1 + WireSizes.optional(this.sampleKeyExpr, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0))) + WireSizes.optional(this.samplePayload, (__boltffi_value_0) -> (4 + __boltffi_value_0.length))) + WireSizes.optional(this.sampleEncoding, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0))) + WireSizes.optional(this.sampleKind, (__boltffi_value_0) -> 1)) + WireSizes.optional(this.sampleTimestamp, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0))) + WireSizes.optional(this.errorPayload, (__boltffi_value_0) -> (4 + __boltffi_value_0.length))) + WireSizes.optional(this.errorEncoding, (__boltffi_value_0) -> WireSizes.string(__boltffi_value_0)));
    }

    void writeTo(WireWriter writer) {
        writer.writeBoolean(this.success);
        writer.writeOptional(this.sampleKeyExpr, (__boltffi_value_0) -> { writer.writeString(__boltffi_value_0); });
        writer.writeOptional(this.samplePayload, (__boltffi_value_0) -> { writer.writeBytes(__boltffi_value_0); });
        writer.writeOptional(this.sampleEncoding, (__boltffi_value_0) -> { writer.writeString(__boltffi_value_0); });
        writer.writeOptional(this.sampleKind, (__boltffi_value_0) -> { writer.writeByte(__boltffi_value_0); });
        writer.writeOptional(this.sampleTimestamp, (__boltffi_value_0) -> { writer.writeString(__boltffi_value_0); });
        writer.writeOptional(this.errorPayload, (__boltffi_value_0) -> { writer.writeBytes(__boltffi_value_0); });
        writer.writeOptional(this.errorEncoding, (__boltffi_value_0) -> { writer.writeString(__boltffi_value_0); });
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

    static Reply fromReader(WireReader reader) {
        return new Reply(reader.readBoolean(), reader.readOptional(() -> reader.readString()), reader.readOptional(() -> reader.readBytes()), reader.readOptional(() -> reader.readString()), reader.readOptional(() -> reader.readByte()), reader.readOptional(() -> reader.readString()), reader.readOptional(() -> reader.readBytes()), reader.readOptional(() -> reader.readString()));
    }

    static Reply fromByteArray(byte[] bytes) {
        return fromReader(new WireReader(bytes));
    }
}