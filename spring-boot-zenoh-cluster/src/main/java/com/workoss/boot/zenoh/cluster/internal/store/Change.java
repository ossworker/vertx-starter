package com.workoss.boot.zenoh.cluster.internal.store;

/**
 * Outcome of a {@link ReplicatedStore#update} function.
 */
public sealed interface Change {

    Change KEEP = new Keep();
    Change DELETE = new Delete();

    static Change put(byte[] value) {
        return new Put(value, 0L, false);
    }

    /**
     * @param ttlMillis {@code <= 0} means no expiry
     */
    static Change put(byte[] value, long ttlMillis) {
        return new Put(value, ttlMillis, false);
    }

    /**
     * Put that keeps the current entry's expiry.
     */
    static Change replace(byte[] value) {
        return new Put(value, 0L, true);
    }

    record Keep() implements Change {
    }

    record Delete() implements Change {
    }

    record Put(byte[] value, long ttlMillis, boolean keepExpiry) implements Change {
        public Put {
            if (value == null) {
                throw new IllegalArgumentException("Stored value must not be null");
            }
        }
    }
}
