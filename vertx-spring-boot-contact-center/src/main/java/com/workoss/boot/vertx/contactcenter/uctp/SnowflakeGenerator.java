package com.workoss.boot.vertx.contactcenter.uctp;

import java.util.Objects;
import java.util.function.LongSupplier;

public final class SnowflakeGenerator {

    public static final long EPOCH = 1704067200000L;
    private static final long MAX_TIMESTAMP = EPOCH + (1L << 41) - 1;
    private static final int MAX_SEQUENCE = (1 << 12) - 1;

    public record Window(int nodeId, String owner, long from, long until) {
        public Window {
            if (nodeId < 0 || nodeId > 1023) {
                throw new IllegalArgumentException("nodeId must be in 0..1023");
            }
            Objects.requireNonNull(owner, "owner");
            if (owner.isBlank() || owner.length() > 64) {
                throw new IllegalArgumentException("owner must contain 1..64 characters");
            }
            if (from < EPOCH || from > until || until > MAX_TIMESTAMP) {
                throw new IllegalArgumentException("window must fit the 41-bit timestamp range");
            }
        }

        public int datacenterId() {
            return nodeId >>> 5;
        }

        public int workerId() {
            return nodeId & 31;
        }
    }

    private final LongSupplier clock;
    private Window window;
    private long lastTimestamp = EPOCH - 1;
    private int sequence;
    private boolean revoked;

    public SnowflakeGenerator(Window window) {
        this(window, System::currentTimeMillis);
    }

    public SnowflakeGenerator(Window window, LongSupplier clock) {
        this.window = Objects.requireNonNull(window, "window");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public synchronized long nextId() {
        if (revoked) {
            throw new IllegalStateException("Snowflake node authorization revoked");
        }
        long timestamp = Math.max(Math.max(clock.getAsLong(), window.from()), lastTimestamp);
        int nextSequence = timestamp == lastTimestamp ? sequence + 1 : 0;
        if (nextSequence > MAX_SEQUENCE) {
            // Logical time avoids spinning on an event-loop thread or a rolled-back clock.
            timestamp++;
            nextSequence = 0;
        }
        if (timestamp > window.until()) {
            throw new IllegalStateException("Snowflake authorized window exhausted");
        }
        // Reserve zero so every public ID is a positive long, including at the epoch.
        if (timestamp == EPOCH && window.nodeId() == 0 && nextSequence == 0) {
            nextSequence = 1;
        }
        lastTimestamp = timestamp;
        sequence = nextSequence;
        return ((timestamp - EPOCH) << 22) | ((long) window.nodeId() << 12) | sequence;
    }

    public synchronized Window window() {
        return window;
    }

    public synchronized void extend(Window extended) {
        Objects.requireNonNull(extended, "extended");
        if (extended.nodeId() != window.nodeId() || !extended.owner().equals(window.owner())
                || extended.from() != window.from() || extended.until() < window.until()) {
            throw new IllegalArgumentException("extension must preserve the node, owner and range origin");
        }
        window = extended;
    }

    public synchronized void revoke() {
        revoked = true;
    }

    public synchronized boolean revoked() {
        return revoked;
    }

    /** Timestamp of the last issued ID, or {@code EPOCH - 1} when none was issued. */
    public synchronized long lastTimestamp() {
        return lastTimestamp;
    }

    /**
     * Switches to a newly leased node. Logical time stays monotonic, so even when the same node is
     * leased again no earlier (timestamp, sequence) pair can be reissued.
     */
    public synchronized void reassign(Window next) {
        Objects.requireNonNull(next, "next");
        if (!next.owner().equals(window.owner())) {
            throw new IllegalArgumentException("reassignment must preserve the owner");
        }
        window = next;
        revoked = false;
    }
}
