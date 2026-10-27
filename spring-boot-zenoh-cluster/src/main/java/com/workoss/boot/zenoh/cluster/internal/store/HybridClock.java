package com.workoss.boot.zenoh.cluster.internal.store;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Hybrid logical clock: versions are {@code wallMillis << 16 | logical}. A node's next version is
 * always greater than every version it has produced or observed, so a write made after seeing
 * another write wins against it regardless of clock skew; unrelated concurrent writes are ordered by
 * wall time.
 */
public final class HybridClock {

    private static final int LOGICAL_BITS = 16;

    private final AtomicLong last = new AtomicLong();
    private final LongSupplier wallClock;

    public HybridClock() {
        this(System::currentTimeMillis);
    }

    public HybridClock(LongSupplier wallClock) {
        this.wallClock = wallClock;
    }

    public long next() {
        long physical = wallClock.getAsLong() << LOGICAL_BITS;
        return last.updateAndGet(prev -> Math.max(prev + 1, physical));
    }

    public void observe(long version) {
        last.accumulateAndGet(version, Math::max);
    }

    public static long wallMillis(long version) {
        return version >>> LOGICAL_BITS;
    }
}
