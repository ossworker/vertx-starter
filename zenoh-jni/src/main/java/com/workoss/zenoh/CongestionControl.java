package com.workoss.zenoh;

    /**
     * Congestion control strategy
     */
public enum CongestionControl {
    DROP(0),
    BLOCK(1),
    BLOCK_FIRST(2);

    public final int value;

    CongestionControl(int value) {
        this.value = value;
    }

    public static CongestionControl fromValue(int value) {
        switch (value) {
            case 0: return DROP;
            case 1: return BLOCK;
            case 2: return BLOCK_FIRST;
            default: break;
        }
        throw new IllegalArgumentException("Unknown CongestionControl value: " + value);
    }

    int nativeValue() {
        return value;
    }
}