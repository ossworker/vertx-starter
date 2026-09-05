package com.workoss.zenoh;

    /**
     * Reliability level
     */
public enum Reliability {
    BEST_EFFORT(0),
    RELIABLE(1);

    public final int value;

    Reliability(int value) {
        this.value = value;
    }

    public static Reliability fromValue(int value) {
        switch (value) {
            case 0: return BEST_EFFORT;
            case 1: return RELIABLE;
            default: break;
        }
        throw new IllegalArgumentException("Unknown Reliability value: " + value);
    }

    int nativeValue() {
        return value;
    }
}