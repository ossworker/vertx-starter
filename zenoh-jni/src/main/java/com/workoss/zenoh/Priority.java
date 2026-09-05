package com.workoss.zenoh;

    /**
     * Message priority
     */
public enum Priority {
    BACKGROUND(1),
    DATA_LOW(2),
    DATA(3),
    DATA_HIGH(4),
    INTERACTIVE_LOW(5),
    INTERACTIVE_HIGH(6),
    REAL_TIME(7);

    public final int value;

    Priority(int value) {
        this.value = value;
    }

    public static Priority fromValue(int value) {
        switch (value) {
            case 1: return BACKGROUND;
            case 2: return DATA_LOW;
            case 3: return DATA;
            case 4: return DATA_HIGH;
            case 5: return INTERACTIVE_LOW;
            case 6: return INTERACTIVE_HIGH;
            case 7: return REAL_TIME;
            default: break;
        }
        throw new IllegalArgumentException("Unknown Priority value: " + value);
    }

    int nativeValue() {
        return value;
    }
}