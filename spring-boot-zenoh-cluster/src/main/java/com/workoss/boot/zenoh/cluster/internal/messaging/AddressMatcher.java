package com.workoss.boot.zenoh.cluster.internal.messaging;

/**
 * Matches a broadcast address against a subscription pattern, using Zenoh key-expression segment
 * rules on the {@code /}-separated address:
 * <ul>
 *   <li>{@code *} matches exactly one segment and never spans a {@code /}</li>
 *   <li>{@code **} matches zero or more segments, spanning {@code /}</li>
 *   <li>any other segment is compared literally</li>
 * </ul>
 * So {@code news/*} matches {@code news/eu} but not {@code news} or {@code news/eu/west}, while
 * {@code news/**} matches all three.
 */
public final class AddressMatcher {

    private AddressMatcher() {
    }

    /**
     * Whether {@code pattern} contains a wildcard and therefore cannot be used as an exact key.
     */
    public static boolean isPattern(String pattern) {
        return pattern.indexOf('*') >= 0;
    }

    public static boolean matches(String pattern, String address) {
        if (pattern == null || address == null) {
            return false;
        }
        if (!isPattern(pattern)) {
            return pattern.equals(address);
        }
        return matchSegments(pattern.split("/", -1), 0, address.split("/", -1), 0);
    }

    private static boolean matchSegments(String[] pattern, int pi, String[] address, int ai) {
        while (pi < pattern.length && ai < address.length) {
            String segment = pattern[pi];
            if (segment.equals("**")) {
                for (int skip = ai; skip <= address.length; skip++) {
                    if (matchSegments(pattern, pi + 1, address, skip)) {
                        return true;
                    }
                }
                return false;
            }
            if (segment.equals("*") || segment.equals(address[ai])) {
                pi++;
                ai++;
            } else {
                return false;
            }
        }
        // Trailing "**" also matches zero remaining segments.
        while (pi < pattern.length && pattern[pi].equals("**")) {
            pi++;
        }
        return pi == pattern.length && ai == address.length;
    }
}
