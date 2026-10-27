package com.workoss.boot.zenoh.cluster.internal.messaging;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Broadcast address wildcard matching")
class AddressMatcherTest {

    private static void assertMatches(String pattern, String... addresses) {
        for (String address : addresses) {
            assertTrue(AddressMatcher.matches(pattern, address), pattern + " should match " + address);
        }
    }

    private static void assertNoMatch(String pattern, String... addresses) {
        for (String address : addresses) {
            assertFalse(AddressMatcher.matches(pattern, address), pattern + " should not match " + address);
        }
    }

    @Test
    @DisplayName("a literal address matches only itself")
    void literal() {
        assertFalse(AddressMatcher.isPattern("news/eu"));
        assertMatches("news/eu", "news/eu");
        assertNoMatch("news/eu", "news/us", "news", "news/eu/west");
    }

    @Test
    @DisplayName("* matches exactly one segment and never spans a slash")
    void singleStar() {
        assertTrue(AddressMatcher.isPattern("news/*"));
        assertMatches("news/*", "news/eu", "news/us");
        assertNoMatch("news/*", "news", "news/eu/west", "orders/eu");
        assertMatches("*", "anything");
        assertNoMatch("*", "a/b");
    }

    @Test
    @DisplayName("** matches zero or more segments across slashes")
    void doubleStar() {
        assertMatches("news/**", "news", "news/eu", "news/eu/west");
        assertNoMatch("news/**", "orders/eu");
        assertMatches("**", "a", "a/b", "a/b/c");
        assertMatches("a/**/c", "a/c", "a/b/c", "a/b/d/c");
        assertNoMatch("a/**/c", "a/b/d");
    }

    @Test
    @DisplayName("mixed literal and wildcard segments")
    void mixed() {
        assertMatches("orders/*/shipped", "orders/eu/shipped", "orders/us/shipped");
        assertNoMatch("orders/*/shipped", "orders/eu/pending", "orders/shipped");
    }
}
