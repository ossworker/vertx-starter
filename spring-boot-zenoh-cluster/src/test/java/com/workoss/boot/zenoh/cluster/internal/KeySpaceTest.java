package com.workoss.boot.zenoh.cluster.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Key escaping and keyspace layout")
class KeySpaceTest {

    private static final List<String> SAMPLES = List.of("plain", "a/b", "a%2Fb", "*", "**", "?", "$*", "#x",
            "with space", "中文", "a.b-c_d", "%", "%%", "trailing%2");

    @Test
    @DisplayName("escaping is injective, reversible and yields a single wildcard-free chunk")
    void escapeRoundTrip() {
        Set<String> escaped = new HashSet<>();
        for (String sample : SAMPLES) {
            String chunk = KeyEscaper.escape(sample);
            assertTrue(chunk.matches("[A-Za-z0-9_.%-]+"), chunk);
            assertEquals(sample, KeyEscaper.unescape(chunk));
            escaped.add(chunk);
        }
        assertEquals(SAMPLES.size(), escaped.size());
    }

    @Test
    @DisplayName("empty components are rejected except where explicitly allowed")
    void emptyComponents() {
        assertThrows(IllegalArgumentException.class, () -> KeyEscaper.escape(""));
        assertEquals("", KeyEscaper.unescape(KeyEscaper.escapeAllowEmpty("")));
    }

    @Test
    @DisplayName("names keep '/' as chunk separator and decode back to the original id")
    void multiSegmentNames() {
        KeySpace keys = new KeySpace("team/my app", "prod/eu");
        assertEquals("zenoh/team/my%20app/prod/eu", keys.root());
        assertEquals("zenoh/team/my%20app/prod/eu/members/node/1", keys.memberKey("node/1"));
        assertEquals("zenoh/team/my%20app/prod/eu/members/**", keys.membersPattern());
        assertEquals("node/1", keys.memberId(keys.memberKey("node/1")));
        assertNull(keys.memberId(keys.broadcastKey("node/1")));
        assertEquals("zenoh/team/my%20app/prod/eu/bus/inbox/edge/n%2A", keys.inboxKey("edge/n*"));
        assertEquals("maps/orders/eu", KeySpace.mapNamespace("orders/eu"));
        assertEquals("counters/hits/eu%3F", KeySpace.counterNamespace("hits/eu?"));
        assertEquals("locks/jobs/nightly", KeySpace.lockNamespace("jobs/nightly"));
        assertEquals("default", new KeySpace(null, " ").root().split("/")[1]);
        assertThrows(IllegalArgumentException.class, () -> keys.memberKey("a//b"));
        assertThrows(IllegalArgumentException.class, () -> KeySpace.lockNamespace("jobs/"));
    }

    @Test
    @DisplayName("broadcast addresses keep '/' as chunk separator and escape each segment")
    void broadcastKeys() {
        KeySpace keys = new KeySpace("app", "env");
        assertEquals("zenoh/app/env/bus/broadcast/demo/broadcast", keys.broadcastKey("demo/broadcast"));
        assertEquals("zenoh/app/env/bus/broadcast/orders.%2A/a%20b", keys.broadcastKey("orders.*/a b"));
        assertEquals("zenoh/app/env/bus/broadcast/**", keys.broadcastPattern());
        for (String address : List.of("demo/broadcast", "news", "orders.*/a b", "中文/x", "a%2Fb/c")) {
            assertEquals(address, keys.broadcastAddress(keys.broadcastKey(address)));
        }
        assertNull(keys.broadcastAddress(keys.memberKey("n1")));
        assertNull(keys.broadcastAddress("zenoh/app/env/bus/broadcast/"));
        for (String bad : List.of("", "/a", "a/", "a//b", "/")) {
            assertThrows(IllegalArgumentException.class, () -> keys.broadcastKey(bad), bad);
        }
    }
}
