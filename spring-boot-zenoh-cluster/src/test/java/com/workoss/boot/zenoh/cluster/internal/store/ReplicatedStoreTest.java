package com.workoss.boot.zenoh.cluster.internal.store;

import com.workoss.boot.zenoh.cluster.internal.KeySpace;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ReplicatedStore convergence")
class ReplicatedStoreTest {

    private static final String PREFIX = new KeySpace("app", "test").storePrefix();
    private static final String NS = KeySpace.mapNamespace("m");

    private record Write(String key, byte[] payload) {
    }

    private static ReplicatedStore store(String nodeId, List<Write> outbox) {
        ReplicatedStore store = new ReplicatedStore(PREFIX, nodeId, new HybridClock(), 60_000L);
        store.setSink((key, payload) -> outbox.add(new Write(key, payload)));
        return store;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("concurrent writes to one key converge to the same winner whatever the arrival order")
    void concurrentWritesConverge() {
        List<Write> outA = new ArrayList<>();
        List<Write> outB = new ArrayList<>();
        ReplicatedStore a = store("node-a", outA);
        ReplicatedStore b = store("node-b", outB);
        for (int i = 0; i < 50; i++) {
            int n = i;
            a.update(NS, "k", current -> Change.put(bytes("a" + n)));
            b.update(NS, "k", current -> Change.put(bytes("b" + n)));
        }
        List<Write> toB = new ArrayList<>(outA);
        List<Write> toA = new ArrayList<>(outB);
        Collections.shuffle(toB);
        Collections.reverse(toA);
        toB.forEach(w -> b.applyRemote(w.key(), w.payload()));
        toA.forEach(w -> a.applyRemote(w.key(), w.payload()));

        assertEquals(text(a.get(NS, "k")), text(b.get(NS, "k")));
    }

    @Test
    @DisplayName("an echoed or older write never regresses a newer value")
    void olderWriteDoesNotRegress() {
        List<Write> out = new ArrayList<>();
        ReplicatedStore a = store("node-a", out);
        a.update(NS, "k", current -> Change.put(bytes("v1")));
        a.update(NS, "k", current -> Change.put(bytes("v2")));

        assertFalse(a.applyRemote(out.get(0).key(), out.get(0).payload()), "stale v1 echo");
        assertFalse(a.applyRemote(out.get(1).key(), out.get(1).payload()), "own v2 echo");
        assertEquals("v2", text(a.get(NS, "k")));
    }

    @Test
    @DisplayName("a tombstone prevents a delayed older write from resurrecting the entry")
    void tombstonePreventsResurrection() {
        List<Write> outA = new ArrayList<>();
        ReplicatedStore a = store("node-a", outA);
        ReplicatedStore b = store("node-b", new ArrayList<>());
        a.update(NS, "k", current -> Change.put(bytes("v")));
        a.update(NS, "k", current -> Change.DELETE);
        Write put = outA.get(0);
        Write delete = outA.get(1);

        b.applyRemote(delete.key(), delete.payload());
        assertFalse(b.applyRemote(put.key(), put.payload()));
        assertNull(b.get(NS, "k"));
    }

    @Test
    @DisplayName("bootstrap through forEachMatching carries tombstones")
    void bootstrapIncludesTombstones() {
        List<Write> outA = new ArrayList<>();
        ReplicatedStore a = store("node-a", outA);
        a.update(NS, "live", current -> Change.put(bytes("1")));
        a.update(NS, "gone", current -> Change.put(bytes("2")));
        Write stalePut = outA.get(1);
        a.update(NS, "gone", current -> Change.DELETE);

        ReplicatedStore b = store("node-b", new ArrayList<>());
        a.forEachMatching(PREFIX + "/**", b::applyRemote);

        assertEquals("1", text(b.get(NS, "live")));
        assertFalse(b.applyRemote(stalePut.key(), stalePut.payload()));
        assertEquals(Set.of("live"), b.entries(NS).keySet());
    }

    @Test
    @DisplayName("ttl expiry is absolute and the sweep turns expired entries into tombstones")
    void ttlExpiry() throws InterruptedException {
        ReplicatedStore a = store("node-a", new ArrayList<>());
        a.update(NS, "k", current -> Change.put(bytes("v"), 100));
        assertEquals("v", text(a.get(NS, "k")));
        Thread.sleep(150);
        assertNull(a.get(NS, "k"));
        assertEquals(0, a.size(NS));
        a.sweep();
        assertEquals(1, storedKeys(a).size(), "expired entry is kept as a tombstone");
    }

    @Test
    @DisplayName("tombstones are kept for the retention period, then purged")
    void tombstonePurge() {
        ReplicatedStore recent = new ReplicatedStore(PREFIX, "node-a", new HybridClock(), 60_000L);
        recent.update(NS, "k", current -> Change.put(bytes("v")));
        recent.update(NS, "k", current -> Change.DELETE);
        recent.sweep();
        assertEquals(1, storedKeys(recent).size());

        // Purge age comes from the version's wall time, so a clock stuck at epoch makes it ancient.
        ReplicatedStore ancient = new ReplicatedStore(PREFIX, "node-a", new HybridClock(() -> 0L), 60_000L);
        ancient.update(NS, "k", current -> Change.put(bytes("v")));
        ancient.update(NS, "k", current -> Change.DELETE);
        ancient.sweep();
        assertTrue(storedKeys(ancient).isEmpty());
    }

    private static List<String> storedKeys(ReplicatedStore store) {
        List<String> keys = new ArrayList<>();
        store.forEachMatching(PREFIX + "/**", (key, payload) -> keys.add(key));
        return keys;
    }

    @Test
    @DisplayName("replace keeps the existing expiry; a delete of an absent key writes nothing")
    void replaceAndNoopDelete() {
        List<Write> out = new ArrayList<>();
        ReplicatedStore a = store("node-a", out);
        assertFalse(a.update(NS, "missing", current -> Change.DELETE).changed());
        assertTrue(out.isEmpty());

        a.update(NS, "k", current -> Change.put(bytes("v1"), 60_000));
        ReplicatedStore.Result result = a.update(NS, "k", current -> Change.replace(bytes("v2")));
        assertArrayEquals(bytes("v1"), result.previous());
        assertTrue(result.changed());
        assertEquals("v2", text(a.get(NS, "k")));
    }

    @Test
    @DisplayName("malformed or foreign payloads are ignored")
    void ignoresGarbage() {
        ReplicatedStore a = store("node-a", new ArrayList<>());
        assertFalse(a.applyRemote(PREFIX + "/" + NS + "/k", new byte[]{9, 9, 9}));
        assertFalse(a.applyRemote("elsewhere/k", new byte[0]));
        assertEquals(0, a.size(NS));
    }
}
