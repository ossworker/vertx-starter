package com.workoss.boot.vertx.contactcenter.uctp;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UctpEnvelopeTest {
    @Test
    void parsesObjectsDefaultsPayloadAndRemovesInternalRoute() {
        JsonObject input = envelope().put("_route", new JsonObject().put("identity", "untrusted"));
        JsonObject parsed = UctpEnvelope.parse(input.encode());
        assertEquals(1, parsed.getInteger("v"));
        assertNotNull(parsed.getJsonObject("payload"));
        assertFalse(parsed.containsKey("_route"));
        assertEquals(new JsonObject().put("text", "咨询"), UctpEnvelope.parse(
                envelope().put("payload", new JsonObject().put("text", "咨询")).encode()).getJsonObject("payload"));
    }

    @Test
    void rejectsInvalidTypesAndMalformedJson() {
        for (Object invalid : List.of("1", 1.5, true, 2147483648L)) {
            assertThrows(IllegalArgumentException.class, () -> UctpEnvelope.parse(envelope().put("v", invalid).encode()));
        }
        for (String field : List.of("type", "id", "ts", "cid", "sid", "connid", "in_reply_to")) {
            for (Object invalid : List.of(123, "", " ", "x".repeat(129), new JsonArray())) {
                assertThrows(IllegalArgumentException.class,
                        () -> UctpEnvelope.parse(envelope().put(field, invalid).encode()), field);
            }
        }
        for (Object invalid : List.of("text", 1, new JsonArray())) {
            assertThrows(IllegalArgumentException.class,
                    () -> UctpEnvelope.parse(envelope().put("payload", invalid).encode()));
        }
        assertThrows(IllegalArgumentException.class, () -> UctpEnvelope.parse(envelope().putNull("payload").encode()));
        assertThrows(IllegalArgumentException.class, () -> UctpEnvelope.parse(envelope().put("ts", "not-a-date").encode()));
        assertThrows(IllegalArgumentException.class, () -> UctpEnvelope.parse("{broken"));
        assertThrows(IllegalArgumentException.class, () -> UctpEnvelope.parse("[]"));
        assertThrows(IllegalArgumentException.class, () -> UctpEnvelope.parse("{}"));
    }

    private static JsonObject envelope() {
        return new JsonObject().put("v", 1).put("type", "auth.hello").put("id", "envid1")
                .put("ts", "2026-11-07T00:00:00Z");
    }
}
