package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.json.JsonObject;

public record TimelineEntry(String kind, long occurredAt, String sourceId, JsonObject value) {
}
