package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.List;

/** Durable message journal. Completion acknowledges database persistence, not provider delivery. */
public interface JournalRepository {

    /**
     * @return true for a new message, false only for a retry with the same cid and business payload;
     * conflicting reuse of payload.msg_id or any database failure fails the future
     */
    Future<Boolean> append(String cid, JsonObject envelope);

    /** Latest 1..200 messages, returned in ascending (created_at, msg_id) order. */
    Future<List<JsonObject>> history(String cid, int limit);

    default Future<List<TimelineEntry>> timeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
        return Future.failedFuture("message-timeline-store-unavailable");
    }
}
