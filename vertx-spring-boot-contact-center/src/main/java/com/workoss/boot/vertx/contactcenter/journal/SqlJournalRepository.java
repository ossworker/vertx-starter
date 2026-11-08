package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Parameterized, asynchronous journal using the existing uctp_journal schema. */
public final class SqlJournalRepository implements JournalRepository {
    private final Pool pool;
    private final boolean postgres;
    private final SqlDialect dialect;

    public SqlJournalRepository(Pool pool, boolean postgres) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.postgres = postgres;
        this.dialect = new SqlDialect(postgres);
    }

    public Future<Void> initialize() {
        String ddl = "CREATE TABLE IF NOT EXISTS uctp_journal ("
                + "msg_id " + dialect.idType(64) + " PRIMARY KEY, "
                + "cid " + dialect.idType(64) + " NOT NULL, "
                + "msg_type VARCHAR(32) NOT NULL, sender VARCHAR(64), content_type VARCHAR(128), "
                + "body " + dialect.textType() + ", envelope " + dialect.textType() + " NOT NULL, "
                + "created_at BIGINT NOT NULL"
                + (postgres ? ")" : ", KEY idx_uctp_journal_cid (cid, created_at, msg_id))");
        return pool.query(ddl).execute().compose(ignored -> postgres
                ? pool.query("CREATE INDEX IF NOT EXISTS idx_uctp_journal_cid "
                        + "ON uctp_journal (cid, created_at, msg_id)").execute().mapEmpty()
                : Future.succeededFuture());
    }

    @Override
    public Future<Boolean> append(String cid, JsonObject envelope) {
        try {
            SqlDialect.requireId(cid, "cid", 64);
            // Capture before starting IO so a caller mutation cannot change the collision check.
            JsonObject captured = Objects.requireNonNull(envelope, "envelope").copy();
            JsonObject payload = Objects.requireNonNull(captured.getJsonObject("payload"), "payload");
            String msgId = SqlDialect.requireId(payload.getString("msg_id"), "msg_id", 64);
            String type = captured.getString("type", "message.send");
            Object body = payload.getValue("body");
            Tuple parameters = Tuple.of(msgId, cid, type, payload.getString("from"),
                    payload.getString("content_type"), body == null || body instanceof String ? body : Json.encode(body),
                    captured.encode(), System.currentTimeMillis());
            // Autocommitted INSERT: a PostgreSQL duplicate must not abort a surrounding transaction.
            return pool.preparedQuery(dialect.parameters("INSERT INTO uctp_journal "
                            + "(msg_id, cid, msg_type, sender, content_type, body, envelope, created_at) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"))
                    .execute(parameters)
                    .compose(rows -> rows.rowCount() == 1 ? Future.succeededFuture(true)
                            : Future.failedFuture(new IllegalStateException("Journal insert affected no row")))
                    .recover(failure -> dialect.isDuplicate(failure)
                            ? verifyRetry(msgId, cid, type, payload) : Future.failedFuture(failure));
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }

    private Future<Boolean> verifyRetry(String msgId, String cid, String type, JsonObject payload) {
        return pool.preparedQuery(dialect.parameters(
                        "SELECT cid, msg_type, envelope FROM uctp_journal WHERE msg_id=?"))
                .execute(Tuple.of(msgId)).compose(rows -> {
                    if (rows.size() == 1) {
                        Row row = rows.iterator().next();
                        JsonObject persisted = new JsonObject(row.getString("envelope"));
                        // Transport envelope id/timestamp may change on retry; the business payload may not.
                        if (cid.equals(row.getString("cid")) && type.equals(row.getString("msg_type"))
                                && payload.equals(persisted.getJsonObject("payload"))) {
                            return Future.succeededFuture(false);
                        }
                    }
                    return Future.failedFuture(new IllegalStateException("Conflicting journal msg_id: " + msgId));
                });
    }

    @Override
    public Future<List<TimelineEntry>> timeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
        try {
            SqlDialect.requireId(cid, "cid", 64);
            SqlDialect.requireTimelineLimit(limit);
            if ((beforeAt == null) != (beforeId == null) || (beforeAt == null) != (beforeKind == null))
                throw new IllegalArgumentException("incomplete timeline cursor");
            String where = "WHERE cid=?";
            Tuple parameters = Tuple.of(cid);
            if (beforeAt != null) {
                if (beforeAt < 0) throw new IllegalArgumentException("invalid timeline timestamp");
                SqlDialect.requireId(beforeId, "beforeId", 64);
                SqlDialect.requireTimelineKind(beforeKind);
                where += " AND (created_at < ? OR (created_at = ? AND (msg_id < ? OR (msg_id = ? AND 'message' < ?))))";
                parameters.addLong(beforeAt).addLong(beforeAt).addString(beforeId).addString(beforeId).addString(beforeKind);
            }
            parameters.addInteger(limit);
            String sql = "SELECT created_at, msg_id, envelope FROM (SELECT created_at, msg_id, envelope FROM uctp_journal "
                    + where + " ORDER BY created_at DESC, msg_id DESC LIMIT ?) AS latest "
                    + "ORDER BY created_at ASC, msg_id ASC";
            return pool.preparedQuery(dialect.parameters(sql)).execute(parameters).map(rows -> {
                List<TimelineEntry> history = new ArrayList<>(rows.size());
                for (Row row : rows) {
                    history.add(new TimelineEntry("message", row.getLong("created_at"), row.getString("msg_id"),
                            new JsonObject(row.getString("envelope"))));
                }
                return history;
            });
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }

    @Override
    public Future<List<JsonObject>> history(String cid, int limit) {
        try {
            SqlDialect.requireId(cid, "cid", 64);
            SqlDialect.requireLimit(limit);
            // Select the latest slice before reversing it; timestamp ties use the immutable primary key.
            String sql = "SELECT envelope FROM (SELECT envelope, created_at, msg_id FROM uctp_journal "
                    + "WHERE cid=? ORDER BY created_at DESC, msg_id DESC LIMIT ?) AS latest "
                    + "ORDER BY created_at ASC, msg_id ASC";
            return pool.preparedQuery(dialect.parameters(sql)).execute(Tuple.of(cid, limit)).map(rows -> {
                List<JsonObject> history = new ArrayList<>(rows.size());
                for (Row row : rows) {
                    history.add(new JsonObject(row.getString("envelope")));
                }
                return history;
            });
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }
}
