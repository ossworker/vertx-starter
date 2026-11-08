package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SqlSessionRepository {
    private final Pool pool;
    private final boolean postgres;
    private final SqlDialect dialect;

    SqlSessionRepository(Pool pool, boolean postgres) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.postgres = postgres;
        this.dialect = new SqlDialect(postgres);
    }

    Future<Void> initialize() {
        String ddl = "CREATE TABLE IF NOT EXISTS uctp_sessions ("
                + "cid " + dialect.idType(64) + " NOT NULL, sid " + dialect.idType(64) + " NOT NULL, "
                + "snapshot " + dialect.textType() + " NOT NULL, invited_at BIGINT NOT NULL, ended_at BIGINT, "
                + "PRIMARY KEY (cid, sid)"
                + (postgres ? ")" : ", KEY idx_uctp_sessions_cid (cid, invited_at, sid))");
        return pool.query(ddl).execute().compose(ignored -> postgres
                ? pool.query("CREATE INDEX IF NOT EXISTS idx_uctp_sessions_cid "
                        + "ON uctp_sessions (cid, invited_at, sid)").execute().mapEmpty()
                : Future.succeededFuture());
    }

    JsonObject validate(JsonObject snapshot) {
        JsonObject captured = Objects.requireNonNull(snapshot, "session snapshot").copy();
        SqlDialect.requireId(captured.getString("cid"), "cid", 64);
        SqlDialect.requireId(captured.getString("sid"), "sid", 64);
        Objects.requireNonNull(captured.getLong("invited_at"), "invited_at");
        return captured;
    }

    Future<Void> save(SqlConnection connection, JsonObject snapshot) {
        try {
            JsonObject captured = validate(snapshot);
            String upsert = "INSERT INTO uctp_sessions (cid, sid, snapshot, invited_at, ended_at) VALUES (?, ?, ?, ?, ?) "
                    + (postgres ? "ON CONFLICT (cid, sid) DO UPDATE SET snapshot=EXCLUDED.snapshot, "
                            + "invited_at=EXCLUDED.invited_at, ended_at=EXCLUDED.ended_at"
                    : "ON DUPLICATE KEY UPDATE snapshot=VALUES(snapshot), invited_at=VALUES(invited_at), ended_at=VALUES(ended_at)");
            Tuple parameters = Tuple.of(captured.getString("cid"), captured.getString("sid"), captured.encode(),
                    captured.getLong("invited_at"), captured.getLong("ended_at"));
            return connection.preparedQuery(dialect.parameters(upsert)).execute(parameters).mapEmpty();
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }

    Future<List<TimelineEntry>> timeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
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
                where += " AND (invited_at < ? OR (invited_at = ? AND sid < ?))";
                parameters.addLong(beforeAt).addLong(beforeAt).addString(beforeId);
            }
            parameters.addInteger(limit);
            String sql = "SELECT invited_at, sid, snapshot FROM (SELECT invited_at, sid, snapshot FROM uctp_sessions "
                    + where + " ORDER BY invited_at DESC, sid DESC LIMIT ?) AS latest "
                    + "ORDER BY invited_at ASC, sid ASC";
            return pool.preparedQuery(dialect.parameters(sql)).execute(parameters).map(rows -> {
                List<TimelineEntry> history = new ArrayList<>(rows.size());
                for (Row row : rows) {
                    history.add(new TimelineEntry("session", row.getLong("invited_at"), row.getString("sid"),
                            new JsonObject(row.getString("snapshot"))));
                }
                return history;
            });
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }

    Future<List<JsonObject>> history(String cid, int limit) {
        try {
            SqlDialect.requireId(cid, "cid", 64);
            SqlDialect.requireLimit(limit);
            String sql = "SELECT snapshot FROM (SELECT snapshot, invited_at, sid FROM uctp_sessions "
                    + "WHERE cid=? ORDER BY invited_at DESC, sid DESC LIMIT ?) AS latest "
                    + "ORDER BY invited_at ASC, sid ASC";
            return pool.preparedQuery(dialect.parameters(sql)).execute(Tuple.of(cid, limit)).map(rows -> {
                List<JsonObject> history = new ArrayList<>(rows.size());
                for (Row row : rows) {
                    history.add(new JsonObject(row.getString("snapshot")));
                }
                return history;
            });
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }
}
