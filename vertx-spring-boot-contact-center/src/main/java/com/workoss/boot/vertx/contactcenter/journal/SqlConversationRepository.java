package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Snapshot text plus a relational membership index; deliberately uses no database JSON functions. */
public final class SqlConversationRepository {
    private final Pool pool;
    private final boolean postgres;
    private final SqlDialect dialect;
    private final SqlSessionRepository sessions;

    public SqlConversationRepository(Pool pool, boolean postgres) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.postgres = postgres;
        this.dialect = new SqlDialect(postgres);
        this.sessions = new SqlSessionRepository(pool, postgres);
    }

    public Future<Void> initialize() {
        String conversations = "CREATE TABLE IF NOT EXISTS uctp_conversations ("
                + "cid " + dialect.idType(64) + " PRIMARY KEY, snapshot " + dialect.textType()
                + " NOT NULL, opened_at BIGINT NOT NULL)";
        String members = "CREATE TABLE IF NOT EXISTS uctp_conversation_members ("
                + "cid " + dialect.idType(64) + " NOT NULL, identity_id " + dialect.idType(128)
                + " NOT NULL, PRIMARY KEY (cid, identity_id)"
                + (postgres ? ")" : ", KEY idx_uctp_members_identity (identity_id, cid))");
        return pool.query(conversations).execute()
                .compose(ignored -> pool.query(members).execute())
                .compose(ignored -> postgres
                        ? pool.query("CREATE INDEX IF NOT EXISTS idx_uctp_members_identity "
                                + "ON uctp_conversation_members (identity_id, cid)").execute().mapEmpty()
                        : Future.succeededFuture())
                .compose(ignored -> sessions.initialize());
    }

    public Future<Void> save(JsonObject snapshot) {
        return save(snapshot, List.of());
    }

    public Future<Void> save(JsonObject snapshot, List<JsonObject> sessionHistory) {
        try {
            JsonObject captured = Objects.requireNonNull(snapshot, "snapshot").copy();
            String cid = SqlDialect.requireId(captured.getString("cid"), "cid", 64);
            List<JsonObject> capturedSessions = Objects.requireNonNull(sessionHistory, "sessionHistory").stream()
                    .map(sessions::validate).map(session -> {
                        if (!cid.equals(session.getString("cid"))) throw new IllegalArgumentException("session cid must match snapshot cid");
                        return session;
                    }).toList();
            Long openedAt = Objects.requireNonNull(captured.getLong("opened_at"), "opened_at");
            JsonArray participants = Objects.requireNonNull(captured.getJsonArray("participants"), "participants");
            Set<String> identities = new LinkedHashSet<>();
            for (int index = 0; index < participants.size(); index++) {
                JsonObject participant = Objects.requireNonNull(participants.getJsonObject(index), "participant");
                identities.add(SqlDialect.requireId(participant.getString("identity_id"), "identity_id", 128));
            }
            String upsert = "INSERT INTO uctp_conversations (cid, snapshot, opened_at) VALUES (?, ?, ?) "
                    + (postgres ? "ON CONFLICT (cid) DO UPDATE SET snapshot=EXCLUDED.snapshot, opened_at=EXCLUDED.opened_at"
                    : "ON DUPLICATE KEY UPDATE snapshot=VALUES(snapshot), opened_at=VALUES(opened_at)");
            // Upsert holds the cid row lock until commit, serializing member-list replacements for the same cid.
            return pool.withTransaction(connection -> connection.preparedQuery(dialect.parameters(upsert))
                    .execute(Tuple.of(cid, captured.encode(), openedAt))
                    .compose(ignored -> connection.preparedQuery(dialect.parameters(
                            "DELETE FROM uctp_conversation_members WHERE cid=?")).execute(Tuple.of(cid)))
                    .compose(ignored -> {
                        if (identities.isEmpty()) {
                            return Future.<Void>succeededFuture();
                        }
                        List<Tuple> members = identities.stream().map(identity -> Tuple.of(cid, identity)).toList();
                        return connection.preparedQuery(dialect.parameters(
                                        "INSERT INTO uctp_conversation_members (cid, identity_id) VALUES (?, ?)"))
                                .executeBatch(members).mapEmpty();
                    })
                    .compose(ignored -> {
                        Future<Void> writes = Future.succeededFuture();
                        for (JsonObject session : capturedSessions) {
                            writes = writes.compose(v -> sessions.save(connection, session));
                        }
                        return writes;
                    }));
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }

    /** A missing conversation is represented by a successful null result; IO failures are not misses. */
    public Future<JsonObject> load(String cid) {
        try {
            SqlDialect.requireId(cid, "cid", 64);
            return pool.preparedQuery(dialect.parameters("SELECT snapshot FROM uctp_conversations WHERE cid=?"))
                    .execute(Tuple.of(cid)).map(rows -> rows.size() == 0 ? null
                            : new JsonObject(rows.iterator().next().getString("snapshot")));
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }

    public Future<List<JsonObject>> sessionHistory(String cid, int limit) {
        return sessions.history(cid, limit);
    }

    public Future<List<TimelineEntry>> sessionTimeline(String cid, Long beforeAt, String beforeId, String beforeKind, int limit) {
        return sessions.timeline(cid, beforeAt, beforeId, beforeKind, limit);
    }

    /** Includes closed conversations so the core, not the SQL layer, decides resumability. */
    public Future<List<JsonObject>> findByIdentity(String identityId, int limit) {
        try {
            SqlDialect.requireId(identityId, "identity_id", 128);
            SqlDialect.requireLimit(limit);
            String sql = "SELECT c.snapshot FROM uctp_conversations c "
                    + "JOIN uctp_conversation_members m ON m.cid=c.cid WHERE m.identity_id=? "
                    + "ORDER BY c.opened_at DESC, c.cid DESC LIMIT ?";
            return pool.preparedQuery(dialect.parameters(sql)).execute(Tuple.of(identityId, limit)).map(rows -> {
                List<JsonObject> snapshots = new ArrayList<>(rows.size());
                for (Row row : rows) {
                    snapshots.add(new JsonObject(row.getString("snapshot")));
                }
                return snapshots;
            });
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
    }
}
