package com.workoss.boot.vertx.contactcenter.core;

import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Conversation（VoIP-3 §3.1 / UCTP §7.1）：跨渠道、跨时间的持久互动记录，
 * 容纳 Messages、Sessions 与 Participants。结束通话不关闭 Conversation。
 *
 * <p>闭包策略（UCTP §7.1）：{@code ephemeral}（最后 Session 结束且 N 秒无消息则关闭，
 * 默认 60s）或 {@code persistent}（仅显式关闭）。v1 实现 persistent 与显式/空闲关闭。
 */
public final class Conversation {

    private final String cid;
    private final String tenantId;
    private final String policy;
    private final long idleCloseSecs;
    private final JsonObject metadata;
    private Instant openedAt = Instant.now();
    private String ownerNodeId;
    private final Map<String, Participant> participants = new ConcurrentHashMap<>();
    private final Map<String, String> partIdByIdentity = new ConcurrentHashMap<>();
    private final Map<String, SessionState> sessions = new ConcurrentHashMap<>();
    /** 消息与事件日志（容量受限），供 message.history 与断线重连补发。 */
    private final Deque<JsonObject> journal = new ArrayDeque<>();

    private volatile boolean closed;
    private volatile Instant closedAt;
    private volatile long idleTimerId = -1;

    public Conversation(String cid, String tenantId, String policy, long idleCloseSecs, JsonObject metadata) {
        this.cid = cid;
        this.tenantId = tenantId;
        this.policy = policy == null ? "persistent" : policy;
        this.idleCloseSecs = idleCloseSecs;
        this.metadata = metadata == null ? new JsonObject() : metadata.copy();
    }

    public String cid() {
        return cid;
    }

    public String tenantId() {
        return tenantId;
    }

    public String policy() {
        return policy;
    }

    public long idleCloseSecs() {
        return idleCloseSecs;
    }

    public JsonObject metadata() {
        return metadata;
    }

    public Instant openedAt() {
        return openedAt;
    }

    public boolean closed() {
        return closed;
    }

    public Instant closedAt() {
        return closedAt;
    }

    public void markClosed() {
        this.closed = true;
        this.closedAt = Instant.now();
    }

    public Map<String, Participant> participants() {
        return participants;
    }

    public Map<String, SessionState> sessions() {
        return sessions;
    }

    public Participant participantByIdentity(String identityId) {
        String partId = partIdByIdentity.get(identityId);
        return partId == null ? null : participants.get(partId);
    }

    public Participant participant(String partId) {
        return participants.get(partId);
    }

    public void addParticipant(Participant participant) {
        participants.put(participant.participantId(), participant);
        partIdByIdentity.put(participant.identityId(), participant.participantId());
    }

    public void removeParticipant(String partId) {
        Participant removed = participants.remove(partId);
        if (removed != null) {
            partIdByIdentity.remove(removed.identityId(), removed.participantId());
        }
    }

    public Deque<JsonObject> journal() {
        return journal;
    }

    public void appendJournal(JsonObject envelope, int capacity) {
        synchronized (journal) {
            JsonObject copy = envelope.copy();
            copy.remove("_route");
            journal.addLast(copy);
            while (journal.size() > Math.max(1, capacity)) {
                journal.removeFirst();
            }
        }
    }

    public List<JsonObject> journalSnapshot() {
        synchronized (journal) {
            return List.copyOf(journal);
        }
    }

    public long idleTimerId() {
        return idleTimerId;
    }

    public void idleTimerId(long idleTimerId) {
        this.idleTimerId = idleTimerId;
    }

    public String ownerNodeId() {
        return ownerNodeId;
    }

    public void ownerNodeId(String ownerNodeId) {
        this.ownerNodeId = ownerNodeId;
        if (ownerNodeId != null) metadata.put("_owner_node_id", ownerNodeId);
    }

    /** Durable consultation state only: live media must be explicitly invited again after recovery. */
    public JsonObject describe() {
        io.vertx.core.json.JsonArray roster = new io.vertx.core.json.JsonArray();
        participants.values().stream().sorted(java.util.Comparator.comparing(Participant::participantId))
                .forEach(p -> roster.add(p.toJson()));
        return new JsonObject()
                .put("cid", cid)
                .put("tenant_id", tenantId)
                .put("policy", policy)
                .put("idle_close_secs", idleCloseSecs)
                .put("opened_at", openedAt.toEpochMilli())
                .put("closed", closed)
                .put("closed_at", closedAt == null ? null : closedAt.toEpochMilli())
                .put("metadata", metadata.copy())
                .put("participants", roster);
    }

    public static Conversation fromSnapshot(JsonObject snapshot) {
        Conversation conversation = new Conversation(snapshot.getString("cid"),
                snapshot.getString("tenant_id", "default"), snapshot.getString("policy", "persistent"),
                snapshot.getLong("idle_close_secs", 60L), snapshot.getJsonObject("metadata"));
        Object openedAt = snapshot.getValue("opened_at");
        conversation.openedAt = openedAt instanceof Number number ? Instant.ofEpochMilli(number.longValue())
                : Instant.parse((String) openedAt);
        conversation.closed = snapshot.getBoolean("closed", false);
        Object closedAt = snapshot.getValue("closed_at");
        conversation.closedAt = closedAt == null ? null : closedAt instanceof Number number
                ? Instant.ofEpochMilli(number.longValue()) : Instant.parse((String) closedAt);
        conversation.ownerNodeId = conversation.metadata.getString("_owner_node_id");
        for (Object value : snapshot.getJsonArray("participants", new io.vertx.core.json.JsonArray())) {
            conversation.addParticipant(Participant.fromJson((JsonObject) value));
        }
        return conversation;
    }

    /** Private working state for a command; discard on failed persistence. */
    Conversation copy() {
        Conversation copy = fromSnapshot(describe());
        copy.ownerNodeId = ownerNodeId;
        copy.idleTimerId = idleTimerId;
        sessions.forEach((sid, session) -> copy.sessions.put(sid, session.copy()));
        journalSnapshot().forEach(e -> copy.journal.addLast(e.copy()));
        return copy;
    }
}
