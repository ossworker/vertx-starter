package com.workoss.boot.vertx.contactcenter.core;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session（VoIP-3 §3.2 / UCTP §7.2）：Conversation 内一次同步、有界的实时互动（通话/视频/聊天）。
 *
 * <p>状态机：{@code INVITING}（邀请中）→ {@code ACTIVE}（≥1 受邀者接受且每方均有双方确认
 * connected 的 READY Connection）→ {@code ENDED}。SDP answer 仅表示协商完成。
 * 最后一个 Connection 结束后进入宽限期（默认 30s），期内
 * 重连复用本 Session（UCTP §7.3），超时广播 {@code session.ended}。
 */
public final class SessionState {

    public enum Status {INVITING, ACTIVE, ENDED}

    /** Connection 协商状态（UCTP §7.4）。 */
    public enum ConnectionStatus {OFFERED, ANSWERED, READY, ENDED}

    /** 一条 Connection：某 Participant 在一次 Session 中的一条传输级附着（leg）。 */
    public static final class ConnectionState {
        public final String connId;
        public final String participantId;
        public final String substrate;
        /** 指定的 SDP 对端；P2P 一条 Connection 同时覆盖两个已接受的参与者。 */
        public volatile String answererPartId;
        public volatile ConnectionStatus status = ConnectionStatus.OFFERED;
        public final Set<String> connectedParticipants = ConcurrentHashMap.newKeySet();
        public final Map<String, io.vertx.core.json.JsonObject> mediaByParticipant = new ConcurrentHashMap<>();

        ConnectionState copy() {
            ConnectionState copy = new ConnectionState(connId, participantId, substrate);
            copy.answererPartId = answererPartId;
            copy.status = status;
            copy.connectedParticipants.addAll(connectedParticipants);
            mediaByParticipant.forEach((pid, state) -> copy.mediaByParticipant.put(pid, state.copy()));
            return copy;
        }

        public ConnectionState(String connId, String participantId, String substrate) {
            this.connId = connId;
            this.participantId = participantId;
            this.substrate = substrate;
        }

        public boolean covers(String partId) {
            return participantId.equals(partId) || partId.equals(answererPartId);
        }
    }

    private final String sid;
    private final String cid;
    private final String medium;
    private final String initiatorPartId;
    private Instant startedAt = Instant.now();
    private final Set<String> pendingInvitees = ConcurrentHashMap.newKeySet();
    private final Set<String> acceptedParticipants = ConcurrentHashMap.newKeySet();
    private final Map<String, ConnectionState> connections = new ConcurrentHashMap<>();
    private final Map<String, String> inviteEnvelopeByPartId = new ConcurrentHashMap<>();

    private volatile Status status = Status.INVITING;
    private volatile Instant acceptedAt;
    private volatile Instant activatedAt;
    private volatile Instant endedAt;
    private volatile String endReason;
    private volatile long graceTimerId = -1;

    public SessionState(String sid, String cid, String medium, String initiatorPartId) {
        this.sid = sid;
        this.cid = cid;
        this.medium = medium;
        this.initiatorPartId = initiatorPartId;
    }

    public String sid() {
        return sid;
    }

    public String cid() {
        return cid;
    }

    public String medium() {
        return medium;
    }

    public String initiatorPartId() {
        return initiatorPartId;
    }

    public Status status() {
        return status;
    }

    public void status(Status status) {
        this.status = status;
        if (status == Status.ACTIVE) {
            this.activatedAt = Instant.now();
        }
    }

    void accept(Instant at) {
        if (acceptedAt == null) acceptedAt = at;
    }

    void end(Instant at, String reason) {
        status = Status.ENDED;
        endedAt = at;
        endReason = reason;
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Instant acceptedAt() {
        return acceptedAt;
    }

    public Instant activatedAt() {
        return activatedAt;
    }

    public Instant endedAt() {
        return endedAt;
    }

    public String endReason() {
        return endReason;
    }

    public Set<String> pendingInvitees() {
        return pendingInvitees;
    }

    public Set<String> acceptedParticipants() {
        return acceptedParticipants;
    }

    public Map<String, ConnectionState> connections() {
        return connections;
    }

    public Map<String, String> inviteEnvelopeByPartId() {
        return inviteEnvelopeByPartId;
    }

    public long graceTimerId() {
        return graceTimerId;
    }

    public void graceTimerId(long graceTimerId) {
        this.graceTimerId = graceTimerId;
    }

    public boolean hasLiveConnection() {
        return connections.values().stream().anyMatch(c -> c.status != ConnectionStatus.ENDED);
    }

    /** SDP negotiation alone is not media readiness: every accepted leg needs two-sided confirmation. */
    public boolean negotiationSatisfied() {
        return acceptedParticipants.size() >= 2 && acceptedParticipants.stream().allMatch(p ->
                connections.values().stream().anyMatch(c -> c.status == ConnectionStatus.READY && c.covers(p)));
    }

    public JsonObject historySnapshot() {
        Set<String> participants = new TreeSet<>(acceptedParticipants);
        participants.addAll(pendingInvitees);
        participants.addAll(inviteEnvelopeByPartId.keySet());
        JsonObject snapshot = new JsonObject().put("cid", cid).put("sid", sid).put("medium", medium)
                .put("status", status.name()).put("initiator_participant_id", initiatorPartId)
                .put("participant_ids", new JsonArray(participants.stream().toList()))
                .put("accepted_participant_ids", new JsonArray(acceptedParticipants.stream().sorted().toList()))
                .put("invited_at", startedAt.toEpochMilli());
        if (acceptedAt != null) snapshot.put("accepted_at", acceptedAt.toEpochMilli());
        if (activatedAt != null) snapshot.put("started_at", activatedAt.toEpochMilli());
        if (endedAt != null) snapshot.put("ended_at", endedAt.toEpochMilli()).put("end_reason", endReason);
        if (activatedAt != null && endedAt != null) snapshot.put("duration_ms", endedAt.toEpochMilli() - activatedAt.toEpochMilli());
        return snapshot;
    }

    SessionState copy() {
        SessionState copy = new SessionState(sid, cid, medium, initiatorPartId);
        copy.status = status;
        copy.startedAt = startedAt;
        copy.acceptedAt = acceptedAt;
        copy.activatedAt = activatedAt;
        copy.endedAt = endedAt;
        copy.endReason = endReason;
        copy.graceTimerId = graceTimerId;
        copy.pendingInvitees.addAll(pendingInvitees);
        copy.acceptedParticipants.addAll(acceptedParticipants);
        copy.inviteEnvelopeByPartId.putAll(inviteEnvelopeByPartId);
        connections.forEach((id, connection) -> copy.connections.put(id, connection.copy()));
        return copy;
    }
}
