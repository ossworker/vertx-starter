package com.workoss.boot.vertx.contactcenter.core;

import io.vertx.core.json.JsonObject;

/**
 * Participant（VoIP-3 §3.4）：Identity 在某个 Conversation 中的一次出现，
 * kind 是实体类型（human/ai/system/external），role 是本次会话中的职能
 * （customer/agent/supervisor/observer）。
 */
public final class Participant {

    private final String participantId;
    private final String identityId;
    private final String kind;
    private volatile String role;
    private final String displayName;
    /** 外部渠道标识（sms/email/sip）；null 表示 IM/WebSocket 参与者。 */
    private final String channel;
    /** 渠道地址（手机号/E.164、邮箱、SIP URI）。 */
    private final String address;

    public Participant(String participantId, String identityId, String kind, String role, String displayName) {
        this(participantId, identityId, kind, role, displayName, null, null);
    }

    public Participant(String participantId, String identityId, String kind, String role, String displayName,
                       String channel, String address) {
        this.participantId = participantId;
        this.identityId = identityId;
        this.kind = kind;
        this.role = role;
        this.displayName = displayName;
        this.channel = channel;
        this.address = address;
    }

    public String participantId() {
        return participantId;
    }

    public String identityId() {
        return identityId;
    }

    public String kind() {
        return kind;
    }

    public String role() {
        return role;
    }

    public void role(String role) {
        this.role = role;
    }

    public String displayName() {
        return displayName;
    }

    public String channel() {
        return channel;
    }

    public String address() {
        return address;
    }

    /** 外部渠道参与者（短信/邮件/SIP），出站需经 ChannelConnector。 */
    public boolean external() {
        return channel != null;
    }

    public boolean canSpeak() {
        return "customer".equals(role) || "agent".equals(role) || "supervisor".equals(role);
    }

    public static Participant fromJson(JsonObject json) {
        return new Participant(json.getString("participant_id"), json.getString("identity_id"),
                json.getString("kind", "human"), json.getString("role", "observer"),
                json.getString("display_name", json.getString("identity_id")),
                json.getString("channel"), json.getString("address"));
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject()
                .put("participant_id", participantId)
                .put("identity_id", identityId)
                .put("kind", kind)
                .put("role", role)
                .put("display_name", displayName);
        if (channel != null) {
            json.put("channel", channel).put("address", address);
        }
        return json;
    }
}
