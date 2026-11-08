package com.workoss.boot.vertx.contactcenter.uctp;

import io.vertx.core.json.JsonObject;

/**
 * 已认证的 (Identity, Device) 主体（UCTP §5.7）。participant 身份由 Conversation 内的
 * Participant 记录承担；信封 payload 里的 from 只是展示字段，权限一律以本主体为准。
 *
 * @param identityId   稳定身份 id（id_ 前缀）
 * @param displayName  展示名
 * @param kind         human / ai / system / external
 * @param defaultRole  进入会话时的默认角色（customer / agent / supervisor / observer）
 * @param capabilities auth.hello 上报的 CapabilityDescriptor
 * @param assurance    IdentityAssurance 梯度（v1 开发级固定 identified）
 * @param sessionToken 服务端签发的会话令牌（断线重连凭证）
 */
public record AuthPrincipal(String identityId, String displayName, String kind, String defaultRole,
                            JsonObject capabilities, String assurance, String sessionToken) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("identity_id", identityId)
                .put("display_name", displayName)
                .put("kind", kind)
                .put("default_role", defaultRole)
                .put("capabilities", capabilities == null ? new JsonObject() : capabilities.copy())
                .put("assurance", assurance)
                .put("session_token", sessionToken);
    }

    public static AuthPrincipal fromJson(JsonObject json) {
        return new AuthPrincipal(
                json.getString("identity_id"),
                json.getString("display_name"),
                json.getString("kind"),
                json.getString("default_role"),
                json.getJsonObject("capabilities"),
                json.getString("assurance"),
                json.getString("session_token"));
    }
}
