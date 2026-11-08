package com.workoss.boot.vertx.contactcenter.uctp;

import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.List;

/**
 * UCTP v0 信封（§3）工具类。信封本体就是 {@link JsonObject}，字段：
 * v / type / id / ts / cid / sid / connid / in_reply_to / payload。
 *
 * <p>出站投递时附带一个 {@link Outbound} 路由指令（会话组广播 / 按身份定向 / 按 WS 会话定向），
 * 序列化前会剥掉内部字段，线上只出现规范字段。
 */
public final class UctpEnvelope {

    public static final int PROTOCOL_VERSION = 1;

    /** 内部路由指令字段，出站序列化前移除。 */
    static final String ROUTE_FIELD = "_route";

    private final UctpIds ids;

    public UctpEnvelope(UctpIds ids) {
        this.ids = ids;
    }

    /** 解析并校验客户端信封；不合法时抛 {@link IllegalArgumentException}。 */
    public static JsonObject parse(String raw) {
        JsonObject env;
        try {
            env = new JsonObject(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("malformed-envelope");
        }
        require(env, "v");
        Object version = env.getValue("v");
        if (!(version instanceof Number number) || number.doubleValue() != number.intValue()) {
            throw new IllegalArgumentException("invalid-version");
        }
        env.put("v", number.intValue());
        for (String field : List.of("type", "id", "ts")) {
            require(env, field);
            validateString(env, field);
        }
        for (String field : List.of("cid", "sid", "connid", "in_reply_to")) {
            if (env.getValue(field) != null) validateString(env, field);
        }
        try {
            Instant.parse(env.getString("ts"));
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("invalid-timestamp");
        }
        if (!env.containsKey("payload")) env.put("payload", new JsonObject());
        if (!(env.getValue("payload") instanceof JsonObject)) {
            throw new IllegalArgumentException("invalid-payload");
        }
        env.remove(ROUTE_FIELD);
        return env;
    }

    private static void require(JsonObject env, String field) {
        if (!env.containsKey(field) || env.getValue(field) == null) {
            throw new IllegalArgumentException("missing-field:" + field);
        }
    }

    private static void validateString(JsonObject env, String field) {
        if (!(env.getValue(field) instanceof String value) || value.isBlank() || value.length() > 128) {
            throw new IllegalArgumentException("invalid-field:" + field);
        }
    }

    public static String type(JsonObject env) {
        return env.getString("type");
    }

    /** 构造服务端主动下发的信封。 */
    public JsonObject serverEnvelope(String type, String cid, String sid, String connid) {
        JsonObject env = new JsonObject()
                .put("v", PROTOCOL_VERSION)
                .put("type", type)
                .put("id", ids.prefixed("envid"))
                .put("ts", Instant.now().toString());
        if (cid != null) {
            env.put("cid", cid);
        }
        if (sid != null) {
            env.put("sid", sid);
        }
        if (connid != null) {
            env.put("connid", connid);
        }
        env.put("payload", new JsonObject());
        return env;
    }

    /** 以请求信封为模板构造响应（in_reply_to 关联）。 */
    public JsonObject replyTo(JsonObject request, String type) {
        JsonObject env = serverEnvelope(type, request.getString("cid"), request.getString("sid"),
                request.getString("connid"));
        env.put("in_reply_to", request.getString("id"));
        return env;
    }

    /** 构造 error 信封（§11）。 */
    public JsonObject error(JsonObject inReplyTo, int code, String category, String reason) {
        JsonObject env = inReplyTo == null
                ? serverEnvelope("error", null, null, null)
                : replyTo(inReplyTo, "error");
        env.put("payload", new JsonObject()
                .put("code", code)
                .put("category", category)
                .put("reason", reason));
        return env;
    }

    public JsonObject ack(JsonObject inReplyTo) {
        JsonObject env = replyTo(inReplyTo, "ack");
        env.put("payload", new JsonObject());
        return env;
    }

    public static Outbound toConversation(JsonObject envelope, String cid) {
        return new Outbound(envelope, Route.conversation(cid));
    }

    public static Outbound toIdentity(JsonObject envelope, String identityId) {
        return new Outbound(envelope, Route.identity(identityId));
    }

    public static Outbound toWsSession(JsonObject envelope, String wsSessionId) {
        return new Outbound(envelope, Route.wsSession(wsSessionId));
    }

    public static List<Outbound> of(Outbound... outbounds) {
        return List.of(outbounds);
    }

    /** 序列化为线上文本：剥掉内部路由字段。 */
    public static String wire(JsonObject envelope) {
        envelope.remove(ROUTE_FIELD);
        return envelope.encode();
    }

    /** 出站信封 + 投递目标。 */
    public record Outbound(JsonObject envelope, Route route) {
    }

    /** 投递目标。 */
    public sealed interface Route permits Route.ConversationGroup, Route.IdentityTarget, Route.WsSessionTarget {

        /** 会话组广播：所有加入该 Conversation 的本地 WS 会话。 */
        record ConversationGroup(String cid) implements Route {
        }

        /** 按身份定向：该 Identity 在本节点的全部 WS 会话。 */
        record IdentityTarget(String identityId) implements Route {
        }

        /** 按 WS 会话定向。 */
        record WsSessionTarget(String wsSessionId) implements Route {
        }

        static ConversationGroup conversation(String cid) {
            return new ConversationGroup(cid);
        }

        static IdentityTarget identity(String identityId) {
            return new IdentityTarget(identityId);
        }

        static WsSessionTarget wsSession(String wsSessionId) {
            return new WsSessionTarget(wsSessionId);
        }
    }
}
