package com.workoss.boot.vertx.ws;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.ClusterSerializable;

import java.nio.charset.StandardCharsets;

/**
 * 聊天室中的用户信息。
 * <p>
 * 通过 WebSocket 连接时由 {@code ?name=}&amp;{@code role=} 查询参数构造，并在连接期间写入集群共享
 * {@code AsyncMap}（键为用户 id），因此在线名单在 A/B/C 任意节点都一致。
 * <p>
 * 因为集群 AsyncMap 底层走 Zenoh 集群管理器，其值必须可跨节点编解码，故实现
 * {@link ClusterSerializable}（与 {@code ClusterWebSocketBroadcaster.ClusterBroadcast} 同一套机制）。
 * 既是 WebSocket 消息里的“发送者身份”附件，也可被 Jackson 序列化后下发到前端。
 *
 * @see ChatWebSocketHandler
 */
public class UserInfo implements ClusterSerializable, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private String role;
    private long loginTime;
    private String nodeId;
    /** 所属聊天房间；同一房间内的多人跨节点共享消息，不同房间互相隔离。 */
    private String room;

    /** 供 {@link ClusterSerializable} 在远端节点重建实例时调用。 */
    public UserInfo() {
    }

    public UserInfo(String id, String name, String role, long loginTime, String nodeId, String room) {
        this.id = id;
        this.name = name;
        this.role = role;
        this.loginTime = loginTime;
        this.nodeId = nodeId;
        this.room = room;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public long getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(long loginTime) {
        this.loginTime = loginTime;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getRoom() {
        return room;
    }

    public void setRoom(String room) {
        this.room = room;
    }

    @Override
    public void writeToBuffer(Buffer buffer) {
        writeString(buffer, id);
        writeString(buffer, name);
        writeString(buffer, role);
        buffer.appendLong(loginTime);
        writeString(buffer, nodeId);
        writeString(buffer, room);
    }

    @Override
    public int readFromBuffer(int pos, Buffer buffer) {
        int p = pos;
        this.id = readString(buffer, p);
        p += 4 + byteLen(id);
        this.name = readString(buffer, p);
        p += 4 + byteLen(name);
        this.role = readString(buffer, p);
        p += 4 + byteLen(role);
        this.loginTime = buffer.getLong(p);
        p += 8;
        this.nodeId = readString(buffer, p);
        p += 4 + byteLen(nodeId);
        this.room = readString(buffer, p);
        p += 4 + byteLen(room);
        return p;
    }

    private static void writeString(Buffer buffer, String s) {
        byte[] bytes = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
        buffer.appendInt(bytes.length).appendBytes(bytes);
    }

    private static String readString(Buffer buffer, int pos) {
        int len = buffer.getInt(pos);
        if (len == 0) {
            return "";
        }
        return buffer.getString(pos + 4, pos + 4 + len);
    }

    private static int byteLen(String s) {
        return (s == null ? 0 : s.getBytes(StandardCharsets.UTF_8).length);
    }

    @Override
    public String toString() {
        return "UserInfo{id='" + id + "', name='" + name + "', role='" + role + "', nodeId='" + nodeId + "'}";
    }
}
