package com.workoss.boot.vertx.contactcenter.channel;

/**
 * 外部渠道地址绑定存储：channel:address → cid。
 * <p>进程内实现适用于单节点；集群部署用 Redis 实现（任一接入节点收到渠道回调都能命中会话）。
 */
public interface ExternalBindingStore {

    String find(String channel, String address);

    void bind(String channel, String address, String cid);
}
