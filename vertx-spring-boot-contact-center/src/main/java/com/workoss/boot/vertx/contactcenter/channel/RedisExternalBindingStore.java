package com.workoss.boot.vertx.contactcenter.channel;

import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis 实现：集群部署下任一接入节点收到渠道回调都能命中同一绑定。
 * key 结构：hash "uctp:external-bindings"，field "channel:address"，value cid。
 */
public class RedisExternalBindingStore implements ExternalBindingStore {

    private static final String KEY = "uctp:external-bindings";

    private final StringRedisTemplate redis;

    public RedisExternalBindingStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public String find(String channel, String address) {
        Object value = redis.opsForHash().get(KEY, channel + ":" + address);
        return value == null ? null : value.toString();
    }

    @Override
    public void bind(String channel, String address, String cid) {
        redis.opsForHash().put(KEY, channel + ":" + address, cid);
    }
}
