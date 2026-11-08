package com.workoss.boot.vertx.contactcenter.channel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 渠道连接器注册表：Spring 容器内所有 {@link ChannelConnector} Bean 自动注册。
 */
public class ChannelRegistry {

    private final Map<String, ChannelConnector> connectors = new ConcurrentHashMap<>();

    public ChannelRegistry(List<ChannelConnector> connectorList) {
        connectorList.forEach(c -> connectors.put(c.channel(), c));
    }

    public Optional<ChannelConnector> get(String channel) {
        return Optional.ofNullable(connectors.get(channel));
    }

    public boolean hasChannel(String channel) {
        return connectors.containsKey(channel);
    }
}
