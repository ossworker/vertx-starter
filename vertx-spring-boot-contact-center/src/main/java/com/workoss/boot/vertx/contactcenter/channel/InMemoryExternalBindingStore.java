package com.workoss.boot.vertx.contactcenter.channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 单节点默认实现。 */
public class InMemoryExternalBindingStore implements ExternalBindingStore {

    private final Map<String, String> bindings = new ConcurrentHashMap<>();

    @Override
    public String find(String channel, String address) {
        return bindings.get(channel + ":" + address);
    }

    @Override
    public void bind(String channel, String address, String cid) {
        bindings.put(channel + ":" + address, cid);
    }
}
