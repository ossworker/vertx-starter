package com.workoss.boot.vertx.data.dict.cache.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Spring {@link Cache} implementation backed by Zenoh via zenoh-jni.
 * <p>
 * Stores cache entries in Zenoh, which delegates to a Zenoh Router
 * with RocksDB storage for persistence.
 */
public class ZenohCache implements Cache {

    private static final Logger logger = LoggerFactory.getLogger(ZenohCache.class);

    private final String name;
    private final java.util.function.Supplier<Session> sessionSupplier;
    private final String keyPrefix;
    private final ObjectMapper objectMapper;

    public ZenohCache(String name, java.util.function.Supplier<Session> sessionSupplier, String keyPrefix, ObjectMapper objectMapper) {
        this.name = name;
        this.sessionSupplier = sessionSupplier;
        this.keyPrefix = keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return sessionSupplier.get();
    }

    @Override
    public ValueWrapper get(Object key) {
        try {
            Session session = sessionSupplier.get();
            if (session == null) return null;
            String zenohKey = buildKey(key);
            List<Reply> replies = session.get(zenohKey);
            Object value = parseReply(replies);

            if (value != null) {
                logger.debug("Zenoh cache hit for key={}", key);
                return () -> value;
            }
        } catch (Exception e) {
            logger.warn("Zenoh cache get failed for key={}: {}", key, e.getMessage());
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Class<T> type) {
        ValueWrapper wrapper = get(key);
        if (wrapper != null) {
            Object value = wrapper.get();
            if (value != null && type.isInstance(value)) {
                return (T) value;
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        ValueWrapper wrapper = get(key);
        if (wrapper != null) {
            return (T) wrapper.get();
        }
        try {
            T value = valueLoader.call();
            put(key, value);
            return value;
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    @Override
    public void put(Object key, Object value) {
        try {
            Session session = sessionSupplier.get();
            if (session == null) return;
            String zenohKey = buildKey(key);
            String json = objectMapper.writeValueAsString(new CacheValue(value));
            session.put(zenohKey, json.getBytes(StandardCharsets.UTF_8));
            logger.debug("Zenoh cache put for key={}", key);
        } catch (Exception e) {
            logger.warn("Zenoh cache put failed for key={}: {}", key, e.getMessage());
        }
    }

    @Override
    public void evict(Object key) {
        try {
            Session session = sessionSupplier.get();
            if (session == null) return;
            String zenohKey = buildKey(key);
            session.delete(zenohKey);
            logger.debug("Zenoh cache evict for key={}", key);
        } catch (Exception e) {
            logger.warn("Zenoh cache evict failed for key={}: {}", key, e.getMessage());
        }
    }

    @Override
    public void clear() {
        logger.info("Zenoh cache clear not supported");
    }

    private String buildKey(Object key) {
        return keyPrefix + name + "/" + key.toString().replaceAll("[^a-zA-Z0-9.-]", "_");
    }

    private Object parseReply(List<Reply> replies) {
        try {
            if (replies == null || replies.isEmpty()) return null;
            Reply reply = replies.get(0);
            if (!reply.success() || reply.samplePayload().isEmpty()) return null;
            byte[] data = reply.samplePayload().get();
            CacheValue cacheValue = objectMapper.readValue(data, CacheValue.class);
            return cacheValue.getValue();
        } catch (Exception e) {
            logger.debug("Failed to parse Zenoh reply: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Wrapper for cache values to support serialization of any object.
     */
    private static class CacheValue {
        private String type;
        private String json;

        public CacheValue() {}

        public CacheValue(Object value) throws JsonProcessingException {
            if (value != null) {
                this.type = value.getClass().getName();
                this.json = new ObjectMapper().writeValueAsString(value);
            }
        }

        public Object getValue() {
            if (type == null || json == null) return null;
            try {
                Class<?> clazz = Class.forName(type);
                return new ObjectMapper().readValue(json, clazz);
            } catch (Exception e) {
                return json;
            }
        }
    }
}
