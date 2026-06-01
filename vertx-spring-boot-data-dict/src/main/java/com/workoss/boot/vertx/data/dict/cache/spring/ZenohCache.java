package com.workoss.boot.vertx.data.dict.cache.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zenoh.Session;
import io.zenoh.bytes.ZBytes;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.query.Reply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Spring {@link Cache} implementation backed by Zenoh.
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
            KeyExpr keyExpr = KeyExpr.tryFrom(zenohKey);
            java.util.concurrent.BlockingQueue<Optional<Reply>> replies = session.get(keyExpr);
            Object value = parseReply(replies);
            keyExpr.close();

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
            KeyExpr keyExpr = KeyExpr.tryFrom(zenohKey);
            String json = objectMapper.writeValueAsString(new CacheValue(value));
            session.put(keyExpr, json);
            keyExpr.close();
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
            KeyExpr keyExpr = KeyExpr.tryFrom(zenohKey);
            session.delete(keyExpr);
            keyExpr.close();
            logger.debug("Zenoh cache evict for key={}", key);
        } catch (Exception e) {
            logger.warn("Zenoh cache evict failed for key={}: {}", key, e.getMessage());
        }
    }

    @Override
    public void clear() {
        // Zenoh doesn't support prefix-based deletion
        logger.info("Zenoh cache clear not supported");
    }

    private String buildKey(Object key) {
        return keyPrefix + name + "/" + key.toString().replaceAll("[^a-zA-Z0-9.-]", "_");
    }

    private Object parseReply(java.util.concurrent.BlockingQueue<Optional<Reply>> replies) {
        try {
            Optional<Reply> replyOpt = replies.poll(2, TimeUnit.SECONDS);
            if (replyOpt == null || !replyOpt.isPresent()) {
                return null;
            }
            Reply reply = replyOpt.get();
            Object sample = invokeGetSample(reply);
            if (sample == null) return null;
            Object payload = sample.getClass().getMethod("getPayload").invoke(sample);
            byte[] data = ((io.zenoh.bytes.ZBytes) payload).toBytes();
            CacheValue cacheValue = objectMapper.readValue(data, CacheValue.class);
            return cacheValue.getValue();
        } catch (Exception e) {
            logger.debug("Failed to parse Zenoh reply: {}", e.getMessage());
            return null;
        }
    }

    private Object invokeGetSample(Reply reply) {
        try {
            java.lang.reflect.Method getSampleMethod = reply.getClass().getMethod("getSample");
            return getSampleMethod.invoke(reply);
        } catch (Exception e) {
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
