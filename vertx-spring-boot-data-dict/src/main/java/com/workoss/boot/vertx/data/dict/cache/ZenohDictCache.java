package com.workoss.boot.vertx.data.dict.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoss.boot.vertx.data.dict.entity.DictItem;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * L2 Cache implementation using Zenoh (distributed cache backed by RocksDB via Zenoh Router).
 * Uses zenoh-jni native bindings.
 */
public class ZenohDictCache implements DictCache {

    private static final Logger logger = LoggerFactory.getLogger(ZenohDictCache.class);

    private final Session zenohSession;
    private final String keyPrefix;
    private final ObjectMapper objectMapper;

    public ZenohDictCache(Session zenohSession, String keyPrefix, ObjectMapper objectMapper) {
        this.zenohSession = zenohSession;
        this.keyPrefix = keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<DictItem> get(String type, String code) {
        return query(type, code, this::extractItemFromReply, "get");
    }

    @Override
    public void put(String type, String code, DictItem item) {
        try {
            String key = buildItemKey(type, code);
            String json = objectMapper.writeValueAsString(item);
            zenohSession.put(key, json.getBytes(StandardCharsets.UTF_8));
            logger.debug("L2 cache put {}/{}", type, code);
        } catch (Exception e) {
            logger.warn("L2 cache put failed for {}/{}: {}", type, code, e.getMessage());
        }
    }

    @Override
    public Optional<List<DictItem>> getList(String type) {
        try {
            String key = buildListKey(type);
            List<Reply> replies = zenohSession.get(key);
            List<DictItem> items = parseListReply(replies);
            return Optional.ofNullable(items);
        } catch (Exception e) {
            logger.warn("L2 cache list get failed for type={}: {}", type, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void putList(String type, List<DictItem> items) {
        try {
            String key = buildListKey(type);
            String json = objectMapper.writeValueAsString(items);
            zenohSession.put(key, json.getBytes(StandardCharsets.UTF_8));
            logger.debug("L2 cache put list for type={}", type);
        } catch (Exception e) {
            logger.warn("L2 cache list put failed for type={}: {}", type, e.getMessage());
        }
    }

    @Override
    public void evict(String type, String code) {
        try {
            String key = buildItemKey(type, code);
            zenohSession.delete(key);
            logger.debug("L2 cache evict {}/{}", type, code);
        } catch (Exception e) {
            logger.warn("L2 cache evict failed for {}/{}: {}", type, code, e.getMessage());
        }
    }

    @Override
    public void evictType(String type) {
        try {
            String key = buildListKey(type);
            zenohSession.delete(key);
            logger.debug("L2 cache evict type={}", type);
        } catch (Exception e) {
            logger.warn("L2 cache evict type failed for type={}: {}", type, e.getMessage());
        }
    }

    @Override
    public void evictAll() {
        logger.info("L2 cache evict all is not supported via Zenoh directly");
    }

    private <T> Optional<T> query(String type, String code, Function<Reply, T> extractor, String operation) {
        try {
            String key = buildItemKey(type, code);
            List<Reply> replies = zenohSession.get(key);
            if (replies.isEmpty()) {
                return Optional.empty();
            }
            T result = extractor.apply(replies.get(0));
            return Optional.ofNullable(result);
        } catch (Exception e) {
            logger.warn("L2 cache {} failed for {}/{}: {}", operation, type, code, e.getMessage());
            return Optional.empty();
        }
    }

    private DictItem extractItemFromReply(Reply reply) {
        try {
            if (!reply.success() || reply.samplePayload().isEmpty()) return null;
            byte[] data = reply.samplePayload().get();
            return objectMapper.readValue(data, DictItem.class);
        } catch (Exception e) {
            logger.warn("Failed to extract item from reply: {}", e.getMessage());
            return null;
        }
    }

    private List<DictItem> parseListReply(List<Reply> replies) {
        try {
            if (replies.isEmpty()) return null;
            Reply reply = replies.get(0);
            if (!reply.success() || reply.samplePayload().isEmpty()) return null;
            String json = new String(reply.samplePayload().get(), StandardCharsets.UTF_8);
            return objectMapper.readValue(json, objectMapper.getTypeFactory().constructCollectionType(List.class, DictItem.class));
        } catch (Exception e) {
            logger.warn("Failed to parse list reply: {}", e.getMessage());
            return null;
        }
    }

    private String buildItemKey(String type, String code) {
        return keyPrefix + "item/" + type + "/" + code;
    }

    private String buildListKey(String type) {
        return keyPrefix + "list/" + type;
    }
}
