package com.workoss.boot.vertx.data.dict.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoss.boot.vertx.data.dict.entity.DictItem;
import io.zenoh.Session;
import io.zenoh.keyexpr.KeyExpr;
import io.zenoh.query.Reply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * L2 Cache implementation using Zenoh (distributed cache backed by RocksDB via Zenoh Router).
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
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            String json = objectMapper.writeValueAsString(item);
            zenohSession.put(keyExpr, json);
            keyExpr.close();
            logger.debug("L2 cache put {}/{}", type, code);
        } catch (Exception e) {
            logger.warn("L2 cache put failed for {}/{}: {}", type, code, e.getMessage());
        }
    }

    @Override
    public Optional<List<DictItem>> getList(String type) {
        try {
            String key = buildListKey(type);
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            var replies = zenohSession.get(keyExpr);
            List<DictItem> items = parseListReply(replies);
            keyExpr.close();
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
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            String json = objectMapper.writeValueAsString(items);
            zenohSession.put(keyExpr, json);
            keyExpr.close();
            logger.debug("L2 cache put list for type={}", type);
        } catch (Exception e) {
            logger.warn("L2 cache list put failed for type={}: {}", type, e.getMessage());
        }
    }

    @Override
    public void evict(String type, String code) {
        try {
            String key = buildItemKey(type, code);
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            zenohSession.delete(keyExpr);
            keyExpr.close();
            logger.debug("L2 cache evict {}/{}", type, code);
        } catch (Exception e) {
            logger.warn("L2 cache evict failed for {}/{}: {}", type, code, e.getMessage());
        }
    }

    @Override
    public void evictType(String type) {
        try {
            String key = buildListKey(type);
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            zenohSession.delete(keyExpr);
            keyExpr.close();
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
            KeyExpr keyExpr = KeyExpr.tryFrom(key);
            var replies = zenohSession.get(keyExpr);
            Optional<Reply> replyOpt = replies.poll(2, TimeUnit.SECONDS);
            keyExpr.close();

            if (replyOpt == null || replyOpt.isEmpty()) {
                return Optional.empty();
            }
            T result = extractor.apply(replyOpt.get());
            return Optional.ofNullable(result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (Exception e) {
            logger.warn("L2 cache {} failed for {}/{}: {}", operation, type, code, e.getMessage());
            return Optional.empty();
        }
    }

    private DictItem extractItemFromReply(Reply reply) {
        try {
            Object sample = invokeGetSample(reply);
            if (sample == null) return null;
            Object payload = sample.getClass().getMethod("getPayload").invoke(sample);
            byte[] data = ((io.zenoh.bytes.ZBytes) payload).toBytes();
            return objectMapper.readValue(data, DictItem.class);
        } catch (Exception e) {
            logger.warn("Failed to extract item from reply: {}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private List<DictItem> parseListReply(java.util.concurrent.BlockingQueue<Optional<Reply>> replies) {
        try {
            Optional<Reply> replyOpt = replies.poll(2, TimeUnit.SECONDS);
            if (replyOpt == null || replyOpt.isEmpty()) {
                return null;
            }
            Object sample = invokeGetSample(replyOpt.get());
            if (sample == null) return null;
            String json = new String(((io.zenoh.bytes.ZBytes) sample.getClass().getMethod("getPayload").invoke(sample)).toBytes());
            return objectMapper.readValue(json, objectMapper.getTypeFactory().constructCollectionType(List.class, DictItem.class));
        } catch (Exception e) {
            logger.warn("Failed to parse list reply: {}", e.getMessage());
            return null;
        }
    }

    private Object invokeGetSample(Reply reply) throws Exception {
        try {
            return reply.getClass().getMethod("getSample").invoke(reply);
        } catch (NoSuchMethodException e) {
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
