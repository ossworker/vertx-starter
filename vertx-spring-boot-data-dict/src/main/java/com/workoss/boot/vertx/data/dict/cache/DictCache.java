package com.workoss.boot.vertx.data.dict.cache;

import com.workoss.boot.vertx.data.dict.entity.DictItem;

import java.util.List;
import java.util.Optional;

/**
 * Abstraction for dictionary data storage (L1/L2 cache implementations).
 * <p>
 * All operations are best-effort; failures are logged and swallowed
 * so that the caller can fall back gracefully.
 */
public interface DictCache {

    /**
     * Get a single dictionary item by type and code.
     *
     * @param type the dictionary type
     * @param code the dictionary code
     * @return the item, or {@link Optional#empty()} if not found
     */
    Optional<DictItem> get(String type, String code);

    /**
     * Put a single dictionary item.
     *
     * @param type the dictionary type
     * @param code the dictionary code
     * @param item the item to store
     */
    void put(String type, String code, DictItem item);

    /**
     * Get all items of a specific type.
     *
     * @param type the dictionary type
     * @return the list, or {@link Optional#empty()} if not found
     */
    Optional<List<DictItem>> getList(String type);

    /**
     * Put a list of dictionary items for a type.
     *
     * @param type  the dictionary type
     * @param items the items to store
     */
    void putList(String type, List<DictItem> items);

    /**
     * Evict a single item.
     *
     * @param type the dictionary type
     * @param code the dictionary code
     */
    void evict(String type, String code);

    /**
     * Evict all items of a specific type.
     *
     * @param type the dictionary type
     */
    void evictType(String type);

    /**
     * Evict all items.
     */
    void evictAll();
}
