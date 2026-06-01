package com.workoss.boot.vertx.data.dict.repository;

import com.workoss.boot.vertx.data.dict.entity.DictItem;

import java.util.List;

/**
 * Data provider interface for lazy loading dictionary data from database.
 * Users should implement this interface to provide data from their specific data source.
 */
public interface DictDataProvider {

    /**
     * Find a single dictionary item by type and code.
     *
     * @param type the dictionary type
     * @param code the dictionary code
     * @return the dictionary item, or null if not found
     */
    DictItem findByTypeAndCode(String type, String code);

    /**
     * Find all dictionary items of a specific type.
     *
     * @param type the dictionary type
     * @return list of dictionary items
     */
    List<DictItem> findByType(String type);

    /**
     * Find all dictionary items.
     *
     * @return all dictionary items
     */
    List<DictItem> findAll();
}
