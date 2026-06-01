package com.workoss.boot.vertx.data.dict.service;

import com.workoss.boot.vertx.data.dict.cache.DictCacheManager;
import com.workoss.boot.vertx.data.dict.entity.DictItem;
import com.workoss.boot.vertx.data.dict.repository.DictDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Dictionary service with two-level cache and lazy loading from database.
 * <p>
 * Cache lookup order: L1 (Caffeine) -> L2 (Zenoh) -> DB (lazy load)<br>
 * Write order: DB -> L2 (Zenoh) -> L1 (Caffeine)
 */
public class DictService {

    private static final Logger logger = LoggerFactory.getLogger(DictService.class);

    private final DictCacheManager cacheManager;
    private final DictDataProvider dataProvider;

    public DictService(DictCacheManager cacheManager, DictDataProvider dataProvider) {
        this.cacheManager = cacheManager;
        this.dataProvider = dataProvider;
    }

    /**
     * Get a single dictionary item by type and code with lazy loading.
     */
    public Optional<DictItem> get(String type, String code) {
        // 1. Try cache
        Optional<DictItem> cached = cacheManager.get(type, code);
        if (cached.isPresent()) {
            return cached;
        }

        // 2. Lazy load from DB
        return loadFromDb(() -> dataProvider != null ? dataProvider.findByTypeAndCode(type, code) : null)
                .map(item -> {
                    cacheManager.put(type, code, item);
                    return item;
                });
    }

    /**
     * Get all dictionary items of a specific type with lazy loading.
     */
    public List<DictItem> getList(String type) {
        // 1. Try cache
        Optional<List<DictItem>> cached = cacheManager.getList(type);
        if (cached.isPresent()) {
            return cached.get();
        }

        // 2. Lazy load from DB
        return loadFromDb(() -> dataProvider != null ? dataProvider.findByType(type) : null)
                .map(items -> {
                    cacheManager.putList(type, items);
                    return items;
                })
                .orElseGet(Collections::emptyList);
    }

    /**
     * Get all dictionary items grouped by type.
     */
    public Map<String, List<DictItem>> getAll() {
        if (dataProvider == null) {
            return Collections.emptyMap();
        }
        List<DictItem> allItems = dataProvider.findAll();
        if (allItems == null || allItems.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<DictItem>> grouped = allItems.stream()
                .collect(Collectors.groupingBy(
                        DictItem::getType,
                        LinkedHashMap::new,
                        Collectors.toList()));
        grouped.forEach(cacheManager::putList);
        return grouped;
    }

    /**
     * Get dictionary items as a tree structure.
     */
    public List<TreeNode<DictItem>> getTree(String type) {
        return buildTree(getList(type));
    }

    /**
     * Get a tree branch by parent code.
     */
    public List<TreeNode<DictItem>> getTreeByParent(String type, String parentCode) {
        List<DictItem> items = getList(type);
        if (items.isEmpty()) {
            return Collections.emptyList();
        }
        List<DictItem> filtered = items.stream()
                .filter(item -> matchesParent(item, parentCode))
                .sorted(Comparator.comparing(DictItem::getSort, Comparator.nullsFirst(Integer::compareTo)))
                .toList();
        return filtered.stream()
                .map(item -> new TreeNode<>(item, findChildren(items, item.getCode())))
                .toList();
    }

    private boolean matchesParent(DictItem item, String parentCode) {
        String itemParent = item.getParentCode();
        if (parentCode == null) {
            return itemParent == null || itemParent.isEmpty();
        }
        return parentCode.equals(itemParent);
    }

    private List<TreeNode<DictItem>> buildTree(List<DictItem> items) {
        if (items.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, List<DictItem>> parentMap = items.stream()
                .collect(Collectors.groupingBy(
                        item -> item.getParentCode() != null ? item.getParentCode() : "",
                        LinkedHashMap::new,
                        Collectors.toList()));

        List<DictItem> rootItems = parentMap.getOrDefault("", Collections.emptyList());
        if (rootItems.isEmpty()) {
            rootItems = items.stream()
                    .filter(item -> item.getParentCode() == null)
                    .sorted(Comparator.comparing(DictItem::getSort, Comparator.nullsFirst(Integer::compareTo)))
                    .toList();
        }

        return rootItems.stream()
                .map(item -> buildTreeNode(item, parentMap))
                .toList();
    }

    private TreeNode<DictItem> buildTreeNode(DictItem item, Map<String, List<DictItem>> parentMap) {
        List<DictItem> children = parentMap.getOrDefault(item.getCode(), Collections.emptyList());
        if (children.isEmpty()) {
            return new TreeNode<>(item, Collections.emptyList());
        }
        List<TreeNode<DictItem>> childNodes = children.stream()
                .sorted(Comparator.comparing(DictItem::getSort, Comparator.nullsFirst(Integer::compareTo)))
                .map(child -> buildTreeNode(child, parentMap))
                .toList();
        return new TreeNode<>(item, childNodes);
    }

    private List<TreeNode<DictItem>> findChildren(List<DictItem> items, String parentCode) {
        return items.stream()
                .filter(item -> parentCode.equals(item.getParentCode()))
                .sorted(Comparator.comparing(DictItem::getSort, Comparator.nullsFirst(Integer::compareTo)))
                .map(item -> new TreeNode<>(item, findChildren(items, item.getCode())))
                .toList();
    }

    private <T> Optional<T> loadFromDb(Supplier<T> supplier) {
        try {
            return Optional.ofNullable(supplier.get());
        } catch (Exception e) {
            logger.warn("Failed to load from database: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
