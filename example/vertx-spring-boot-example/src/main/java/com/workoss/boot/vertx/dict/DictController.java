package com.workoss.boot.vertx.dict;

import com.workoss.boot.vertx.data.dict.entity.DictItem;
import com.workoss.boot.vertx.data.dict.service.DictService;
import com.workoss.boot.vertx.data.dict.service.TreeNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Dictionary REST API demonstrating two-level cache usage.
 */
@Slf4j
@RestController
@RequestMapping("/api/dict")
@RequiredArgsConstructor
public class DictController {

    private final DictService dictService;

    /**
     * Get a single dictionary item by type and code.
     * Cache lookup order: L1 (Caffeine) -> L2 (Zenoh) -> DB (lazy load)
     */
    @GetMapping("/{type}/{code}")
    public DictItem get(@PathVariable String type, @PathVariable String code) {
        log.info("GET /api/dict/{}/{} - cache lookup", type, code);
        return dictService.get(type, code).orElse(null);
    }

    /**
     * Get all dictionary items of a specific type.
     */
    @GetMapping("/{type}")
    public List<DictItem> getList(@PathVariable String type) {
        log.info("GET /api/dict/{} - list lookup", type);
        return dictService.getList(type);
    }

    /**
     * Get dictionary items as a tree structure (hierarchical).
     * Supports parent-child relationships for region, category, etc.
     */
    @GetMapping("/{type}/tree")
    public List<TreeNode<DictItem>> getTree(@PathVariable String type) {
        log.info("GET /api/dict/{}/tree - tree lookup", type);
        return dictService.getTree(type);
    }

    /**
     * Get a specific tree branch by parent code.
     */
    @GetMapping("/{type}/tree/children")
    public List<TreeNode<DictItem>> getTreeByParent(
            @PathVariable String type,
            @RequestParam(required = false) String parentCode) {
        log.info("GET /api/dict/{}/tree/children?parentCode={}", type, parentCode);
        return dictService.getTreeByParent(type, parentCode);
    }

    /**
     * Get all dictionary data grouped by type.
     */
    @GetMapping("/all")
    public Map<String, List<DictItem>> getAll() {
        log.info("GET /api/dict/all - full load");
        return dictService.getAll();
    }
}
