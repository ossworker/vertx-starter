package com.workoss.boot.vertx.dict;

import com.workoss.boot.vertx.data.dict.entity.DictItem;
import com.workoss.boot.vertx.data.dict.repository.DictDataProvider;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory dictionary data provider for demonstration.
 * Simulates a database-backed data source with regions and user status types.
 */
@Component
public class InMemoryDictDataProvider implements DictDataProvider {

    private final Map<String, DictItem> dataStore = new ConcurrentHashMap<>();

    public InMemoryDictDataProvider() {
        initRegionData();
        initUserStatusData();
        initProductCategoryData();
    }

    private void initRegionData() {
        // Country level
        put(new DictItem.Builder()
                .type("region")
                .code("CN")
                .name("中国")
                .sort(1)
                .build());
        put(new DictItem.Builder()
                .type("region")
                .code("US")
                .name("美国")
                .sort(2)
                .build());

        // Province level under CN
        put(new DictItem.Builder()
                .type("region")
                .code("BJ")
                .name("北京")
                .parentCode("CN")
                .sort(1)
                .build());
        put(new DictItem.Builder()
                .type("region")
                .code("SH")
                .name("上海")
                .parentCode("CN")
                .sort(2)
                .build());
        put(new DictItem.Builder()
                .type("region")
                .code("GD")
                .name("广东")
                .parentCode("CN")
                .sort(3)
                .build());

        // City level under GD
        put(new DictItem.Builder()
                .type("region")
                .code("SZ")
                .name("深圳")
                .parentCode("GD")
                .sort(1)
                .build());
        put(new DictItem.Builder()
                .type("region")
                .code("GZ")
                .name("广州")
                .parentCode("GD")
                .sort(2)
                .build());
    }

    private void initUserStatusData() {
        put(new DictItem.Builder()
                .type("user.status")
                .code("ACTIVE")
                .name("活跃")
                .sort(1)
                .build());
        put(new DictItem.Builder()
                .type("user.status")
                .code("INACTIVE")
                .name("停用")
                .sort(2)
                .build());
        put(new DictItem.Builder()
                .type("user.status")
                .code("PENDING")
                .name("待审核")
                .sort(3)
                .build());
        put(new DictItem.Builder()
                .type("user.status")
                .code("DELETED")
                .name("已删除")
                .sort(4)
                .build());
    }

    private void initProductCategoryData() {
        // Electronics
        put(new DictItem.Builder()
                .type("product.category")
                .code("ELECTRONICS")
                .name("电子产品")
                .sort(1)
                .build());
        put(new DictItem.Builder()
                .type("product.category")
                .code("PHONE")
                .name("手机")
                .parentCode("ELECTRONICS")
                .sort(1)
                .build());
        put(new DictItem.Builder()
                .type("product.category")
                .code("LAPTOP")
                .name("笔记本")
                .parentCode("ELECTRONICS")
                .sort(2)
                .build());

        // Clothing
        put(new DictItem.Builder()
                .type("product.category")
                .code("CLOTHING")
                .name("服装")
                .sort(2)
                .build());
        put(new DictItem.Builder()
                .type("product.category")
                .code("SHIRT")
                .name("衬衫")
                .parentCode("CLOTHING")
                .sort(1)
                .build());
    }

    private void put(DictItem item) {
        dataStore.put(item.getType() + ":" + item.getCode(), item);
    }

    @Override
    public DictItem findByTypeAndCode(String type, String code) {
        return dataStore.get(type + ":" + code);
    }

    @Override
    public List<DictItem> findByType(String type) {
        return dataStore.values().stream()
                .filter(item -> item.getType().equals(type))
                .collect(Collectors.toList());
    }

    @Override
    public List<DictItem> findAll() {
        return List.copyOf(dataStore.values());
    }
}
