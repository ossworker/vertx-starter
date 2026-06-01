package com.workoss.boot.vertx.data.dict.entity;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Dictionary item entity for cache storage.
 * <p>
 * Supports flat and tree-structured dictionary data.
 * Immutable by default; use {@link Builder} for construction.
 */
public final class DictItem implements Serializable, Comparable<DictItem> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String type;
    private final String code;
    private final String name;
    private final String parentCode;
    private final Integer sort;
    private final Map<String, Object> ext;

    private DictItem(Builder builder) {
        this.id = builder.id;
        this.type = builder.type;
        this.code = builder.code;
        this.name = builder.name;
        this.parentCode = builder.parentCode;
        this.sort = builder.sort;
        this.ext = builder.ext == null ? null : Map.copyOf(builder.ext);
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getParentCode() {
        return parentCode;
    }

    public Integer getSort() {
        return sort;
    }

    public Map<String, Object> getExt() {
        return ext;
    }

    /**
     * Return a new builder pre-filled with this item's values.
     */
    public Builder toBuilder() {
        return new Builder()
                .id(this.id)
                .type(this.type)
                .code(this.code)
                .name(this.name)
                .parentCode(this.parentCode)
                .sort(this.sort)
                .ext(this.ext);
    }

    @Override
    public int compareTo(DictItem o) {
        if (o == null) return 1;
        int sortCompare = compareSort(this.sort, o.sort);
        if (sortCompare != 0) return sortCompare;
        return Objects.compare(this.code, o.code, String::compareTo);
    }

    private static int compareSort(Integer a, Integer b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return Integer.compare(a, b);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DictItem that)) return false;
        return Objects.equals(type, that.type) && Objects.equals(code, that.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, code);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DictItem.class.getSimpleName() + "[", "]")
                .add("type=" + type)
                .add("code=" + code)
                .add("name=" + name)
                .toString();
    }

    /**
     * Builder for {@link DictItem}.
     */
    public static class Builder {
        private String id;
        private String type;
        private String code;
        private String name;
        private String parentCode;
        private Integer sort;
        private Map<String, Object> ext;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder code(String code) {
            this.code = code;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder parentCode(String parentCode) {
            this.parentCode = parentCode;
            return this;
        }

        public Builder sort(Integer sort) {
            this.sort = sort;
            return this;
        }

        public Builder ext(Map<String, Object> ext) {
            this.ext = ext == null ? null : Map.copyOf(ext);
            return this;
        }

        public DictItem build() {
            Objects.requireNonNull(type, "type must not be null");
            Objects.requireNonNull(code, "code must not be null");
            return new DictItem(this);
        }
    }
}
