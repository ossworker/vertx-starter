package com.workoss.boot.vertx.data.dict.service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Generic tree node for hierarchical data structures.
 *
 * @param <T> the type of data held by this node
 */
public final class TreeNode<T> {

    private final T data;
    private final List<TreeNode<T>> children;

    public TreeNode(T data, List<TreeNode<T>> children) {
        this.data = data;
        this.children = children != null ? List.copyOf(children) : Collections.emptyList();
    }

    public T getData() {
        return data;
    }

    public List<TreeNode<T>> getChildren() {
        return children;
    }

    public boolean isLeaf() {
        return children.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TreeNode<?> that)) return false;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TreeNode.class.getSimpleName() + "[", "]")
                .add("data=" + data)
                .add("childrenCount=" + children.size())
                .toString();
    }
}
