package com.workoss.boot.vertx.mybatis.core;

import org.apache.ibatis.cursor.Cursor;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link Cursor} implementation for vertx-mybatis.
 * <p>
 * Since vertx-sql-client does not support JDBC-style streaming {@code ResultSet},
 * this cursor loads all results eagerly (same as a regular query) and provides
 * the standard {@link Cursor} API for lazy iteration with status tracking.
 * <p>
 * Typical usage:
 * <pre>{@code
 * try (Cursor<User> cursor = mapper.selectAllCursor()) {
 *     for (User user : cursor) {
 *         // process user
 *     }
 * }
 * }</pre>
 *
 * @param <T> the element type
 */
public class VertxCursor<T> implements Cursor<T> {

    private final List<T> results;
    private int currentIndex = -1;
    private boolean closed = false;

    public VertxCursor(List<T> results) {
        this.results = results != null ? results : Collections.emptyList();
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public boolean isConsumed() {
        return currentIndex >= 0 && currentIndex >= results.size() - 1;
    }

    @Override
    public int getCurrentIndex() {
        return currentIndex;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            private int iterIndex = -1;

            @Override
            public boolean hasNext() {
                return !closed && iterIndex < results.size() - 1;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more elements in cursor");
                }
                iterIndex++;
                currentIndex = iterIndex;
                return results.get(iterIndex);
            }
        };
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}
