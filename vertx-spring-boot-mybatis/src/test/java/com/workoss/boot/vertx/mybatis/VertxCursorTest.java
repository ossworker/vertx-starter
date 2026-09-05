package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.core.VertxCursor;
import com.workoss.boot.vertx.mybatis.mapper.MapperMethodInfo;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VertxCursor} and Cursor return type detection.
 */
class VertxCursorTest {

    // ===== VertxCursor behavior tests =====

    @Test
    void testCursorIteration() {
        List<String> data = Arrays.asList("a", "b", "c");
        VertxCursor<String> cursor = new VertxCursor<>(data);

        assertTrue(cursor.isOpen());
        assertFalse(cursor.isConsumed());
        assertEquals(-1, cursor.getCurrentIndex());

        Iterator<String> it = cursor.iterator();
        assertTrue(it.hasNext());
        assertEquals("a", it.next());
        assertEquals(0, cursor.getCurrentIndex());

        assertEquals("b", it.next());
        assertEquals(1, cursor.getCurrentIndex());

        assertEquals("c", it.next());
        assertEquals(2, cursor.getCurrentIndex());
        assertTrue(cursor.isConsumed());
        assertFalse(it.hasNext());
    }

    @Test
    void testCursorClose() throws IOException {
        VertxCursor<Integer> cursor = new VertxCursor<>(List.of(1, 2, 3));
        assertTrue(cursor.isOpen());

        cursor.close();
        assertFalse(cursor.isOpen());

        // After close, iterator should report no more elements
        assertFalse(cursor.iterator().hasNext());
    }

    @Test
    void testEmptyCursor() {
        VertxCursor<String> cursor = new VertxCursor<>(List.of());
        assertTrue(cursor.isOpen());
        assertFalse(cursor.isConsumed());
        assertFalse(cursor.iterator().hasNext());
    }

    @Test
    void testNullResults() {
        VertxCursor<String> cursor = new VertxCursor<>(null);
        assertTrue(cursor.isOpen());
        assertFalse(cursor.iterator().hasNext());
    }

    @Test
    void testSingleElementCursor() {
        VertxCursor<String> cursor = new VertxCursor<>(List.of("only"));
        Iterator<String> it = cursor.iterator();

        assertTrue(it.hasNext());
        assertEquals("only", it.next());
        assertTrue(cursor.isConsumed());
        assertFalse(it.hasNext());
    }

    @Test
    void testCursorTryWithResources() throws IOException {
        List<Integer> data = List.of(10, 20, 30);
        int sum = 0;

        try (Cursor<Integer> cursor = new VertxCursor<>(data)) {
            for (int val : cursor) {
                sum += val;
            }
        }

        assertEquals(60, sum);
    }

    @Test
    void testNoSuchElementException() {
        VertxCursor<String> cursor = new VertxCursor<>(List.of("one"));
        Iterator<String> it = cursor.iterator();
        it.next();
        assertThrows(NoSuchElementException.class, it::next);
    }

    // ===== MapperMethodInfo Cursor detection =====

    @Test
    void testCursorReturnTypeDetection() throws Exception {
        Configuration config = new Configuration();

        // Add a MappedStatement for the cursor method
        var builder = new org.apache.ibatis.mapping.MappedStatement.Builder(
                config,
                CursorMapper.class.getName() + ".findCursor",
                new org.apache.ibatis.scripting.defaults.RawSqlSource(config, "SELECT 1", null),
                SqlCommandType.SELECT);
        config.addMappedStatement(builder.build());

        var method = CursorMapper.class.getMethod("findCursor");
        MapperMethodInfo info = new MapperMethodInfo(CursorMapper.class, method, config);

        assertTrue(info.isCursorReturnType(), "Should detect Cursor<T> return type");
        assertFalse(info.isFutureReturnType(), "Cursor should not be treated as Future");
        assertFalse(info.isSingleResult(), "Cursor is not a single result");
    }

    @Test
    void testCursorWithParamDetection() throws Exception {
        Configuration config = new Configuration();

        var builder = new org.apache.ibatis.mapping.MappedStatement.Builder(
                config,
                CursorMapper.class.getName() + ".findCursorByName",
                new org.apache.ibatis.scripting.defaults.RawSqlSource(config, "SELECT 1", null),
                SqlCommandType.SELECT);
        config.addMappedStatement(builder.build());

        var method = CursorMapper.class.getMethod("findCursorByName", String.class);
        MapperMethodInfo info = new MapperMethodInfo(CursorMapper.class, method, config);

        assertTrue(info.isCursorReturnType());
        assertEquals("hello", info.extractParameter(new Object[]{"hello"}));
    }

    // Test mapper interface with Cursor methods
    interface CursorMapper {
        Cursor<String> findCursor();

        Cursor<String> findCursorByName(@org.apache.ibatis.annotations.Param("name") String name);
    }
}
