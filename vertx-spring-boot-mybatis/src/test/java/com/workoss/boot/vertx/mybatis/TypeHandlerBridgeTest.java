package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.typehandler.VertxTypeHandlerBridge;
import io.vertx.sqlclient.Row;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.EnumOrdinalTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link VertxTypeHandlerBridge}: JDBC-shaped MyBatis {@code TypeHandler}s are adapted
 * onto vertx-sql-client values on both the write (Tuple) and read (Row) sides, and handlers that touch
 * an unsupported part of the JDBC surface fall back cleanly instead of throwing.
 */
class TypeHandlerBridgeTest {

    private final VertxTypeHandlerBridge bridge = new VertxTypeHandlerBridge();

    enum Colour {
        RED, GREEN, BLUE
    }

    // ===== Write side =====

    @Test
    void writeEnumOrdinalProducesIntTupleValue() {
        EnumOrdinalTypeHandler<Colour> handler = new EnumOrdinalTypeHandler<>(Colour.class);
        // BLUE.ordinal() == 2
        Object value = bridge.tryWrite(handler, Colour.BLUE, JdbcType.INTEGER);
        assertEquals(2, value);
    }

    @Test
    void writeCustomHandlerCapturesTransformedValue() {
        Object value = bridge.tryWrite(new UpperCasingStringHandler(), "abc", null);
        assertEquals("ABC", value);
    }

    @Test
    void writeNullValueBecomesSqlNull() {
        Object value = bridge.tryWrite(new UpperCasingStringHandler(), null, JdbcType.VARCHAR);
        assertEquals(null, value);
    }

    @Test
    void writeUnsupportedStreamHandlerFallsBack() {
        Object value = bridge.tryWrite(new StreamWritingHandler(), "abc", JdbcType.VARCHAR);
        assertSame(VertxTypeHandlerBridge.FALLBACK, value);
    }

    @Test
    void writeNullHandlerFallsBack() {
        assertSame(VertxTypeHandlerBridge.FALLBACK, bridge.tryWrite(null, "x", null));
    }

    // ===== Read side =====

    @Test
    void readEnumOrdinalRestoresEnum() {
        Row row = mock(Row.class);
        when(row.getValue("colour")).thenReturn(1); // GREEN.ordinal()
        EnumOrdinalTypeHandler<Colour> handler = new EnumOrdinalTypeHandler<>(Colour.class);
        Object value = bridge.tryRead(handler, row, "colour");
        assertEquals(Colour.GREEN, value);
    }

    @Test
    void readCustomHandlerTransformsColumnValue() {
        Row row = mock(Row.class);
        when(row.getValue("name")).thenReturn("BOB");
        Object value = bridge.tryRead(new UpperCasingStringHandler(), row, "name");
        assertEquals("bob", value);
    }

    @Test
    void readNullColumnReturnsNull() {
        Row row = mock(Row.class);
        when(row.getValue("name")).thenReturn(null);
        Object value = bridge.tryRead(new UpperCasingStringHandler(), row, "name");
        assertEquals(null, value);
    }

    @Test
    void readUnsupportedStreamHandlerFallsBack() {
        Row row = mock(Row.class);
        Object value = bridge.tryRead(new StreamReadingHandler(), row, "blob");
        assertSame(VertxTypeHandlerBridge.FALLBACK, value);
    }

    @Test
    void readNullOrRowFallsBack() {
        assertSame(VertxTypeHandlerBridge.FALLBACK, bridge.tryRead(null, mock(Row.class), "c"));
        assertSame(VertxTypeHandlerBridge.FALLBACK,
                bridge.tryRead(new UpperCasingStringHandler(), null, "c"));
    }

    // ===== Test handlers =====

    /** Upper-cases on write, lower-cases on read — proves the transform runs through the proxy. */
    static class UpperCasingStringHandler extends BaseTypeHandler<String> {
        @Override
        public void setNonNullParameter(PreparedStatement ps, int i, String parameter, JdbcType jdbcType)
                throws SQLException {
            ps.setString(i, parameter.toUpperCase(Locale.ROOT));
        }

        @Override
        public String getNullableResult(ResultSet rs, String columnName) throws SQLException {
            String s = rs.getString(columnName);
            return s == null ? null : s.toLowerCase(Locale.ROOT);
        }

        @Override
        public String getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
            String s = rs.getString(columnIndex);
            return s == null ? null : s.toLowerCase(Locale.ROOT);
        }

        @Override
        public String getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
            return cs.getString(columnIndex);
        }
    }

    /** Touches an unsupported JDBC setter (streams) — must trigger fallback, not an exception. */
    static class StreamWritingHandler extends BaseTypeHandler<String> {
        @Override
        public void setNonNullParameter(PreparedStatement ps, int i, String parameter, JdbcType jdbcType)
                throws SQLException {
            ps.setAsciiStream(i, new ByteArrayInputStream(parameter.getBytes()), parameter.length());
        }

        @Override
        public String getNullableResult(ResultSet rs, String columnName) throws SQLException {
            return rs.getString(columnName);
        }

        @Override
        public String getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
            return rs.getString(columnIndex);
        }

        @Override
        public String getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
            return cs.getString(columnIndex);
        }
    }

    /** Touches an unsupported JDBC getter (character stream) — must trigger fallback. */
    static class StreamReadingHandler extends BaseTypeHandler<String> {
        @Override
        public void setNonNullParameter(PreparedStatement ps, int i, String parameter, JdbcType jdbcType)
                throws SQLException {
            ps.setString(i, parameter);
        }

        @Override
        public String getNullableResult(ResultSet rs, String columnName) throws SQLException {
            Reader reader = rs.getCharacterStream(columnName);
            return reader == null ? null : "read";
        }

        @Override
        public String getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
            return rs.getString(columnIndex);
        }

        @Override
        public String getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
            return cs.getString(columnIndex);
        }
    }
}
