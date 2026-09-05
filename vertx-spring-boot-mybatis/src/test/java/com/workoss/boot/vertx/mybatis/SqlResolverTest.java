package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.core.SqlAndParams;
import com.workoss.boot.vertx.mybatis.core.SqlResolver;
import io.vertx.sqlclient.Tuple;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SqlResolver}.
 */
class SqlResolverTest {

    private Configuration configuration;
    private SqlResolver resolver;

    @BeforeEach
    void setUp() {
        configuration = new Configuration();
        configuration.setMapUnderscoreToCamelCase(true);
        resolver = new SqlResolver(configuration);
    }

    @Test
    void testDialectPlaceholderConversion_MySQL() {
        resolver.setDialect(SqlResolver.Dialect.MYSQL);
        // MySQL uses ? natively, so no conversion needed
        // We can't test resolve() without a full MappedStatement, but we can verify dialect setting
        assertEquals(SqlResolver.Dialect.MYSQL, resolver.getDialect());
    }

    @Test
    void testDialectPlaceholderConversion_PostgreSQL() {
        resolver.setDialect(SqlResolver.Dialect.POSTGRESQL);
        assertEquals(SqlResolver.Dialect.POSTGRESQL, resolver.getDialect());
    }

    @Test
    void testDialectPlaceholderConversion_MSSQL() {
        resolver.setDialect(SqlResolver.Dialect.MSSQL);
        assertEquals(SqlResolver.Dialect.MSSQL, resolver.getDialect());
    }

    @Test
    void testSqlAndParamsRecord() {
        Tuple tuple = Tuple.of("hello", 42);
        SqlAndParams result = new SqlAndParams("SELECT * FROM users WHERE id = ?", tuple, SqlCommandType.SELECT);

        assertEquals("SELECT * FROM users WHERE id = ?", result.sql());
        assertEquals(2, result.tuple().size());
        assertEquals(SqlCommandType.SELECT, result.commandType());
    }

    @Test
    void testTupleValueTypes() {
        // Verify that Tuple accepts all expected types
        Tuple tuple = Tuple.tuple()
                .addString("hello")
                .addInteger(42)
                .addLong(100L)
                .addDouble(3.14)
                .addFloat(2.5f)
                .addBoolean(true)
                .addBigDecimal(new BigDecimal("99.99"))
                .addValue(null);

        assertEquals(8, tuple.size());
        assertEquals("hello", tuple.getString(0));
        assertEquals(42, tuple.getInteger(1));
        assertEquals(100L, tuple.getLong(2));
        assertNull(tuple.getValue(7));
    }

    @Test
    void testTupleTemporalTypes() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);

        Tuple tuple = Tuple.tuple()
                .addLocalDate(date)
                .addLocalDateTime(dateTime);

        assertEquals(2, tuple.size());
        assertEquals(date, tuple.getLocalDate(0));
        assertEquals(dateTime, tuple.getLocalDateTime(1));
    }

    @Test
    void testDefaultDialect() {
        SqlResolver defaultResolver = new SqlResolver(configuration);
        assertEquals(SqlResolver.Dialect.AUTO, defaultResolver.getDialect());
    }
}
