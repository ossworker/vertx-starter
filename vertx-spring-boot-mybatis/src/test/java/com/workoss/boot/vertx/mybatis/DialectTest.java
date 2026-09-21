package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.dialect.Db2Dialect;
import com.workoss.boot.vertx.mybatis.dialect.DialectResolver;
import com.workoss.boot.vertx.mybatis.dialect.MsSqlDialect;
import com.workoss.boot.vertx.mybatis.dialect.MySqlDialect;
import com.workoss.boot.vertx.mybatis.dialect.PostgreSqlDialect;
import com.workoss.boot.vertx.mybatis.dialect.SqlDialect;
import org.apache.ibatis.session.RowBounds;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link SqlDialect} SPI: placeholder rewriting (literal/comment aware)
 * and physical pagination per dialect.
 */
class DialectTest {

    // ===== Placeholder rewriting =====

    @Test
    void mySqlKeepsNativePlaceholders() {
        SqlDialect d = new MySqlDialect();
        String sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        assertEquals(sql, d.rewritePlaceholders(sql));
        assertEquals("MYSQL", d.name());
    }

    @Test
    void postgreSqlUsesPositionalPlaceholders() {
        SqlDialect d = new PostgreSqlDialect();
        assertEquals("SELECT * FROM t WHERE a = $1 AND b = $2",
                d.rewritePlaceholders("SELECT * FROM t WHERE a = ? AND b = ?"));
        assertEquals("POSTGRESQL", d.name());
    }

    @Test
    void msSqlUsesNamedPlaceholders() {
        SqlDialect d = new MsSqlDialect();
        assertEquals("SELECT * FROM t WHERE a = @p1 AND b = @p2",
                d.rewritePlaceholders("SELECT * FROM t WHERE a = ? AND b = ?"));
        assertEquals("MSSQL", d.name());
    }

    @Test
    void db2KeepsNativePlaceholders() {
        SqlDialect d = new Db2Dialect();
        String sql = "SELECT * FROM t WHERE a = ?";
        assertEquals(sql, d.rewritePlaceholders(sql));
        assertEquals("DB2", d.name());
    }

    @Test
    void questionMarkInsideStringLiteralIsNotReplaced() {
        SqlDialect d = new PostgreSqlDialect();
        assertEquals("SELECT * FROM t WHERE a = '?' AND b = $1",
                d.rewritePlaceholders("SELECT * FROM t WHERE a = '?' AND b = ?"));
    }

    @Test
    void escapedQuoteInsideLiteralIsHandled() {
        SqlDialect d = new PostgreSqlDialect();
        // 'it''s ?' is a single literal containing an escaped quote and a literal '?'
        assertEquals("SELECT * FROM t WHERE a = 'it''s ?' AND b = $1",
                d.rewritePlaceholders("SELECT * FROM t WHERE a = 'it''s ?' AND b = ?"));
    }

    @Test
    void questionMarkInsideLineCommentIsNotReplaced() {
        SqlDialect d = new PostgreSqlDialect();
        String sql = "SELECT ? -- is this a param?\nFROM t WHERE b = ?";
        assertEquals("SELECT $1 -- is this a param?\nFROM t WHERE b = $2",
                d.rewritePlaceholders(sql));
    }

    @Test
    void questionMarkInsideBlockCommentIsNotReplaced() {
        SqlDialect d = new PostgreSqlDialect();
        String sql = "SELECT /* why? */ ? FROM t WHERE b = ?";
        assertEquals("SELECT /* why? */ $1 FROM t WHERE b = $2",
                d.rewritePlaceholders(sql));
    }

    @Test
    void rewritingWithoutAnyPlaceholderIsIdentity() {
        SqlDialect d = new PostgreSqlDialect();
        assertEquals("SELECT 1", d.rewritePlaceholders("SELECT 1"));
    }

    // ===== Pagination =====

    @Test
    void noPagingLeavesSqlUnchanged() {
        for (SqlDialect d : new SqlDialect[]{new MySqlDialect(), new PostgreSqlDialect(),
                new MsSqlDialect(), new Db2Dialect()}) {
            assertEquals("SELECT * FROM t", d.applyPagination("SELECT * FROM t", RowBounds.DEFAULT));
            assertEquals("SELECT * FROM t", d.applyPagination("SELECT * FROM t", null));
        }
    }

    @Test
    void mySqlLimitOffset() {
        assertEquals("SELECT * FROM t LIMIT 5 OFFSET 10",
                new MySqlDialect().applyPagination("SELECT * FROM t", new RowBounds(10, 5)));
    }

    @Test
    void postgreSqlLimitOffset() {
        assertEquals("SELECT * FROM t LIMIT 5 OFFSET 10",
                new PostgreSqlDialect().applyPagination("SELECT * FROM t", new RowBounds(10, 5)));
    }

    @Test
    void mySqlLimitOnlyWhenOffsetZero() {
        assertEquals("SELECT * FROM t LIMIT 5",
                new MySqlDialect().applyPagination("SELECT * FROM t", new RowBounds(0, 5)));
    }

    @Test
    void msSqlAppendsOrderByWhenMissing() {
        assertEquals("SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
                new MsSqlDialect().applyPagination("SELECT * FROM t", new RowBounds(10, 5)));
    }

    @Test
    void msSqlKeepsExistingOrderBy() {
        assertEquals("SELECT * FROM t ORDER BY id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
                new MsSqlDialect().applyPagination("SELECT * FROM t ORDER BY id", new RowBounds(10, 5)));
    }

    @Test
    void msSqlFetchFirstWhenOffsetZero() {
        assertEquals("SELECT * FROM t ORDER BY (SELECT NULL) FETCH FIRST 5 ROWS ONLY",
                new MsSqlDialect().applyPagination("SELECT * FROM t", new RowBounds(0, 5)));
    }

    @Test
    void db2OffsetFetch() {
        assertEquals("SELECT * FROM t OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
                new Db2Dialect().applyPagination("SELECT * FROM t", new RowBounds(10, 5)));
    }

    @Test
    void db2FetchFirstWhenOffsetZero() {
        assertEquals("SELECT * FROM t FETCH FIRST 5 ROWS ONLY",
                new Db2Dialect().applyPagination("SELECT * FROM t", new RowBounds(0, 5)));
    }

    // ===== DialectResolver =====

    @Test
    void resolverHonoursExplicitName() {
        assertEquals("POSTGRESQL", DialectResolver.resolve("postgresql", null).name());
        assertEquals("MSSQL", DialectResolver.resolve("SQLSERVER", null).name());
        assertEquals("DB2", DialectResolver.resolve("db2", null).name());
        assertEquals("MYSQL", DialectResolver.resolve("mariadb", null).name());
    }

    @Test
    void resolverAutoAndUnknownFallBackToDetection() {
        // With a null pool, detection defaults to MySQL (matches the previous AUTO behaviour).
        assertEquals("MYSQL", DialectResolver.resolve(null, null).name());
        assertEquals("MYSQL", DialectResolver.resolve("AUTO", null).name());
        assertEquals("MYSQL", DialectResolver.resolve("", null).name());
        assertEquals("MYSQL", DialectResolver.resolve("nonsense-dialect", null).name());
    }

    @Test
    void byNameReturnsSharedInstancesForUnknown() {
        assertTrue(DialectResolver.byName("nope") == null);
        assertSame(MySqlDialect.class, DialectResolver.byName("MYSQL").getClass());
    }
}
