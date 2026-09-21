package com.workoss.boot.vertx.mybatis.dialect;

import org.apache.ibatis.session.RowBounds;

/**
 * Database dialect SPI for vertx-mybatis.
 * <p>
 * A dialect owns two database-specific concerns that were previously hard-coded
 * into {@code SqlResolver} (placeholder rewriting) and {@code VertxExecutor}
 * (fragile string-spliced pagination):
 * <ul>
 *   <li>{@link #rewritePlaceholders(String)} — convert MyBatis {@code ?} placeholders
 *       into the dialect's positional/named form (e.g. PostgreSQL {@code $1}, MSSQL {@code @p1}).</li>
 *   <li>{@link #applyPagination(String, RowBounds)} — append dialect-correct paging
 *       for a logical {@link RowBounds} window.</li>
 * </ul>
 * Implementations must be immutable and thread-safe; a single instance is shared
 * across all executions.
 */
public interface SqlDialect {

    /** Stable identifier used for configuration and logging (e.g. {@code MYSQL}). */
    String name();

    /**
     * Rewrite MyBatis {@code ?} placeholders into this dialect's parameter syntax.
     * Implementations must leave {@code ?} inside string literals and comments untouched.
     *
     * @param sql the SQL with {@code ?} placeholders
     * @return the rewritten SQL
     */
    String rewritePlaceholders(String sql);

    /**
     * Append dialect-specific pagination for the given logical row window.
     *
     * @param sql       the SQL (without paging)
     * @param rowBounds offset/limit window; {@link RowBounds#DEFAULT} means "no paging"
     * @return the paged SQL
     */
    String applyPagination(String sql, RowBounds rowBounds);
}
