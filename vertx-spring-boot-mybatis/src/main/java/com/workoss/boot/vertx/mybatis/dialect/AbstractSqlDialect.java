package com.workoss.boot.vertx.mybatis.dialect;

import org.apache.ibatis.session.RowBounds;

import java.util.function.IntFunction;

/**
 * Base {@link SqlDialect} providing a literal/comment-aware placeholder scanner and
 * the two common physical pagination strategies.
 * <p>
 * The scanner replaces {@code ?} only when it appears in real SQL — never inside
 * single-quoted string literals (including {@code ''} escapes), {@code --} line
 * comments, or {@code /* *}{@code /} block comments. This fixes the previous
 * implementation which only skipped string literals.
 */
public abstract class AbstractSqlDialect implements SqlDialect {

    /**
     * Default: dialects using native {@code ?} placeholders return the SQL unchanged.
     * Subclasses with positional/named parameters override and call {@link #rewrite}.
     */
    @Override
    public String rewritePlaceholders(String sql) {
        return sql;
    }

    /**
     * Scan {@code sql} and replace each real {@code ?} placeholder with
     * {@code replacement.apply(paramIndex)} where {@code paramIndex} is 1-based.
     */
    protected String rewrite(String sql, IntFunction<String> replacement) {
        if (sql == null || sql.indexOf('?') < 0) {
            return sql;
        }
        StringBuilder out = new StringBuilder(sql.length() + 16);
        int paramIndex = 0;
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = copyStringLiteral(sql, i, n, out);
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i = copyLineComment(sql, i, n, out);
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i = copyBlockComment(sql, i, n, out);
            } else if (c == '?') {
                out.append(replacement.apply(++paramIndex));
                i++;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** Copy a single-quoted literal starting at {@code i} (the opening quote); returns new index. */
    private int copyStringLiteral(String sql, int i, int n, StringBuilder out) {
        out.append('\'');
        i++;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // '' inside a literal is an escaped quote, not the terminator
                if (i + 1 < n && sql.charAt(i + 1) == '\'') {
                    out.append("''");
                    i += 2;
                    continue;
                }
                out.append('\'');
                i++;
                break;
            }
            out.append(c);
            i++;
        }
        return i;
    }

    private int copyLineComment(String sql, int i, int n, StringBuilder out) {
        while (i < n && sql.charAt(i) != '\n') {
            out.append(sql.charAt(i));
            i++;
        }
        return i;
    }

    private int copyBlockComment(String sql, int i, int n, StringBuilder out) {
        out.append("/*");
        i += 2;
        while (i < n) {
            if (sql.charAt(i) == '*' && i + 1 < n && sql.charAt(i + 1) == '/') {
                out.append("*/");
                i += 2;
                break;
            }
            out.append(sql.charAt(i));
            i++;
        }
        return i;
    }

    /** True when {@code rowBounds} requests no paging at all. */
    protected boolean isNoPaging(RowBounds rowBounds) {
        return rowBounds == null
                || (rowBounds.getOffset() <= 0 && rowBounds.getLimit() >= RowBounds.NO_ROW_LIMIT);
    }

    /** {@code LIMIT n OFFSET m} — MySQL / PostgreSQL style. */
    protected String limitOffset(String sql, RowBounds rowBounds) {
        if (isNoPaging(rowBounds)) {
            return sql;
        }
        StringBuilder sb = new StringBuilder(sql);
        if (rowBounds.getLimit() < RowBounds.NO_ROW_LIMIT) {
            sb.append(" LIMIT ").append(rowBounds.getLimit());
        }
        if (rowBounds.getOffset() > 0) {
            sb.append(" OFFSET ").append(rowBounds.getOffset());
        }
        return sb.toString();
    }

    /** {@code OFFSET m ROWS FETCH NEXT n ROWS ONLY} — SQL:2008 standard (MSSQL / DB2). */
    protected String offsetFetch(String sql, RowBounds rowBounds) {
        if (isNoPaging(rowBounds)) {
            return sql;
        }
        int offset = rowBounds.getOffset();
        int limit = rowBounds.getLimit();
        StringBuilder sb = new StringBuilder(sql);
        if (offset <= 0) {
            // No OFFSET clause; FETCH FIRST is valid standalone in DB2
            sb.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY");
            return sb.toString();
        }
        sb.append(" OFFSET ").append(offset).append(" ROWS");
        if (limit < RowBounds.NO_ROW_LIMIT) {
            sb.append(" FETCH NEXT ").append(limit).append(" ROWS ONLY");
        }
        return sb.toString();
    }

    /**
     * MSSQL requires an {@code ORDER BY} before {@code OFFSET/FETCH}. Append a stable
     * no-op ordering when the SQL has none.
     */
    protected String ensureOrderBy(String sql) {
        if (containsOrderBy(sql)) {
            return sql;
        }
        return sql + " ORDER BY (SELECT NULL)";
    }

    private boolean containsOrderBy(String sql) {
        // Case-insensitive search for a top-level "order by" token.
        String lower = sql.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("order by");
    }
}
