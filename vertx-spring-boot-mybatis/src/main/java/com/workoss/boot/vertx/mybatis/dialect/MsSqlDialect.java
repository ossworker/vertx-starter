package com.workoss.boot.vertx.mybatis.dialect;

import org.apache.ibatis.session.RowBounds;

/**
 * Microsoft SQL Server dialect: named {@code @pn} placeholders,
 * {@code OFFSET/FETCH} paging (which requires an {@code ORDER BY}).
 */
public class MsSqlDialect extends AbstractSqlDialect {

    @Override
    public String name() {
        return "MSSQL";
    }

    @Override
    public String rewritePlaceholders(String sql) {
        return rewrite(sql, i -> "@p" + i);
    }

    @Override
    public String applyPagination(String sql, RowBounds rowBounds) {
        if (isNoPaging(rowBounds)) {
            return sql;
        }
        return offsetFetch(ensureOrderBy(sql), rowBounds);
    }
}
