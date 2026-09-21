package com.workoss.boot.vertx.mybatis.dialect;

import org.apache.ibatis.session.RowBounds;

/** PostgreSQL dialect: positional {@code $n} placeholders, {@code LIMIT/OFFSET} paging. */
public class PostgreSqlDialect extends AbstractSqlDialect {

    @Override
    public String name() {
        return "POSTGRESQL";
    }

    @Override
    public String rewritePlaceholders(String sql) {
        return rewrite(sql, i -> "$" + i);
    }

    @Override
    public String applyPagination(String sql, RowBounds rowBounds) {
        return limitOffset(sql, rowBounds);
    }
}
