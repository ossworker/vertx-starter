package com.workoss.boot.vertx.mybatis.dialect;

import org.apache.ibatis.session.RowBounds;

/** MySQL / MariaDB dialect: native {@code ?} placeholders, {@code LIMIT/OFFSET} paging. */
public class MySqlDialect extends AbstractSqlDialect {

    @Override
    public String name() {
        return "MYSQL";
    }

    @Override
    public String applyPagination(String sql, RowBounds rowBounds) {
        return limitOffset(sql, rowBounds);
    }
}
