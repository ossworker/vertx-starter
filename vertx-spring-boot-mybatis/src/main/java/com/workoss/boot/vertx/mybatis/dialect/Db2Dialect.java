package com.workoss.boot.vertx.mybatis.dialect;

import org.apache.ibatis.session.RowBounds;

/** DB2 dialect: native {@code ?} placeholders, {@code OFFSET/FETCH} paging. */
public class Db2Dialect extends AbstractSqlDialect {

    @Override
    public String name() {
        return "DB2";
    }

    @Override
    public String applyPagination(String sql, RowBounds rowBounds) {
        return offsetFetch(sql, rowBounds);
    }
}
