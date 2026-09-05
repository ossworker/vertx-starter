package com.workoss.boot.vertx.mybatis.core;

import io.vertx.sqlclient.Tuple;
import org.apache.ibatis.mapping.SqlCommandType;

/**
 * Holds the resolved SQL string, parameter Tuple, and command type
 * ready for execution via vertx-sql-client.
 */
public record SqlAndParams(String sql, Tuple tuple, SqlCommandType commandType) {
}
