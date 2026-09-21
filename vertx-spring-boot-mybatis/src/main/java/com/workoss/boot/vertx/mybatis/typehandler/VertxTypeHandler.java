package com.workoss.boot.vertx.mybatis.typehandler;

/**
 * Native vertx-mybatis type handler SPI.
 * <p>
 * MyBatis {@link org.apache.ibatis.type.TypeHandler} is JDBC-shaped (it reads/writes
 * through {@link java.sql.PreparedStatement}/{@link java.sql.ResultSet}), which vertx-sql-client
 * does not provide. The {@link VertxTypeHandlerBridge} adapts most JDBC handlers via dynamic
 * proxies, but handlers that touch streams, LOBs, {@code OUT} parameters or
 * {@link java.sql.CallableStatement} cannot be adapted.
 * <p>
 * For those — and for maximum efficiency — implement this SPI instead. It converts directly
 * between the Java value and a vertx-sql-client {@code Tuple}-compatible value with no proxy
 * overhead. When a handler implements both this interface and {@code TypeHandler}, this SPI
 * takes precedence.
 *
 * @param <T> the Java property type
 */
public interface VertxTypeHandler<T> {

    /**
     * Convert a Java property value into a value accepted by {@link io.vertx.sqlclient.Tuple}.
     *
     * @param value the property value (may be {@code null})
     * @return a Tuple-compatible value
     */
    Object toSqlValue(T value);

    /**
     * Convert a raw column value produced by vertx-sql-client into the Java property type.
     *
     * @param rawColumnValue the value from {@link io.vertx.sqlclient.Row#getValue(String)}
     * @return the converted property value
     */
    T fromSqlValue(Object rawColumnValue);
}
