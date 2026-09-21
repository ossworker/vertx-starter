package com.workoss.boot.vertx.mybatis.typehandler;

/**
 * Thrown internally when a JDBC {@link org.apache.ibatis.type.TypeHandler} touches a part of the
 * {@link java.sql.PreparedStatement}/{@link java.sql.ResultSet} surface that the vertx adapters
 * cannot fake (streams, LOBs, OUT parameters, {@link java.sql.CallableStatement}, execution).
 * <p>
 * The {@link VertxTypeHandlerBridge} catches this and falls back to the legacy value conversion.
 * It never escapes to user code.
 */
public class UnsupportedTypeHandlerAccessException extends RuntimeException {

    public UnsupportedTypeHandlerAccessException(String message) {
        super(message);
    }

    public UnsupportedTypeHandlerAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
