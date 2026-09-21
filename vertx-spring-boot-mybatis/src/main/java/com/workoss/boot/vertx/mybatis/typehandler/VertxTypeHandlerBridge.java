package com.workoss.boot.vertx.mybatis.typehandler;

import io.vertx.sqlclient.Row;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges JDBC-shaped MyBatis {@link TypeHandler}s onto vertx-sql-client.
 * <p>
 * Write side: runs the handler against a capturing {@link java.sql.PreparedStatement} proxy
 * ({@link TuplePreparedStatement}) and returns the value it set. Read side: runs the handler
 * against a {@link java.sql.ResultSet} proxy over the {@link Row} ({@link RowResultSet}).
 * <p>
 * Handlers that touch an unsupported part of the JDBC surface (streams, LOBs, OUT params,
 * {@code CallableStatement}) — or that fail for any reason — yield {@link #FALLBACK}, signalling
 * the caller to use its legacy conversion instead. This bridge never throws to user code.
 * <p>
 * Thread-safe and stateless apart from logging; a single instance is shared.
 */
public final class VertxTypeHandlerBridge {

    private static final Logger logger = LoggerFactory.getLogger(VertxTypeHandlerBridge.class);

    /**
     * Sentinel returned by {@link #tryWrite} and {@link #tryRead} when the handler cannot be
     * adapted; the caller must fall back to its own conversion.
     */
    public static final Object FALLBACK = new Object();

    /**
     * Run a handler's {@code setParameter} against a capturing proxy and return the bound value.
     *
     * @return the Tuple-compatible value, or {@link #FALLBACK} if unsupported/failed
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Object tryWrite(TypeHandler<?> handler, Object value, JdbcType jdbcType) {
        if (handler == null) {
            return FALLBACK;
        }
        try {
            TuplePreparedStatement.ValueSink sink = new TuplePreparedStatement.ValueSink();
            TypeHandler raw = handler;
            raw.setParameter(TuplePreparedStatement.wrap(sink), 1, value, jdbcType);
            if (!sink.has(1) && sink.single() == null) {
                // Handler set nothing usable (e.g. it only set a null we can't distinguish).
                return sink.has(1) ? null : FALLBACK;
            }
            return sink.single();
        } catch (UnsupportedTypeHandlerAccessException e) {
            logger.debug("TypeHandler {} not adaptable for write, falling back: {}",
                    handler.getClass().getSimpleName(), e.getMessage());
            return FALLBACK;
        } catch (Exception e) {
            logger.debug("TypeHandler {} failed on write, falling back", handler.getClass().getSimpleName(), e);
            return FALLBACK;
        }
    }

    /**
     * Run a handler's {@code getResult} against a {@link Row}-backed proxy.
     *
     * @return the converted value, or {@link #FALLBACK} if unsupported/failed
     */
    public Object tryRead(TypeHandler<?> handler, Row row, String columnName) {
        if (handler == null || row == null) {
            return FALLBACK;
        }
        try {
            return handler.getResult(RowResultSet.wrap(row), columnName);
        } catch (UnsupportedTypeHandlerAccessException e) {
            logger.debug("TypeHandler {} not adaptable for read, falling back: {}",
                    handler.getClass().getSimpleName(), e.getMessage());
            return FALLBACK;
        } catch (Exception e) {
            logger.debug("TypeHandler {} failed on read, falling back", handler.getClass().getSimpleName(), e);
            return FALLBACK;
        }
    }
}
