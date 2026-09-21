package com.workoss.boot.vertx.mybatis.typehandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Write-side JDBC adapter: a {@link PreparedStatement} dynamic proxy that captures the values a
 * MyBatis {@link org.apache.ibatis.type.TypeHandler} sets, instead of sending them to a database.
 * <p>
 * Only {@code setXxx(int, ...)} parameter setters are honoured. {@code java.sql} temporal types are
 * converted to their {@code java.time} equivalents (what vertx-sql-client {@code Tuple} expects).
 * Any unsupported call (streams, LOBs, arrays, execution, metadata) throws
 * {@link UnsupportedTypeHandlerAccessException} so the caller can fall back.
 */
final class TuplePreparedStatement {

    private TuplePreparedStatement() {
    }

    /** Records the values captured from {@code setXxx} calls, keyed by 1-based parameter index. */
    static final class ValueSink {
        private final Map<Integer, Object> values = new HashMap<>(4);

        void put(int index, Object value) {
            values.put(index, value);
        }

        boolean has(int index) {
            return values.containsKey(index);
        }

        Object get(int index) {
            return values.get(index);
        }

        /** The captured value at index 1, or the single captured value if only one was set. */
        Object single() {
            if (values.containsKey(1)) {
                return values.get(1);
            }
            if (values.size() == 1) {
                return values.values().iterator().next();
            }
            return null;
        }
    }

    /**
     * Create a capturing {@link PreparedStatement} proxy.
     *
     * @param sink receives the values set by the TypeHandler
     * @return a proxy whose setters record into {@code sink}
     */
    static PreparedStatement wrap(ValueSink sink) {
        return (PreparedStatement) Proxy.newProxyInstance(
                TuplePreparedStatement.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                new Handler(sink));
    }

    private static final class Handler implements InvocationHandler {

        private final ValueSink sink;

        Handler(ValueSink sink) {
            this.sink = sink;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            switch (name) {
                case "toString":
                    return "TuplePreparedStatement(capturing)";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == (args == null ? null : args[0]);
                default:
                    break;
            }

            // All supported setters take (int parameterIndex, ...) as the first two args.
            if (args == null || args.length < 2 || !(args[0] instanceof Integer index)) {
                throw new UnsupportedTypeHandlerAccessException(
                        "Unsupported PreparedStatement call: " + name);
            }

            switch (name) {
                case "setNull" -> sink.put(index, null);
                case "setBoolean" -> sink.put(index, args[1]);
                case "setByte" -> sink.put(index, toInt(args[1]));
                case "setShort" -> sink.put(index, args[1]);
                case "setInt" -> sink.put(index, args[1]);
                case "setLong" -> sink.put(index, args[1]);
                case "setFloat" -> sink.put(index, args[1]);
                case "setDouble" -> sink.put(index, args[1]);
                case "setBigDecimal" -> sink.put(index, args[1]);
                case "setString", "setNString" -> sink.put(index, args[1]);
                case "setBytes" -> sink.put(index, args[1]);
                case "setDate" -> sink.put(index, toLocalDate(args[1]));
                case "setTime" -> sink.put(index, toLocalTime(args[1]));
                case "setTimestamp" -> sink.put(index, toLocalDateTime(args[1]));
                case "setObject" -> sink.put(index, normalizeObject(args[1]));
                default -> throw new UnsupportedTypeHandlerAccessException(
                        "Unsupported PreparedStatement setter: " + name);
            }
            return null;
        }

        private static Object toInt(Object v) {
            return v instanceof Byte b ? b.intValue() : v;
        }

        private static Object normalizeObject(Object v) {
            if (v instanceof Timestamp ts) {
                return ts.toLocalDateTime();
            }
            if (v instanceof Date d) {
                return d.toLocalDate();
            }
            if (v instanceof Time t) {
                return t.toLocalTime();
            }
            if (v instanceof BigDecimal || v instanceof LocalDate
                    || v instanceof LocalTime || v instanceof LocalDateTime) {
                return v;
            }
            return v;
        }

        private static Object toLocalDate(Object v) {
            if (v instanceof Date d) {
                return d.toLocalDate();
            }
            if (v instanceof Timestamp ts) {
                return ts.toLocalDateTime().toLocalDate();
            }
            if (v instanceof LocalDate ld) {
                return ld;
            }
            return v;
        }

        private static Object toLocalTime(Object v) {
            if (v instanceof Time t) {
                return t.toLocalTime();
            }
            if (v instanceof LocalTime lt) {
                return lt;
            }
            return v;
        }

        private static Object toLocalDateTime(Object v) {
            if (v instanceof Timestamp ts) {
                return ts.toLocalDateTime();
            }
            if (v instanceof LocalDateTime ldt) {
                return ldt;
            }
            if (v instanceof Date d) {
                return d.toLocalDate().atStartOfDay();
            }
            return v;
        }
    }
}
