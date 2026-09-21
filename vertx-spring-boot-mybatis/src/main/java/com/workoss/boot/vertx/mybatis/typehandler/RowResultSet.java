package com.workoss.boot.vertx.mybatis.typehandler;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Row;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;

/**
 * Read-side JDBC adapter: a {@link ResultSet} dynamic proxy over a vertx-sql-client {@link Row},
 * so MyBatis {@link org.apache.ibatis.type.TypeHandler}s can read columns via the standard
 * {@code rs.getXxx(...)} surface.
 * <p>
 * Column access by name and by index are both supported, {@code wasNull()} tracks the last read,
 * and {@code java.time} values are converted to their {@code java.sql} equivalents. Streams and
 * LOBs throw {@link UnsupportedTypeHandlerAccessException} so the caller can fall back.
 */
final class RowResultSet {

    private RowResultSet() {
    }

    static ResultSet wrap(Row row) {
        return (ResultSet) Proxy.newProxyInstance(
                RowResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new Handler(row));
    }

    private static final class Handler implements InvocationHandler {

        private final Row row;
        private boolean lastWasNull;

        Handler(Row row) {
            this.row = row;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            switch (name) {
                case "toString":
                    return "RowResultSet(row)";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == (args == null ? null : args[0]);
                case "wasNull":
                    return lastWasNull;
                case "findColumn":
                    return row.getColumnIndex((String) arg(args, 0));
                case "close":
                case "clearWarnings":
                    return null;
                default:
                    break;
            }
            if (args == null || args.length < 1) {
                throw new UnsupportedTypeHandlerAccessException("Unsupported ResultSet call: " + name);
            }

            switch (name) {
                case "getObject" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    if (args.length >= 2 && args[1] instanceof Class<?> target) {
                        Object conv = convertTo(raw, target);
                        lastWasNull = conv == null;
                        return conv;
                    }
                    return raw;
                }
                case "getString", "getNString" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    return raw == null ? null : raw.toString();
                }
                case "getBoolean" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    Boolean b = asBoolean(raw);
                    return b != null && b;
                }
                case "getByte" -> {
                    Number n = number(args);
                    return n == null ? (byte) 0 : n.byteValue();
                }
                case "getShort" -> {
                    Number n = number(args);
                    return n == null ? (short) 0 : n.shortValue();
                }
                case "getInt" -> {
                    Number n = number(args);
                    return n == null ? 0 : n.intValue();
                }
                case "getLong" -> {
                    Number n = number(args);
                    return n == null ? 0L : n.longValue();
                }
                case "getFloat" -> {
                    Number n = number(args);
                    return n == null ? 0f : n.floatValue();
                }
                case "getDouble" -> {
                    Number n = number(args);
                    return n == null ? 0d : n.doubleValue();
                }
                case "getBigDecimal" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    return asBigDecimal(raw);
                }
                case "getBytes" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    return asBytes(raw);
                }
                case "getDate" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    return asSqlDate(raw);
                }
                case "getTime" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    return asSqlTime(raw);
                }
                case "getTimestamp" -> {
                    Object raw = rawOf(args);
                    lastWasNull = raw == null;
                    return asSqlTimestamp(raw);
                }
                default -> throw new UnsupportedTypeHandlerAccessException(
                        "Unsupported ResultSet getter: " + name);
            }
        }

        private Object rawOf(Object[] args) {
            Object key = args[0];
            if (key instanceof String s) {
                return row.getValue(s);
            }
            if (key instanceof Integer i) {
                return row.getValue(i);
            }
            throw new UnsupportedTypeHandlerAccessException("Bad column key: " + key);
        }

        private Number number(Object[] args) {
            Object raw = rawOf(args);
            lastWasNull = raw == null;
            if (raw instanceof Number n) {
                return n;
            }
            if (raw instanceof BigDecimal bd) {
                return bd;
            }
            if (raw instanceof String s) {
                try {
                    return new BigDecimal(s);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            return null;
        }
    }

    private static Object arg(Object[] args, int i) {
        return args != null && args.length > i ? args[i] : null;
    }

    private static Boolean asBoolean(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof Boolean b) {
            return b;
        }
        if (raw instanceof Number n) {
            return n.intValue() != 0;
        }
        return Boolean.parseBoolean(raw.toString());
    }

    private static BigDecimal asBigDecimal(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof BigDecimal bd) {
            return bd;
        }
        if (raw instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        return new BigDecimal(raw.toString());
    }

    private static byte[] asBytes(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof byte[] b) {
            return b;
        }
        if (raw instanceof Buffer buf) {
            return buf.getBytes();
        }
        throw new UnsupportedTypeHandlerAccessException("Cannot read bytes from: " + raw.getClass());
    }

    private static java.sql.Date asSqlDate(Object raw) {
        LocalDate ld = toLocalDate(raw);
        return ld == null ? null : java.sql.Date.valueOf(ld);
    }

    private static java.sql.Time asSqlTime(Object raw) {
        LocalTime lt = toLocalTime(raw);
        return lt == null ? null : java.sql.Time.valueOf(lt);
    }

    private static java.sql.Timestamp asSqlTimestamp(Object raw) {
        LocalDateTime ldt = toLocalDateTime(raw);
        return ldt == null ? null : java.sql.Timestamp.valueOf(ldt);
    }

    private static LocalDate toLocalDate(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof LocalDate ld) {
            return ld;
        }
        if (raw instanceof LocalDateTime ldt) {
            return ldt.toLocalDate();
        }
        if (raw instanceof OffsetDateTime odt) {
            return odt.toLocalDate();
        }
        return null;
    }

    private static LocalTime toLocalTime(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof LocalTime lt) {
            return lt;
        }
        if (raw instanceof OffsetTime ot) {
            return ot.toLocalTime();
        }
        if (raw instanceof LocalDateTime ldt) {
            return ldt.toLocalTime();
        }
        return null;
    }

    private static LocalDateTime toLocalDateTime(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof LocalDateTime ldt) {
            return ldt;
        }
        if (raw instanceof OffsetDateTime odt) {
            return odt.toLocalDateTime();
        }
        if (raw instanceof LocalDate ld) {
            return ld.atStartOfDay();
        }
        return null;
    }

    /** Convert a raw column value to the requested target type for {@code getObject(col, Class)}. */
    private static Object convertTo(Object raw, Class<?> target) {
        if (raw == null) {
            return null;
        }
        if (target.isInstance(raw)) {
            return raw;
        }
        if (target == String.class) {
            return raw.toString();
        }
        if (target == LocalDate.class) {
            return toLocalDate(raw);
        }
        if (target == LocalTime.class) {
            return toLocalTime(raw);
        }
        if (target == LocalDateTime.class) {
            return toLocalDateTime(raw);
        }
        if (target == OffsetDateTime.class && raw instanceof OffsetDateTime) {
            return raw;
        }
        if (target == UUID.class && raw instanceof UUID) {
            return raw;
        }
        if (target == byte[].class) {
            return asBytes(raw);
        }
        if (target == BigDecimal.class) {
            return asBigDecimal(raw);
        }
        if (target == Boolean.class || target == boolean.class) {
            return asBoolean(raw);
        }
        if (raw instanceof Number n) {
            if (target == Integer.class || target == int.class) return n.intValue();
            if (target == Long.class || target == long.class) return n.longValue();
            if (target == Short.class || target == short.class) return n.shortValue();
            if (target == Byte.class || target == byte.class) return n.byteValue();
            if (target == Float.class || target == float.class) return n.floatValue();
            if (target == Double.class || target == double.class) return n.doubleValue();
        }
        if (target == java.sql.Date.class) {
            return asSqlDate(raw);
        }
        if (target == java.sql.Time.class) {
            return asSqlTime(raw);
        }
        if (target == java.sql.Timestamp.class) {
            return asSqlTimestamp(raw);
        }
        return raw;
    }
}
