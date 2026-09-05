package com.workoss.boot.vertx.mybatis.core;

import io.vertx.sqlclient.Tuple;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves MyBatis {@link BoundSql} into an executable SQL string and
 * {@link Tuple} parameter set for vertx-sql-client.
 * <p>
 * Handles:
 * <ul>
 *   <li>Parameter extraction via {@link ParameterMapping} and {@link MetaObject}</li>
 *   <li>Additional parameters from dynamic SQL (foreach, if/choose etc.)</li>
 *   <li>Placeholder conversion for different database dialects (MySQL ?, PostgreSQL $n, MSSQL @pn)</li>
 *   <li>Enum / byte[] / temporal type conversion for Tuple compatibility</li>
 * </ul>
 */
public class SqlResolver {

    private static final Logger logger = LoggerFactory.getLogger(SqlResolver.class);

    /**
     * Database dialect controlling placeholder style.
     */
    public enum Dialect {
        /** MySQL / MariaDB / DB2: keep {@code ?} */
        MYSQL,
        /** PostgreSQL: convert to {@code $1, $2, ...} */
        POSTGRESQL,
        /** Microsoft SQL Server: convert to {@code @p1, @p2, ...} */
        MSSQL,
        /** DB2: keep {@code ?} (same as MySQL) */
        DB2,
        /** Auto-detect or fallback to MySQL style */
        AUTO
    }

    private final Configuration configuration;
    private volatile Dialect dialect = Dialect.AUTO;

    public SqlResolver(Configuration configuration) {
        this.configuration = configuration;
    }

    public SqlResolver(Configuration configuration, Dialect dialect) {
        this.configuration = configuration;
        this.dialect = dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Dialect getDialect() {
        return dialect;
    }

    /**
     * Resolve a {@link MappedStatement} with the given parameter object
     * into an executable SQL + Tuple pair.
     */
    public SqlAndParams resolve(MappedStatement ms, Object parameter) {
        BoundSql boundSql = ms.getBoundSql(parameter);
        String sql = boundSql.getSql();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();

        Tuple tuple = Tuple.tuple();

        if (parameterMappings != null && !parameterMappings.isEmpty()) {
            for (ParameterMapping mapping : parameterMappings) {
                if (mapping.getMode() == ParameterMode.OUT) {
                    continue;
                }

                Object value = resolveParameterValue(mapping, boundSql, parameter);
                tuple.addValue(convertToSqlValue(value, mapping));
            }
        }

        // Convert placeholders based on dialect
        String resolvedSql = convertPlaceholders(sql);

        return new SqlAndParams(resolvedSql, tuple, ms.getSqlCommandType());
    }

    /**
     * Extract the value for a single parameter mapping.
     */
    private Object resolveParameterValue(ParameterMapping mapping, BoundSql boundSql, Object parameter) {
        String propertyName = mapping.getProperty();

        // 1. Check additional parameters first (dynamic SQL generated params, e.g. foreach)
        if (boundSql.hasAdditionalParameter(propertyName)) {
            return boundSql.getAdditionalParameter(propertyName);
        }

        if (parameter == null) {
            return null;
        }

        // 2. If the parameter itself is a simple type handled by TypeHandler
        if (configuration.getTypeHandlerRegistry().hasTypeHandler(parameter.getClass())) {
            return parameter;
        }

        // 3. Reflect via MetaObject for nested properties
        MetaObject metaObject = configuration.newMetaObject(parameter);
        return metaObject.getValue(propertyName);
    }

    /**
     * Convert a Java value to a vertx-sql-client compatible value.
     * <p>
     * vertx-sql-client Tuple accepts: null, String, Boolean, Short, Integer, Long, Float, Double,
     * BigDecimal, byte[], LocalDate, LocalTime, LocalDateTime, OffsetTime, OffsetDateTime, UUID,
     * JsonObject, JsonArray, Enum, Buffer
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object convertToSqlValue(Object value, ParameterMapping mapping) {
        if (value == null) {
            return null;
        }

        // Enum handling: respect MyBatis TypeHandler configuration
        if (value instanceof Enum<?> enumVal) {
            // Check if EnumOrdinalTypeHandler is configured
            TypeHandler typeHandler = mapping.getTypeHandler();
            if (typeHandler != null) {
                String handlerName = typeHandler.getClass().getSimpleName();
                if (handlerName.contains("Ordinal")) {
                    return enumVal.ordinal();
                }
            }
            // Default: use name()
            return enumVal.name();
        }

        // byte[] → keep as-is (vertx-sql-client handles Buffer conversion)
        if (value instanceof byte[]) {
            return value;
        }

        // Standard types supported by both MyBatis and vertx-sql-client
        if (value instanceof String
                || value instanceof Boolean
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
                || value instanceof BigDecimal
                || value instanceof LocalDate
                || value instanceof LocalTime
                || value instanceof LocalDateTime
                || value instanceof OffsetTime
                || value instanceof OffsetDateTime
                || value instanceof UUID) {
            return value;
        }

        // Character → String
        if (value instanceof Character c) {
            return c.toString();
        }

        // Byte → Integer (vertx-sql-client doesn't accept Byte directly)
        if (value instanceof Byte b) {
            return b.intValue();
        }

        // Fallback: try TypeHandler conversion
        TypeHandler typeHandler = mapping.getTypeHandler();
        if (typeHandler != null) {
            try {
                // TypeHandler is designed for JDBC, but we can try to extract the value
                // For most simple types, the value is already correct
                return value.toString();
            } catch (Exception e) {
                logger.warn("TypeHandler conversion failed for property '{}', using toString()",
                        mapping.getProperty(), e);
                return value.toString();
            }
        }

        // Last resort: toString()
        return value.toString();
    }

    // Pattern to match ? placeholders that are NOT inside string literals
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\?");

    /**
     * Convert MyBatis-style {@code ?} placeholders to the target dialect's format.
     */
    private String convertPlaceholders(String sql) {
        Dialect d = this.dialect;
        if (d == Dialect.AUTO || d == Dialect.MYSQL || d == Dialect.DB2) {
            return sql; // MySQL/DB2 use ? natively
        }

        // Need to replace ? with $n (PostgreSQL) or @pn (MSSQL)
        // But must NOT replace ? inside string literals ('...')
        StringBuilder result = new StringBuilder(sql.length() + 16);
        int paramIndex = 0;
        boolean insideStringLiteral = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\'') {
                insideStringLiteral = !insideStringLiteral;
                result.append(c);
            } else if (c == '?' && !insideStringLiteral) {
                paramIndex++;
                if (d == Dialect.POSTGRESQL) {
                    result.append('$').append(paramIndex);
                } else if (d == Dialect.MSSQL) {
                    result.append("@p").append(paramIndex);
                }
            } else {
                result.append(c);
            }
        }

        return result.toString();
    }
}
