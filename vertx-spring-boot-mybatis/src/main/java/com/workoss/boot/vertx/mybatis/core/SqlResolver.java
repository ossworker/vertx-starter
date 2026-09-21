package com.workoss.boot.vertx.mybatis.core;

import com.workoss.boot.vertx.mybatis.dialect.DialectResolver;
import com.workoss.boot.vertx.mybatis.dialect.SqlDialect;
import com.workoss.boot.vertx.mybatis.typehandler.VertxTypeHandler;
import com.workoss.boot.vertx.mybatis.typehandler.VertxTypeHandlerBridge;
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
     *
     * @deprecated use the {@link SqlDialect} SPI ({@link #setSqlDialect(SqlDialect)}).
     *             Retained as a compatibility shim; each constant maps to a built-in dialect.
     */
    @Deprecated
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
    /** Deprecated enum view, kept in sync with {@link #sqlDialect} for the legacy API. */
    private volatile Dialect dialect = Dialect.AUTO;
    /** Authoritative dialect; {@code null} means "no placeholder rewriting" (AUTO/MySQL). */
    private volatile SqlDialect sqlDialect;
    /** Bridges JDBC-shaped MyBatis TypeHandlers onto vertx-sql-client values. */
    private volatile VertxTypeHandlerBridge typeHandlerBridge = new VertxTypeHandlerBridge();

    public SqlResolver(Configuration configuration) {
        this.configuration = configuration;
    }

    public SqlResolver(Configuration configuration, Dialect dialect) {
        this.configuration = configuration;
        setDialect(dialect);
    }

    public SqlResolver(Configuration configuration, SqlDialect sqlDialect) {
        this.configuration = configuration;
        setSqlDialect(sqlDialect);
    }

    /**
     * @deprecated use {@link #setSqlDialect(SqlDialect)}.
     */
    @Deprecated
    public void setDialect(Dialect dialect) {
        this.dialect = dialect != null ? dialect : Dialect.AUTO;
        this.sqlDialect = toSqlDialect(this.dialect);
    }

    /**
     * @deprecated use {@link #getSqlDialect()}.
     */
    @Deprecated
    public Dialect getDialect() {
        return dialect;
    }

    /** Set the authoritative dialect SPI. Also updates the deprecated enum view best-effort. */
    public void setSqlDialect(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
        if (sqlDialect != null) {
            this.dialect = fromSqlDialect(sqlDialect);
        }
    }

    /** The authoritative dialect; may be {@code null} (no placeholder rewriting). */
    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    private static SqlDialect toSqlDialect(Dialect d) {
        return switch (d) {
            case MYSQL -> DialectResolver.byName("MYSQL");
            case POSTGRESQL -> DialectResolver.byName("POSTGRESQL");
            case MSSQL -> DialectResolver.byName("MSSQL");
            case DB2 -> DialectResolver.byName("DB2");
            case AUTO -> null;
        };
    }

    private static Dialect fromSqlDialect(SqlDialect sqlDialect) {
        Dialect resolved = DialectResolver.byName(sqlDialect.name()) != null
                ? switch (sqlDialect.name().toUpperCase(java.util.Locale.ROOT)) {
            case "POSTGRESQL" -> Dialect.POSTGRESQL;
            case "MSSQL" -> Dialect.MSSQL;
            case "DB2" -> Dialect.DB2;
            default -> Dialect.MYSQL;
        }
                : Dialect.MYSQL;
        return resolved;
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
                tuple.addValue(bindValue(mapping, value));
            }
        }

        // Convert placeholders based on dialect
        SqlDialect d = this.sqlDialect;
        String resolvedSql = d != null ? d.rewritePlaceholders(sql) : sql;

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
     * Bind a parameter value, honouring the MyBatis {@link TypeHandler} configured on the mapping.
     * <p>
     * Precedence: native {@link VertxTypeHandler} SPI → JDBC {@link TypeHandler} adapted through
     * {@link VertxTypeHandlerBridge} → {@link #convertToSqlValue legacy conversion} fallback.
     */
    @SuppressWarnings("unchecked")
    private Object bindValue(ParameterMapping mapping, Object value) {
        if (value == null) {
            return null;
        }
        TypeHandler<?> handler = mapping.getTypeHandler();
        if (handler instanceof VertxTypeHandler) {
            return ((VertxTypeHandler<Object>) handler).toSqlValue(value);
        }
        if (handler != null) {
            Object converted = typeHandlerBridge.tryWrite(handler, value, mapping.getJdbcType());
            if (converted != VertxTypeHandlerBridge.FALLBACK) {
                return converted;
            }
        }
        return convertToSqlValue(value, mapping);
    }

    public VertxTypeHandlerBridge getTypeHandlerBridge() {
        return typeHandlerBridge;
    }

    public void setTypeHandlerBridge(VertxTypeHandlerBridge typeHandlerBridge) {
        this.typeHandlerBridge = typeHandlerBridge != null
                ? typeHandlerBridge : new VertxTypeHandlerBridge();
    }

    /**
     * Legacy value conversion, used as the fallback when no TypeHandler applies or the handler
     * cannot be adapted to vertx-sql-client.
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
}
