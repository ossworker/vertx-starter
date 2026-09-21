package com.workoss.boot.vertx.mybatis.dialect;

import io.vertx.sqlclient.Pool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Resolves the {@link SqlDialect} to use, from explicit configuration first and
 * otherwise by detecting the concrete vertx-sql-client {@link Pool} type.
 */
public final class DialectResolver {

    private static final Logger logger = LoggerFactory.getLogger(DialectResolver.class);

    private DialectResolver() {
    }

    /**
     * Resolve a dialect.
     *
     * @param configured explicit dialect name (case-insensitive); {@code null}, empty or
     *                   {@code AUTO} triggers detection from {@code pool}
     * @param pool       the vertx-sql-client pool, used for type-based detection
     * @return a shared, immutable dialect instance (never {@code null})
     */
    public static SqlDialect resolve(String configured, Pool pool) {
        if (configured != null && !configured.isBlank()
                && !"AUTO".equalsIgnoreCase(configured.trim())) {
            SqlDialect dialect = byName(configured.trim());
            if (dialect != null) {
                return dialect;
            }
            logger.warn("Unknown vertx.mybatis.dialect '{}', falling back to pool detection", configured);
        }
        SqlDialect detected = detect(pool);
        logger.info("Resolved SQL dialect: {}", detected.name());
        return detected;
    }

    /** Look up a built-in dialect by name; returns {@code null} when unknown. */
    public static SqlDialect byName(String name) {
        if (name == null) {
            return null;
        }
        return switch (name.toUpperCase(Locale.ROOT)) {
            case "MYSQL", "MARIADB" -> new MySqlDialect();
            case "POSTGRESQL", "POSTGRES", "PG" -> new PostgreSqlDialect();
            case "MSSQL", "SQLSERVER" -> new MsSqlDialect();
            case "DB2" -> new Db2Dialect();
            default -> null;
        };
    }

    /** Detect the dialect from the concrete pool implementation class name. */
    private static SqlDialect detect(Pool pool) {
        if (pool == null) {
            return new MySqlDialect();
        }
        String cn = pool.getClass().getName().toLowerCase(Locale.ROOT);
        if (cn.contains("pg") || cn.contains("postgres")) {
            return new PostgreSqlDialect();
        }
        if (cn.contains("mssql") || cn.contains("sqlserver")) {
            return new MsSqlDialect();
        }
        if (cn.contains("db2")) {
            return new Db2Dialect();
        }
        if (cn.contains("mysql") || cn.contains("mariadb")) {
            return new MySqlDialect();
        }
        // Default matches the previous AUTO behaviour.
        return new MySqlDialect();
    }
}
