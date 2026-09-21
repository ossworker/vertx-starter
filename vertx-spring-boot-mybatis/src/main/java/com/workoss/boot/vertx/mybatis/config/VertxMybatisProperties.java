package com.workoss.boot.vertx.mybatis.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Vert.x-specific MyBatis configuration properties.
 * <p>
 * Standard MyBatis properties are read from {@code mybatis.*} via
 * {@link org.mybatis.spring.boot.autoconfigure.MybatisProperties} (from mybatis-spring-boot-autoconfigure).
 * <p>
 * This class only holds properties unique to the Vert.x SQL Client integration.
 */
@ConfigurationProperties(prefix = "vertx.mybatis")
public class VertxMybatisProperties {

    /**
     * SQL dialect for placeholder conversion and pagination.
     * Supported: MYSQL, POSTGRESQL, MSSQL, DB2, AUTO.
     * AUTO detects the dialect from the SQL client (Pool) type.
     */
    private String dialect = "AUTO";

    /**
     * Whether to register the fully reactive {@code VertxReactiveTransactionManager}
     * (requires {@code spring-tx}'s reactive API and {@code reactor-core} on the classpath).
     * <p>
     * When {@code true} (default), a {@code ReactiveTransactionManager} bean is created if none is
     * present, enabling reactive {@code @Transactional} methods whose transactional connection is
     * also visible to {@code Future}-returning mapper methods running on the Vert.x event loop.
     * The blocking {@code VertxDataSourceTransactionManager} is registered independently.
     */
    private boolean reactiveTransactionsEnabled = true;

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public boolean isReactiveTransactionsEnabled() {
        return reactiveTransactionsEnabled;
    }

    public void setReactiveTransactionsEnabled(boolean reactiveTransactionsEnabled) {
        this.reactiveTransactionsEnabled = reactiveTransactionsEnabled;
    }
}
