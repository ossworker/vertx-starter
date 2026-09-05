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
     * SQL dialect for placeholder conversion.
     * Supported: MYSQL, POSTGRESQL, MSSQL, DB2, AUTO.
     * AUTO detects the dialect from the SQL client type.
     */
    private String dialect = "AUTO";

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }
}
