package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.sqlclient.DatabaseException;

/** Small PostgreSQL / MySQL (including PolarDB-X) SQL syntax boundary. */
public final class SqlDialect {
    private final boolean postgres;

    public SqlDialect(boolean postgres) {
        this.postgres = postgres;
    }

    /** Only static SQL templates pass through here, never user input. */
    public String parameters(String template) {
        if (!postgres) {
            return template;
        }
        StringBuilder sql = new StringBuilder();
        int parameter = 0;
        for (char character : template.toCharArray()) {
            if (character == '?') {
                sql.append('$').append(++parameter);
            } else {
                sql.append(character);
            }
        }
        return sql.toString();
    }

    public String textType() {
        return postgres ? "TEXT" : "MEDIUMTEXT";
    }

    public String idType(int length) {
        return "VARCHAR(" + length + ")"
                + (postgres ? "" : " CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
    }

    public boolean isDuplicate(Throwable failure) {
        return failure instanceof DatabaseException database
                && (postgres ? "23505".equals(database.getSqlState()) : database.getErrorCode() == 1062);
    }

    public static String requireId(String value, String name, int maxLength) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(name + " must contain 1.." + maxLength + " characters");
        }
        return value;
    }

    public static int requireLimit(int limit) {
        if (limit < 1 || limit > 200) {
            throw new IllegalArgumentException("limit must be in 1..200");
        }
        return limit;
    }

    static int requireTimelineLimit(int limit) {
        if (limit < 1 || limit > 201) {
            throw new IllegalArgumentException("timeline limit must be in 1..201");
        }
        return limit;
    }

    static String requireTimelineKind(String kind) {
        if (!"message".equals(kind) && !"session".equals(kind)) {
            throw new IllegalArgumentException("invalid timeline kind");
        }
        return kind;
    }
}
