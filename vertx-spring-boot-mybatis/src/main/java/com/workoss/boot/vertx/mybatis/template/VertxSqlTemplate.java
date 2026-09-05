package com.workoss.boot.vertx.mybatis.template;

import com.workoss.boot.vertx.mybatis.spring.VertxDataSourceTransactionManager;
import io.vertx.core.Future;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * High-level SQL template backed by vertx-sql-client-templates.
 * <p>
 * Provides a convenient, type-safe API for executing SQL with <b>named parameters</b>
 * (e.g. {@code SELECT * FROM users WHERE id = #{id}}), complementing the full MyBatis
 * mapper infrastructure for simpler or ad-hoc queries.
 * <p>
 * Features:
 * <ul>
 *   <li>Named parameters via {@code #{paramName}} syntax</li>
 *   <li>Automatic row mapping via {@code mapTo(Class)} (Jackson-based)</li>
 *   <li>Automatic parameter mapping via {@code mapFrom(Class)} (Jackson-based)</li>
 *   <li>Custom row mapping via {@link io.vertx.sqlclient.templates.RowMapper}</li>
 *   <li>Custom parameter mapping via {@link io.vertx.sqlclient.templates.TupleMapper}</li>
 *   <li>Transaction-aware: respects Spring {@code @Transactional} connections</li>
 *   <li>Both sync (blocking) and async ({@link Future}) execution</li>
 *   <li>Template caching: parsed SQL templates are cached internally by vertx-sql-templates</li>
 * </ul>
 * <p>
 * Usage example:
 * <pre>{@code
 * // Async query with named params
 * Future<RowSet<Row>> future = sqlTemplate.query("SELECT * FROM users WHERE id = #{id}")
 *         .execute(Map.of("id", 42));
 *
 * // Async query with POJO result mapping
 * Future<RowSet<User>> future = sqlTemplate.query("SELECT * FROM users WHERE id = #{id}")
 *         .mapTo(User.class)
 *         .execute(Map.of("id", 42));
 *
 * // Sync update
 * int rows = sqlTemplate.update("UPDATE users SET name = #{name} WHERE id = #{id}")
 *         .executeBlocking(Map.of("name", "Alice", "id", 1));
 * }</pre>
 *
 * @see io.vertx.sqlclient.templates.SqlTemplate
 */
public class VertxSqlTemplate {

    private static final Logger logger = LoggerFactory.getLogger(VertxSqlTemplate.class);

    private final Pool pool;

    /**
     * Cache for parsed query templates keyed by SQL string.
     * Templates are parsed once with the Pool, then {@code withClient()} is used
     * at execution time to switch to a transactional connection if needed.
     */
    private final ConcurrentHashMap<String, SqlTemplate<Map<String, Object>, RowSet<Row>>> queryCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SqlTemplate<Map<String, Object>, SqlResult<Void>>> updateCache = new ConcurrentHashMap<>();

    public VertxSqlTemplate(Pool pool) {
        this.pool = pool;
    }

    private SqlTemplate<Map<String, Object>, RowSet<Row>> getOrCacheQueryTemplate(String sql) {
        return queryCache.computeIfAbsent(sql, s -> SqlTemplate.forQuery(pool, s));
    }

    private SqlTemplate<Map<String, Object>, SqlResult<Void>> getOrCacheUpdateTemplate(String sql) {
        return updateCache.computeIfAbsent(sql, s -> SqlTemplate.forUpdate(pool, s));
    }

    // ===== Query operations =====

    /**
     * Create a query template with named parameters.
     *
     * @param sql SQL template string, e.g. {@code SELECT * FROM users WHERE id = #{id}}
     * @return a {@link QueryTemplate} for further configuration
     */
    public QueryTemplate query(String sql) {
        return new QueryTemplate(sql);
    }

    /**
     * Create an update template with named parameters.
     *
     * @param sql SQL template string, e.g. {@code UPDATE users SET name = #{name} WHERE id = #{id}}
     * @return an {@link UpdateTemplate} for further configuration
     */
    public UpdateTemplate update(String sql) {
        return new UpdateTemplate(sql);
    }

    /**
     * Execute a query directly with named parameters and return rows.
     *
     * @param sql    SQL template with {@code #{param}} placeholders
     * @param params named parameter map
     * @return future of row set
     */
    public Future<RowSet<Row>> query(String sql, Map<String, Object> params) {
        return query(sql).execute(params);
    }

    /**
     * Execute a query and map results to a POJO class.
     *
     * @param sql       SQL template with {@code #{param}} placeholders
     * @param params    named parameter map
     * @param resultType target class for row mapping
     * @param <T>       result type
     * @return future of typed row set
     */
    public <T> Future<RowSet<T>> query(String sql, Map<String, Object> params, Class<T> resultType) {
        return query(sql).mapTo(resultType).execute(params);
    }

    /**
     * Execute an update directly with named parameters.
     *
     * @param sql    SQL template with {@code #{param}} placeholders
     * @param params named parameter map
     * @return future of row count
     */
    public Future<Integer> update(String sql, Map<String, Object> params) {
        return update(sql).execute(params);
    }

    /**
     * Execute a batch query.
     *
     * @param sql   SQL template with {@code #{param}} placeholders
     * @param batch list of parameter maps
     * @return future of row set
     */
    public Future<RowSet<Row>> queryBatch(String sql, List<Map<String, Object>> batch) {
        SqlTemplate<Map<String, Object>, RowSet<Row>> cached = getOrCacheQueryTemplate(sql);
        return cached.withClient(getSqlClient()).executeBatch(batch);
    }

    /**
     * Execute a batch update.
     *
     * @param sql   SQL template with {@code #{param}} placeholders
     * @param batch list of parameter maps
     * @return future of row set (check rowCount)
     */
    public Future<SqlResult<Void>> updateBatch(String sql, List<Map<String, Object>> batch) {
        SqlTemplate<Map<String, Object>, SqlResult<Void>> cached = getOrCacheUpdateTemplate(sql);
        return cached.withClient(getSqlClient()).executeBatch(batch);
    }

    // ===== Template wrapper classes =====

    /**
     * Fluent wrapper for query templates with chaining support for
     * {@code mapTo(Class)}, {@code mapTo(RowMapper)}, and {@code mapFrom(TupleMapper)}.
     */
    public class QueryTemplate {

        private final String sql;
        private SqlTemplate<Map<String, Object>, RowSet<Row>> baseTemplate;

        QueryTemplate(String sql) {
            this.sql = sql;
        }

        /**
         * Map result rows to the specified type using Jackson-based mapping.
         *
         * @param type target class
         * @param <T>  result type
         * @return a typed query template
         */
        public <T> TypedQueryTemplate<T> mapTo(Class<T> type) {
            SqlTemplate<Map<String, Object>, RowSet<T>> template =
                    getOrCacheQueryTemplate(sql).mapTo(type);
            return new TypedQueryTemplate<>(template);
        }

        /**
         * Map result rows using a custom {@link io.vertx.sqlclient.templates.RowMapper}.
         *
         * @param mapper custom row mapper
         * @param <T>    result type
         * @return a typed query template
         */
        public <T> TypedQueryTemplate<T> mapTo(io.vertx.sqlclient.templates.RowMapper<T> mapper) {
            SqlTemplate<Map<String, Object>, RowSet<T>> template =
                    getOrCacheQueryTemplate(sql).mapTo(mapper);
            return new TypedQueryTemplate<>(template);
        }

        /**
         * Execute the query with the given named parameters.
         *
         * @param params parameter map
         * @return future of row set
         */
        public Future<RowSet<Row>> execute(Map<String, Object> params) {
            SqlTemplate<Map<String, Object>, RowSet<Row>> cached = getOrCacheQueryTemplate(sql);
            return cached.withClient(getSqlClient())
                    .execute(params != null ? params : Collections.emptyMap());
        }

        /**
         * Execute the query with no parameters.
         *
         * @return future of row set
         */
        public Future<RowSet<Row>> execute() {
            return execute(Collections.emptyMap());
        }

        /**
         * Execute the query synchronously (blocking).
         *
         * @param params parameter map
         * @return row set
         */
        public RowSet<Row> executeBlocking(Map<String, Object> params) {
            return execute(params).toCompletionStage().toCompletableFuture().join();
        }

        /**
         * Execute the query synchronously with no parameters.
         *
         * @return row set
         */
        public RowSet<Row> executeBlocking() {
            return executeBlocking(Collections.emptyMap());
        }
    }

    /**
     * Typed query template with result mapping already configured.
     *
     * @param <T> result type
     */
    public class TypedQueryTemplate<T> {

        private final SqlTemplate<Map<String, Object>, RowSet<T>> template;

        TypedQueryTemplate(SqlTemplate<Map<String, Object>, RowSet<T>> template) {
            this.template = template;
        }

        /**
         * Execute the typed query with the given parameters.
         *
         * @param params parameter map
         * @return future of typed row set
         */
        public Future<RowSet<T>> execute(Map<String, Object> params) {
            SqlClient client = getSqlClient();
            return template.withClient(client)
                    .execute(params != null ? params : Collections.emptyMap());
        }

        /**
         * Execute the typed query with no parameters.
         *
         * @return future of typed row set
         */
        public Future<RowSet<T>> execute() {
            return execute(Collections.emptyMap());
        }

        /**
         * Execute the typed query synchronously (blocking).
         *
         * @param params parameter map
         * @return typed row set
         */
        public RowSet<T> executeBlocking(Map<String, Object> params) {
            return execute(params).toCompletionStage().toCompletableFuture().join();
        }

        /**
         * Execute the typed query synchronously with no parameters.
         *
         * @return typed row set
         */
        public RowSet<T> executeBlocking() {
            return executeBlocking(Collections.emptyMap());
        }

        /**
         * Execute and collect results as a {@link List}.
         *
         * @param params parameter map
         * @return future of list
         */
        public Future<List<T>> executeList(Map<String, Object> params) {
            return execute(params).map(rowSet -> {
                return rowSet.stream().collect(Collectors.toList());
            });
        }

        /**
         * Execute and collect results as a {@link List} synchronously.
         *
         * @param params parameter map
         * @return list of results
         */
        public List<T> executeListBlocking(Map<String, Object> params) {
            return executeList(params).toCompletionStage().toCompletableFuture().join();
        }

        /**
         * Execute and return a single result (first row or null).
         *
         * @param params parameter map
         * @return future of single result
         */
        public Future<T> executeOne(Map<String, Object> params) {
            return execute(params).map(rowSet -> {
                var iterator = rowSet.iterator();
                return iterator.hasNext() ? iterator.next() : null;
            });
        }

        /**
         * Execute and return a single result synchronously.
         *
         * @param params parameter map
         * @return single result or null
         */
        public T executeOneBlocking(Map<String, Object> params) {
            return executeOne(params).toCompletionStage().toCompletableFuture().join();
        }
    }

    /**
     * Fluent wrapper for update templates.
     */
    public class UpdateTemplate {

        private final String sql;

        UpdateTemplate(String sql) {
            this.sql = sql;
        }

        /**
         * Map parameters from a POJO class using Jackson-based mapping.
         *
         * @param type source class
         * @param <T>  parameter type
         * @return a typed update template
         */
        public <T> TypedUpdateTemplate<T> mapFrom(Class<T> type) {
            SqlTemplate<T, SqlResult<Void>> template =
                    getOrCacheUpdateTemplate(sql).mapFrom(type);
            return new TypedUpdateTemplate<>(template);
        }

        /**
         * Map parameters using a custom {@link io.vertx.sqlclient.templates.TupleMapper}.
         *
         * @param mapper custom tuple mapper
         * @param <T>    parameter type
         * @return a typed update template
         */
        public <T> TypedUpdateTemplate<T> mapFrom(io.vertx.sqlclient.templates.TupleMapper<T> mapper) {
            SqlTemplate<T, SqlResult<Void>> template =
                    getOrCacheUpdateTemplate(sql).mapFrom(mapper);
            return new TypedUpdateTemplate<>(template);
        }

        /**
         * Execute the update with named parameters.
         *
         * @param params parameter map
         * @return future of row count
         */
        public Future<Integer> execute(Map<String, Object> params) {
            SqlTemplate<Map<String, Object>, SqlResult<Void>> cached = getOrCacheUpdateTemplate(sql);
            return cached.withClient(getSqlClient())
                    .execute(params != null ? params : Collections.emptyMap())
                    .map(SqlResult::rowCount);
        }

        /**
         * Execute the update with no parameters.
         *
         * @return future of row count
         */
        public Future<Integer> execute() {
            return execute(Collections.emptyMap());
        }

        /**
         * Execute the update synchronously (blocking).
         *
         * @param params parameter map
         * @return row count
         */
        public int executeBlocking(Map<String, Object> params) {
            return execute(params).toCompletionStage().toCompletableFuture().join();
        }

        /**
         * Execute the update synchronously with no parameters.
         *
         * @return row count
         */
        public int executeBlocking() {
            return executeBlocking(Collections.emptyMap());
        }
    }

    /**
     * Typed update template with parameter mapping already configured.
     *
     * @param <T> parameter type
     */
    public class TypedUpdateTemplate<T> {

        private final SqlTemplate<T, SqlResult<Void>> template;

        TypedUpdateTemplate(SqlTemplate<T, SqlResult<Void>> template) {
            this.template = template;
        }

        /**
         * Execute the typed update.
         *
         * @param params parameter object
         * @return future of row count
         */
        public Future<Integer> execute(T params) {
            SqlClient client = getSqlClient();
            return template.withClient(client)
                    .execute(params)
                    .map(SqlResult::rowCount);
        }

        /**
         * Execute the typed update synchronously.
         *
         * @param params parameter object
         * @return row count
         */
        public int executeBlocking(T params) {
            return execute(params).toCompletionStage().toCompletableFuture().join();
        }

        /**
         * Execute a batch update.
         *
         * @param batch list of parameter objects
         * @return future of sql result
         */
        public Future<SqlResult<Void>> executeBatch(List<T> batch) {
            SqlClient client = getSqlClient();
            return template.withClient(client).executeBatch(batch);
        }
    }

    // ===== Utility =====

    /**
     * Get the appropriate SqlClient, respecting Spring @Transactional connections.
     */
    private SqlClient getSqlClient() {
        SqlConnection springConn = VertxDataSourceTransactionManager.getCurrentConnection(pool);
        return springConn != null ? springConn : pool;
    }
}
