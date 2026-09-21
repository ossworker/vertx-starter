package com.workoss.boot.vertx.mybatis.core;

import com.workoss.boot.vertx.mybatis.typehandler.VertxTypeHandler;
import com.workoss.boot.vertx.mybatis.typehandler.VertxTypeHandlerBridge;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
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
import java.util.*;

/**
 * Maps vertx-sql-client {@link RowSet} results to Java objects,
 * following MyBatis {@link ResultMap} configuration.
 * <p>
 * Supports:
 * <ul>
 *   <li>Simple types (String, Integer, Long, etc.) via first column</li>
 *   <li>Map&lt;String, Object&gt; from all columns</li>
 *   <li>POJO mapping via {@link MetaObject} with underscore-to-camelCase conversion</li>
 *   <li>ResultMap with explicit column→property mapping</li>
 *   <li>association (nested object) with columnPrefix support</li>
 *   <li>collection (nested list) with multi-row aggregation</li>
 * </ul>
 */
public class VertxRowMapper {

    private static final Logger logger = LoggerFactory.getLogger(VertxRowMapper.class);

    private final Configuration configuration;
    /** Bridges JDBC-shaped MyBatis TypeHandlers onto vertx-sql-client row values. */
    private volatile VertxTypeHandlerBridge typeHandlerBridge = new VertxTypeHandlerBridge();

    public VertxRowMapper(Configuration configuration) {
        this.configuration = configuration;
    }

    public VertxTypeHandlerBridge getTypeHandlerBridge() {
        return typeHandlerBridge;
    }

    public void setTypeHandlerBridge(VertxTypeHandlerBridge typeHandlerBridge) {
        this.typeHandlerBridge = typeHandlerBridge != null
                ? typeHandlerBridge : new VertxTypeHandlerBridge();
    }

    /**
     * Map a RowSet to a list of result objects using the given ResultMap.
     */
    @SuppressWarnings("unchecked")
    public <E> List<E> mapResults(RowSet<Row> rowSet, ResultMap resultMap) {
        if (rowSet == null || rowSet.rowCount() == 0) {
            return Collections.emptyList();
        }

        List<E> results = new ArrayList<>(rowSet.rowCount());

        if (resultMap == null || resultMap.getResultMappings().isEmpty()) {
            // Simple resultType mapping
            Class<?> resultType = resultMap != null ? resultMap.getType() : Object.class;
            for (Row row : rowSet) {
                results.add((E) mapSimpleRow(row, resultType));
            }
        } else {
            // Full ResultMap mapping (with possible collections requiring multi-row aggregation)
            results = mapWithResultMap(rowSet, resultMap);
        }

        return results;
    }

    /**
     * Map results using a ResultHandler callback (one row at a time).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void mapResultsWithHandler(RowSet<Row> rowSet, ResultHandler handler,
                                      ResultMap resultMap) {
        if (rowSet == null) {
            return;
        }

        Class<?> resultType = resultMap != null ? resultMap.getType() : Object.class;
        boolean hasResultMappings = resultMap != null && !resultMap.getResultMappings().isEmpty();

        for (Row row : rowSet) {
            Object value;
            if (hasResultMappings) {
                value = mapRowToResultMap(row, resultMap, null);
            } else {
                value = mapSimpleRow(row, resultType);
            }
            handler.handleResult(new org.apache.ibatis.session.ResultContext() {
                private int count = 0;

                @Override
                public Object getResultObject() {
                    return value;
                }

                @Override
                public int getResultCount() {
                    return count;
                }

                @Override
                public boolean isStopped() {
                    return false;
                }

                @Override
                public void stop() {
                }
            });
        }
    }

    /**
     * Map a single row to a simple type (primitive, String, Map, or POJO).
     */
    @SuppressWarnings("unchecked")
    private Object mapSimpleRow(Row row, Class<?> resultType) {
        if (resultType == null || resultType == Object.class) {
            return rowToMap(row);
        }

        // Primitive / wrapper / String / BigDecimal → take first column
        if (isSimpleType(resultType)) {
            String firstColumn = row.getColumnName(0);
            return getColumnValue(row, firstColumn, resultType);
        }

        // Map → all columns
        if (Map.class.isAssignableFrom(resultType)) {
            return rowToMap(row);
        }

        // POJO → map columns to properties
        return mapRowToPojo(row, resultType, null);
    }

    /**
     * Map rows using a full ResultMap with possible multi-row collection aggregation.
     */
    @SuppressWarnings("unchecked")
    private <E> List<E> mapWithResultMap(RowSet<Row> rowSet, ResultMap resultMap) {
        // Check if there are any collection mappings (which require multi-row aggregation)
        boolean hasCollection = resultMap.getResultMappings().stream()
                .anyMatch(rm -> getNestedResultMap(rm) != null
                        && isCollectionMapping(rm));

        if (!hasCollection) {
            // Simple one-row-per-object mapping
            List<E> results = new ArrayList<>(rowSet.rowCount());
            for (Row row : rowSet) {
                results.add((E) mapRowToResultMap(row, resultMap, null));
            }
            return results;
        }

        // Multi-row aggregation for collections
        return mapWithCollectionAggregation(rowSet, resultMap);
    }

    private boolean isCollectionMapping(ResultMapping rm) {
        // A collection mapping typically has javaType that is a Collection
        return rm.getJavaType() != null && Collection.class.isAssignableFrom(rm.getJavaType());
    }

    /**
     * Resolve the nested ResultMap from a ResultMapping.
     * In MyBatis 3.5.x, ResultMapping stores the nested result map ID,
     * and we look it up from Configuration.
     */
    private ResultMap getNestedResultMap(ResultMapping mapping) {
        String nestedId = mapping.getNestedResultMapId();
        if (nestedId == null) {
            return null;
        }
        return configuration.getResultMap(nestedId);
    }

    /**
     * Multi-row aggregation: group rows by the root object's id columns
     * and accumulate collection items.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <E> List<E> mapWithCollectionAggregation(RowSet<Row> rowSet, ResultMap resultMap) {
        // Find ID mappings for grouping
        List<ResultMapping> idMappings = resultMap.getResultMappings().stream()
                .filter(rm -> rm.getFlags().contains(ResultFlag.ID))
                .toList();

        Map<String, E> groupMap = new LinkedHashMap<>();
        List<E> results = new ArrayList<>();

        for (Row row : rowSet) {
            String groupKey = buildGroupKey(row, idMappings);

            E existing = groupMap.get(groupKey);
            if (existing == null) {
                existing = (E) mapRowToResultMap(row, resultMap, null);
                groupMap.put(groupKey, existing);
                results.add(existing);
            } else {
                // Append collection items
                appendCollectionItems(row, existing, resultMap);
            }
        }

        return results;
    }

    private String buildGroupKey(Row row, List<ResultMapping> idMappings) {
        StringBuilder sb = new StringBuilder();
        for (ResultMapping mapping : idMappings) {
            Object val = getColumnValue(row, mapping.getColumn(), mapping.getJavaType());
            sb.append(val).append('|');
        }
        return sb.toString();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void appendCollectionItems(Row row, Object root, ResultMap resultMap) {
        MetaObject metaObject = configuration.newMetaObject(root);

        for (ResultMapping mapping : resultMap.getResultMappings()) {
            if (getNestedResultMap(mapping) != null
                    && isCollectionMapping(mapping)) {
                String property = mapping.getProperty();
                Collection collection = (Collection) metaObject.getValue(property);
                if (collection == null) {
                    collection = new ArrayList<>();
                    metaObject.setValue(property, collection);
                }

                // Map nested row
                Object nestedItem = mapRowToResultMap(row, getNestedResultMap(mapping),
                        mapping.getColumnPrefix());
                if (nestedItem != null) {
                    collection.add(nestedItem);
                }
            }
        }
    }

    /**
     * Map a single row using a ResultMap.
     */
    private Object mapRowToResultMap(Row row, ResultMap resultMap, String parentColumnPrefix) {
        Class<?> type = resultMap.getType();
        if (type == null) {
            return rowToMap(row);
        }

        if (isSimpleType(type)) {
            ResultMapping firstMapping = resultMap.getResultMappings().get(0);
            String column = (parentColumnPrefix != null ? parentColumnPrefix : "")
                    + firstMapping.getColumn();
            return getColumnValue(row, column, type);
        }

        if (Map.class.isAssignableFrom(type)) {
            return rowToMap(row);
        }

        Object instance;
        try {
            instance = type.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate result type: " + type.getName(), e);
        }

        MetaObject metaObject = configuration.newMetaObject(instance);

        for (ResultMapping mapping : resultMap.getResultMappings()) {
            String columnPrefix = parentColumnPrefix != null
                    ? parentColumnPrefix + (mapping.getColumnPrefix() != null ? mapping.getColumnPrefix() : "")
                    : mapping.getColumnPrefix();
            String column = (columnPrefix != null ? columnPrefix : "") + mapping.getColumn();
            String property = mapping.getProperty();

            if (getNestedResultMap(mapping) != null) {
                // Nested object (association or collection)
                if (isCollectionMapping(mapping)) {
                    // Collection: initialize empty, items added during multi-row aggregation
                    @SuppressWarnings("unchecked")
                    Collection<Object> collection = (Collection<Object>) metaObject.getValue(property);
                    if (collection == null) {
                        collection = new ArrayList<>();
                        metaObject.setValue(property, collection);
                    }
                    // First item
                    Object nestedItem = mapRowToResultMap(row, getNestedResultMap(mapping),
                            mapping.getColumnPrefix());
                    if (nestedItem != null) {
                        collection.add(nestedItem);
                    }
                } else {
                    // Association (single nested object)
                    Object nested = mapRowToResultMap(row, getNestedResultMap(mapping),
                            mapping.getColumnPrefix());
                    metaObject.setValue(property, nested);
                }
            } else {
                // Direct column mapping
                Object value = getColumnValueWithHandler(row, column, mapping);
                if (value != null) {
                    metaObject.setValue(property, value);
                }
            }
            // nestedQuery (lazy loading) is not supported in this implementation
        }

        // Handle auto-mapping for columns not in explicit mappings
        Set<String> mappedColumns = new HashSet<>();
        for (ResultMapping mapping : resultMap.getResultMappings()) {
            if (mapping.getColumn() != null) {
                mappedColumns.add(mapping.getColumn().toLowerCase(Locale.ROOT));
            }
        }

        // Auto-map unmapped columns
        for (int i = 0; i < row.size(); i++) {
            String columnName = row.getColumnName(i);
            if (columnName != null && !mappedColumns.contains(columnName.toLowerCase(Locale.ROOT))) {
                String propertyName = mapUnderscoreToCamelCase(columnName);
                if (metaObject.hasSetter(propertyName)) {
                    Class<?> propType = metaObject.getSetterType(propertyName);
                    Object value = getColumnValue(row, columnName, propType);
                    if (value != null) {
                        metaObject.setValue(propertyName, value);
                    }
                }
            }
        }

        return instance;
    }

    /**
     * Map a row to a POJO using reflection via MyBatis MetaObject.
     */
    private Object mapRowToPojo(Row row, Class<?> type, String columnPrefix) {
        Object instance;
        try {
            instance = type.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate result type: " + type.getName(), e);
        }

        MetaObject metaObject = configuration.newMetaObject(instance);

        for (int i = 0; i < row.size(); i++) {
            String columnName = row.getColumnName(i);
            if (columnName == null) {
                continue;
            }
            if (columnPrefix != null && !columnName.toLowerCase(Locale.ROOT)
                    .startsWith(columnPrefix.toLowerCase(Locale.ROOT))) {
                continue;
            }

            String effectiveColumn = columnPrefix != null
                    ? columnName.substring(columnPrefix.length())
                    : columnName;
            String propertyName = mapUnderscoreToCamelCase(effectiveColumn);

            if (metaObject.hasSetter(propertyName)) {
                Class<?> propType = metaObject.getSetterType(propertyName);
                Object value = getColumnValue(row, columnName, propType);
                if (value != null) {
                    metaObject.setValue(propertyName, value);
                }
            }
        }

        return instance;
    }

    /**
     * Get a column value with TypeHandler support.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object getColumnValueWithHandler(Row row, String column, ResultMapping mapping) {
        TypeHandler typeHandler = mapping.getTypeHandler();
        if (typeHandler instanceof VertxTypeHandler) {
            return ((VertxTypeHandler<Object>) typeHandler).fromSqlValue(row.getValue(column));
        }
        if (typeHandler != null) {
            Object raw = row.getValue(column);
            if (raw == null) {
                return null;
            }
            Object converted = typeHandlerBridge.tryRead(typeHandler, row, column);
            if (converted != VertxTypeHandlerBridge.FALLBACK) {
                return converted;
            }
        }
        return getColumnValue(row, column, mapping.getJavaType());
    }

    /**
     * Get a column value from a Row, converting to the target Java type.
     */
    @SuppressWarnings("unchecked")
    public Object getColumnValue(Row row, String columnName, Class<?> javaType) {
        if (columnName == null || javaType == null) {
            return row.getValue(columnName);
        }

        try {
            if (javaType == String.class) return row.getString(columnName);
            if (javaType == Integer.class || javaType == int.class) return row.getInteger(columnName);
            if (javaType == Long.class || javaType == long.class) return row.getLong(columnName);
            if (javaType == Boolean.class || javaType == boolean.class) return row.getBoolean(columnName);
            if (javaType == Double.class || javaType == double.class) return row.getDouble(columnName);
            if (javaType == Float.class || javaType == float.class) return row.getFloat(columnName);
            if (javaType == Short.class || javaType == short.class) return row.getShort(columnName);
            if (javaType == BigDecimal.class) return row.getBigDecimal(columnName);
            if (javaType == LocalDateTime.class) return row.getLocalDateTime(columnName);
            if (javaType == LocalDate.class) return row.getLocalDate(columnName);
            if (javaType == LocalTime.class) return row.getLocalTime(columnName);
            if (javaType == OffsetTime.class) return row.getOffsetTime(columnName);
            if (javaType == OffsetDateTime.class) return row.getOffsetDateTime(columnName);
            if (javaType == UUID.class) return row.getUUID(columnName);
            if (javaType == byte[].class) {
                io.vertx.core.buffer.Buffer buf = row.getBuffer(columnName);
                return buf != null ? buf.getBytes() : null;
            }
            if (javaType.isEnum()) {
                return convertEnum(row.getValue(columnName), javaType);
            }

            // Fallback: getValue + basic conversion
            Object raw = row.getValue(columnName);
            if (raw == null) return null;
            if (javaType.isInstance(raw)) return raw;

            // Try common conversions
            if (javaType == String.class) return raw.toString();
            if (javaType == Integer.class) return ((Number) raw).intValue();
            if (javaType == Long.class) return ((Number) raw).longValue();

            return raw;
        } catch (ClassCastException | IllegalArgumentException e) {
            // Fallback to raw value
            logger.debug("Type conversion failed for column '{}' to {}: {}",
                    columnName, javaType.getSimpleName(), e.getMessage());
            return row.getValue(columnName);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object convertEnum(Object value, Class<?> enumType) {
        if (value == null) return null;
        if (enumType.isInstance(value)) return value;

        if (value instanceof Number number) {
            Object[] constants = enumType.getEnumConstants();
            int ordinal = number.intValue();
            if (ordinal >= 0 && ordinal < constants.length) {
                return constants[ordinal];
            }
        }

        // Try name()
        try {
            return Enum.valueOf((Class<Enum>) enumType, value.toString());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Convert a Row to a Map with column names as keys.
     */
    private Map<String, Object> rowToMap(Row row) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < row.size(); i++) {
            String columnName = row.getColumnName(i);
            map.put(columnName, row.getValue(columnName));
        }
        return map;
    }

    private boolean isSimpleType(Class<?> type) {
        return type.isPrimitive()
                || type == String.class
                || type == Integer.class
                || type == Long.class
                || type == Double.class
                || type == Float.class
                || type == Boolean.class
                || type == Short.class
                || type == Byte.class
                || type == BigDecimal.class
                || type == LocalDateTime.class
                || type == LocalDate.class
                || type == LocalTime.class
                || type == UUID.class;
    }

    private String mapUnderscoreToCamelCase(String name) {
        if (!configuration.isMapUnderscoreToCamelCase()) {
            return name;
        }

        StringBuilder sb = new StringBuilder(name.length());
        boolean capitalizeNext = false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '_') {
                capitalizeNext = true;
            } else {
                sb.append(capitalizeNext ? Character.toUpperCase(c) : c);
                capitalizeNext = false;
            }
        }
        return sb.toString();
    }
}
