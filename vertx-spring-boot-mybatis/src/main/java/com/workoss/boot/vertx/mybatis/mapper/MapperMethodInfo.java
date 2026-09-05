package com.workoss.boot.vertx.mybatis.mapper;

import io.vertx.core.Future;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Metadata for a single mapper interface method, including:
 * <ul>
 *   <li>SQL statement ID (interface name + method name)</li>
 *   <li>SQL command type (SELECT, INSERT, UPDATE, DELETE)</li>
 *   <li>Return type analysis (Future, List, Optional, single object)</li>
 *   <li>Parameter extraction logic (@Param, RowBounds, ResultHandler)</li>
 * </ul>
 */
public class MapperMethodInfo {

    private final String statementId;
    private final SqlCommandType sqlCommandType;
    private final boolean futureReturnType;
    private final Type futureInnerType;
    private final boolean singleResult;
    private final boolean optionalReturn;
    private final Class<?> rawReturnType;
    private final int rowBoundsParamIndex;
    private final int resultHandlerParamIndex;
    private final boolean cursorReturnType;
    private final ParamExtractor paramExtractor;

    public MapperMethodInfo(Class<?> mapperInterface, Method method, Configuration configuration) {
        this.statementId = mapperInterface.getName() + "." + method.getName();

        // Resolve SQL command type from Configuration
        MappedStatement ms = configuration.hasStatement(statementId)
                ? configuration.getMappedStatement(statementId)
                : null;
        this.sqlCommandType = ms != null ? ms.getSqlCommandType() : SqlCommandType.UNKNOWN;

        // Analyze return type
        ReturnTypeInfo rti = analyzeReturnType(method);
        this.futureReturnType = rti.future;
        this.futureInnerType = rti.innerType;
        this.singleResult = rti.single;
        this.optionalReturn = rti.optional;
        this.rawReturnType = method.getReturnType();
        this.cursorReturnType = Cursor.class.isAssignableFrom(rawReturnType);

        // Analyze parameters
        int rbIdx = -1;
        int rhIdx = -1;
        Annotation[][] paramAnnotations = method.getParameterAnnotations();
        Class<?>[] paramTypes = method.getParameterTypes();

        List<Integer> dataParamIndices = new ArrayList<>();
        Map<String, Integer> namedParams = new LinkedHashMap<>();

        for (int i = 0; i < paramTypes.length; i++) {
            if (RowBounds.class.isAssignableFrom(paramTypes[i])) {
                rbIdx = i;
            } else if (ResultHandler.class.isAssignableFrom(paramTypes[i])) {
                rhIdx = i;
            } else {
                dataParamIndices.add(i);
                Param param = findParamAnnotation(paramAnnotations[i]);
                if (param != null) {
                    namedParams.put(param.value(), i);
                } else {
                    // Use parameter name as key (requires -parameters compiler flag)
                    String name = method.getParameters()[i].getName();
                    namedParams.put(name, i);
                }
            }
        }

        this.rowBoundsParamIndex = rbIdx;
        this.resultHandlerParamIndex = rhIdx;

        // Build parameter extractor
        if (dataParamIndices.isEmpty()) {
            this.paramExtractor = args -> null;
        } else if (dataParamIndices.size() == 1) {
            int idx = dataParamIndices.get(0);
            this.paramExtractor = args -> args != null ? args[idx] : null;
        } else {
            // Multiple data params → wrap in a Map
            final Map<String, Integer> finalNamedParams = Collections.unmodifiableMap(namedParams);
            final List<Integer> finalDataIndices = Collections.unmodifiableList(dataParamIndices);
            this.paramExtractor = args -> {
                if (args == null) return null;
                Map<String, Object> map = new HashMap<>();
                for (Map.Entry<String, Integer> entry : finalNamedParams.entrySet()) {
                    map.put(entry.getKey(), args[entry.getValue()]);
                }
                // Also add param1, param2, etc.
                int pi = 1;
                for (int idx : finalDataIndices) {
                    map.put("param" + pi++, args[idx]);
                }
                return map;
            };
        }
    }

    private Param findParamAnnotation(Annotation[] annotations) {
        for (Annotation a : annotations) {
            if (a instanceof Param p) {
                return p;
            }
        }
        return null;
    }

    public String getStatementId() {
        return statementId;
    }

    public SqlCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    public boolean isFutureReturnType() {
        return futureReturnType;
    }

    public Type getFutureInnerType() {
        return futureInnerType;
    }

    public boolean isSingleResult() {
        return singleResult;
    }

    public boolean isOptionalReturn() {
        return optionalReturn;
    }

    public Class<?> getRawReturnType() {
        return rawReturnType;
    }

    public boolean isCursorReturnType() {
        return cursorReturnType;
    }

    /**
     * Extract the parameter object from the method arguments.
     */
    public Object extractParameter(Object[] args) {
        return paramExtractor.extract(args);
    }

    /**
     * Extract RowBounds from the method arguments.
     */
    public RowBounds extractRowBounds(Object[] args) {
        if (rowBoundsParamIndex >= 0 && args != null && args.length > rowBoundsParamIndex) {
            return (RowBounds) args[rowBoundsParamIndex];
        }
        return RowBounds.DEFAULT;
    }

    /**
     * Extract ResultHandler from the method arguments.
     */
    public ResultHandler<?> extractResultHandler(Object[] args) {
        if (resultHandlerParamIndex >= 0 && args != null && args.length > resultHandlerParamIndex) {
            return (ResultHandler<?>) args[resultHandlerParamIndex];
        }
        return null;
    }

    /**
     * Determine if the inner type of a Future is a list/collection type.
     * e.g., Future&lt;List&lt;User&gt;&gt; → true; Future&lt;User&gt; → false
     */
    public boolean isFutureListResult() {
        if (!futureReturnType || futureInnerType == null) return false;
        if (futureInnerType instanceof ParameterizedType pt) {
            return Collection.class.isAssignableFrom((Class<?>) pt.getRawType());
        }
        return false;
    }

    /**
     * For Future&lt;List&lt;T&gt;&gt;, returns true if the inner list contains single items.
     * For Future&lt;T&gt;, returns true (single item).
     */
    public boolean isFutureSingleResult() {
        if (!futureReturnType) return singleResult;
        return !isFutureListResult();
    }

    // ===== Return type analysis =====

    private record ReturnTypeInfo(boolean future, Type innerType, boolean single, boolean optional) {
    }

    private static ReturnTypeInfo analyzeReturnType(Method method) {
        Type genericReturnType = method.getGenericReturnType();
        Class<?> returnType = method.getReturnType();

        // void
        if (returnType == void.class || returnType == Void.class) {
            return new ReturnTypeInfo(false, null, false, false);
        }

        // Future<T>
        if (Future.class.isAssignableFrom(returnType) && genericReturnType instanceof ParameterizedType pt) {
            Type innerType = pt.getActualTypeArguments()[0];
            boolean single = isSingleType(innerType);
            return new ReturnTypeInfo(true, innerType, single, false);
        }

        // Optional<T>
        if (Optional.class.isAssignableFrom(returnType)) {
            Type innerType = null;
            if (genericReturnType instanceof ParameterizedType pt) {
                innerType = pt.getActualTypeArguments()[0];
            }
            return new ReturnTypeInfo(false, innerType, true, true);
        }

        // Cursor<T>
        if (Cursor.class.isAssignableFrom(returnType)) {
            return new ReturnTypeInfo(false, genericReturnType, false, false);
        }

        // Collection types (List, Set, etc.)
        if (Collection.class.isAssignableFrom(returnType)) {
            return new ReturnTypeInfo(false, genericReturnType, false, false);
        }

        // Map
        if (Map.class.isAssignableFrom(returnType)) {
            return new ReturnTypeInfo(false, genericReturnType, false, false);
        }

        // Array
        if (returnType.isArray()) {
            return new ReturnTypeInfo(false, genericReturnType, false, false);
        }

        // Single object
        return new ReturnTypeInfo(false, returnType, true, false);
    }

    private static boolean isSingleType(Type type) {
        if (type instanceof Class<?> clazz) {
            return !Collection.class.isAssignableFrom(clazz) && !Map.class.isAssignableFrom(clazz);
        }
        if (type instanceof ParameterizedType pt) {
            Class<?> raw = (Class<?>) pt.getRawType();
            return !Collection.class.isAssignableFrom(raw) && !Map.class.isAssignableFrom(raw);
        }
        return true;
    }

    @FunctionalInterface
    private interface ParamExtractor {
        Object extract(Object[] args);
    }
}
