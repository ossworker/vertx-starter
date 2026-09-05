package com.workoss.boot.vertx.mybatis;

import com.workoss.boot.vertx.mybatis.mapper.MapperMethodInfo;
import com.workoss.boot.vertx.mybatis.mapper.VertxMapperProxy;
import com.workoss.boot.vertx.mybatis.mapper.VertxMapperProxyFactory;
import io.vertx.core.Future;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for mapper proxy and method info.
 */
class MapperProxyTest {

    // Test mapper interfaces
    @Mapper
    interface UserMapper {
        Future<User> findById(@Param("id") Long id);

        Future<List<User>> findAll();

        List<User> selectAll();

        User selectOne(@Param("name") String name);

        Optional<User> selectOptional(@Param("id") Long id);

        int insert(@Param("name") String name);

        Future<Integer> insertAsync(@Param("name") String name);

        void deleteAll();
    }

    // Simple POJO for testing
    static class User {
        private Long id;
        private String name;

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    @Test
    void testMapperMethodInfo_FutureSingle() throws Exception {
        Configuration config = new Configuration();
        // Add the select statement for findById
        addMockMappedStatement(config, UserMapper.class.getName() + ".findById");

        var method = UserMapper.class.getMethod("findById", Long.class);
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertTrue(info.isFutureReturnType());
        assertTrue(info.isFutureSingleResult());
        assertFalse(info.isFutureListResult());
    }

    @Test
    void testMapperMethodInfo_FutureList() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".findAll");

        var method = UserMapper.class.getMethod("findAll");
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertTrue(info.isFutureReturnType());
        assertFalse(info.isFutureSingleResult());
        assertTrue(info.isFutureListResult());
    }

    @Test
    void testMapperMethodInfo_SyncList() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".selectAll");

        var method = UserMapper.class.getMethod("selectAll");
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertFalse(info.isFutureReturnType());
        assertFalse(info.isSingleResult());
    }

    @Test
    void testMapperMethodInfo_SyncSingle() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".selectOne");

        var method = UserMapper.class.getMethod("selectOne", String.class);
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertFalse(info.isFutureReturnType());
        assertTrue(info.isSingleResult());
        assertFalse(info.isOptionalReturn());
    }

    @Test
    void testMapperMethodInfo_Optional() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".selectOptional");

        var method = UserMapper.class.getMethod("selectOptional", Long.class);
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertFalse(info.isFutureReturnType());
        assertTrue(info.isSingleResult());
        assertTrue(info.isOptionalReturn());
    }

    @Test
    void testMapperMethodInfo_IntReturn() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".insert");

        var method = UserMapper.class.getMethod("insert", String.class);
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertFalse(info.isFutureReturnType());
        assertTrue(info.isSingleResult());
    }

    @Test
    void testMapperMethodInfo_FutureIntReturn() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".insertAsync");

        var method = UserMapper.class.getMethod("insertAsync", String.class);
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertTrue(info.isFutureReturnType());
        assertTrue(info.isFutureSingleResult());
    }

    @Test
    void testMapperMethodInfo_VoidReturn() throws Exception {
        Configuration config = new Configuration();
        addMockMappedStatement(config, UserMapper.class.getName() + ".deleteAll");

        var method = UserMapper.class.getMethod("deleteAll");
        MapperMethodInfo info = new MapperMethodInfo(UserMapper.class, method, config);

        assertFalse(info.isFutureReturnType());
        assertFalse(info.isSingleResult());
    }

    @Test
    void testMapperProxyFactory() {
        VertxMapperProxyFactory<UserMapper> factory = new VertxMapperProxyFactory<>(UserMapper.class);
        assertEquals(UserMapper.class, factory.getMapperInterface());
    }

    private void addMockMappedStatement(Configuration config, String id) {
        // Add a minimal MappedStatement so the MapperMethodInfo can resolve the SQL command type
        var builder = new org.apache.ibatis.mapping.MappedStatement.Builder(
                config, id,
                new org.apache.ibatis.scripting.defaults.RawSqlSource(config, "SELECT 1", null),
                SqlCommandType.SELECT);
        config.addMappedStatement(builder.build());
    }
}
