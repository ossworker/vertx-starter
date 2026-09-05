package com.workoss.boot.vertx.mybatis.annotation;

import com.workoss.boot.vertx.mybatis.mapper.VertxMapperFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.mapper.MapperFactoryBean;

import java.lang.annotation.*;

/**
 * Convenience alias for mybatis-spring's {@link MapperScan} pre-configured
 * with {@link VertxMapperFactoryBean} as the factory bean.
 * <p>
 * This enables automatic scanning of mapper interfaces and registration as
 * Spring beans backed by {@link VertxMapperFactoryBean}, which creates proxies
 * supporting both synchronous and asynchronous ({@code Future}) return types.
 * <p>
 * Equivalent to:
 * <pre>{@code
 * @MapperScan(value = "...", factoryBean = VertxMapperFactoryBean.class)
 * }</pre>
 * <p>
 * Example usage:
 * <pre>{@code
 * @VertxMapperScan("com.example.mapper")
 * @SpringBootApplication
 * public class Application {
 *     public static void main(String[] args) {
 *         SpringApplication.run(Application.class, args);
 *     }
 * }
 * }</pre>
 *
 * @see MapperScan
 * @see VertxMapperFactoryBean
 */
@MapperScan(factoryBean = VertxMapperFactoryBean.class)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface VertxMapperScan {

    /**
     * Alias for {@link MapperScan#value()}.
     */
    String[] value() default {};

    /**
     * Alias for {@link MapperScan#basePackages()}.
     */
    String[] basePackages() default {};

    /**
     * Alias for {@link MapperScan#basePackageClasses()}.
     */
    Class<?>[] basePackageClasses() default {};

    /**
     * Alias for {@link MapperScan#annotationClass()}.
     */
    Class<? extends Annotation> annotationClass() default Annotation.class;

    /**
     * Alias for {@link MapperScan#markerInterface()}.
     */
    Class<?> markerInterface() default Class.class;

    /**
     * Alias for {@link MapperScan#sqlSessionFactoryRef()}.
     */
    String sqlSessionFactoryRef() default "";

    /**
     * Alias for {@link MapperScan#sqlSessionTemplateRef()}.
     */
    String sqlSessionTemplateRef() default "";

    /**
     * Alias for {@link MapperScan#lazyInitialization()}.
     */
    String lazyInitialization() default "";
}
