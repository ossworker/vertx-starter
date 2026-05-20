package com.workoss.boot.vertx.autoconfigure;

import io.vertx.core.VertxOptions;

/**
 * Callback interface to customize VertxOptions before the Vertx instance is created.
 */
@FunctionalInterface
public interface VertxOptionsCustomizer {

    /**
     * Customize the given VertxOptions.
     *
     * @param options the options to customize
     * @return the customized options
     */
    VertxOptions customize(VertxOptions options);
}
