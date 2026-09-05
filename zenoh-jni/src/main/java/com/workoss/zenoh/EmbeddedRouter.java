package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

public final class EmbeddedRouter implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    EmbeddedRouter(long handle) {
        this.handle = handle;
    }

    /**
     * Default constructor — starts with default peer-mode config.
     * Required by boltffi for opaque type instantiation.
     */
    public EmbeddedRouter() {
        this(EmbeddedRouter.__boltffiCreateHandle0());
    }

    private static long __boltffiCreateHandle0() {
        return Native.boltffi_init_class_zenoh_ffi_router_embedded_router_new();
    }

    /**
     * Start an embedded router with the given Config.
     * The config should have mode set to "router" and plugins configured.
     * 
     * Example usage from Java:
     * ```java
     * Config config = Config.fromFile("router.json5");
     * EmbeddedRouter router = EmbeddedRouter.start(config);
     * Session session = router.session();
     * ```
     */
    public EmbeddedRouter(Config config) {
        this(EmbeddedRouter.__boltffiCreateHandle1(config));
    }

    private static long __boltffiCreateHandle1(Config config) {
        try {
    return Native.boltffi_init_class_zenoh_ffi_router_embedded_router_start(config.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException("Constructor failed");
}
    }

    /**
     * Start an embedded router from a JSON5 configuration string.
     * This is equivalent to loading a zenohd configuration file.
     * 
     * Example JSON5:
     * ```json5
     * {
     *   mode: "router",
     *   listen: { endpoints: ["tcp/[::]:7447"] },
     *   plugins_loading: { enabled: true, search_dirs: ["~/.zenoh/lib"] },
     *   plugins: {
     *     storage_manager: {
     *       volumes: { fs: { backend: "filesystem" } },
     *       storages: {
     *         demo: { key_expr: "demo/**", volume: { id: "fs", dir: "data" } }
     *       }
     *     }
     *   }
     * }
     * ```
     */
    public EmbeddedRouter(String json5Config) {
        this(EmbeddedRouter.__boltffiCreateHandle2(json5Config));
    }

    private static long __boltffiCreateHandle2(String json5Config) {
        WireLease __boltffi_json5Config_wire = WireWriterPool.acquire(WireSizes.string(json5Config));
        try {
    WireWriter __boltffi_json5Config_writer = __boltffi_json5Config_wire.writer();
    __boltffi_json5Config_writer.writeString(json5Config);
    try {
        return Native.boltffi_init_class_zenoh_ffi_router_embedded_router_start_with_json5(__boltffi_json5Config_wire.directBuffer(), __boltffi_json5Config_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException("Constructor failed");
    }
} finally {
    __boltffi_json5Config_wire.close();
}
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("EmbeddedRouter is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_router_embedded_router(this.handle);
    }

    /**
     * Start a router with default configuration.
     * Uses router mode with default listen endpoint (tcp/[::]:7447).
     * Plugin loading is enabled with default search dirs (`.` and `~/.zenoh/lib`),
     * but no plugins are configured by default.
     */
    public static EmbeddedRouter startDefault() {
        try {
    return new EmbeddedRouter(Native.boltffi_init_class_zenoh_ffi_router_embedded_router_start_default());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException("Factory constructor failed");
}
    }

    /**
     * Start a router in peer mode (no routing, just mesh connectivity).
     */
    public static EmbeddedRouter startPeer(Config config) {
        try {
    return new EmbeddedRouter(Native.boltffi_init_class_zenoh_ffi_router_embedded_router_start_peer(config.rawHandle()));
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException("Factory constructor failed");
}
    }

    /**
     * Put data on a key expression via the router's session.
     * Equivalent to zenoh-java: router.session().put(keyExpr, payload)
     */
    public void put(String keyExpr, byte[] payload) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        WireLease __boltffi_payload_wire = WireWriterPool.acquire((4 + payload.length));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    WireWriter __boltffi_payload_writer = __boltffi_payload_wire.writer();
    __boltffi_payload_writer.writeBytes(payload);
    try {
        Native.boltffi_method_class_zenoh_ffi_router_embedded_router_put(this.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size(), __boltffi_payload_wire.directBuffer(), __boltffi_payload_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
    __boltffi_payload_wire.close();
}
    }

    /**
     * Delete data on a key expression via the router's session.
     * Equivalent to zenoh-java: router.session().delete(keyExpr)
     */
    public void delete(String keyExpr) {
        WireLease __boltffi_keyExpr_wire = WireWriterPool.acquire(WireSizes.string(keyExpr));
        try {
    WireWriter __boltffi_keyExpr_writer = __boltffi_keyExpr_wire.writer();
    __boltffi_keyExpr_writer.writeString(keyExpr);
    try {
        Native.boltffi_method_class_zenoh_ffi_router_embedded_router_delete(this.rawHandle(), __boltffi_keyExpr_wire.directBuffer(), __boltffi_keyExpr_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_keyExpr_wire.close();
}
    }

    /**
     * Close the embedded router.
     */
    public void closeRouter() {
        try {
    Native.boltffi_method_class_zenoh_ffi_router_embedded_router_close_router(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Check if the router is still running (session not closed).
     */
    public boolean isRunning() {
        return Native.boltffi_method_class_zenoh_ffi_router_embedded_router_is_running(this.rawHandle());
    }

    /**
     * Get the ZID (Zenoh ID) of this router.
     */
    public String zid() {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_router_embedded_router_zid(this.rawHandle());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    }
}