package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An embedded Zenoh router that runs within the application process.
 * Supports loading plugins (storage_manager, filesystem backend, rocksdb backend)
 * via JSON5 configuration compatible with zenohd.
 *
 * In router mode, this node can:
 * - Accept connections from peers and clients
 * - Route messages between connected nodes
 * - Load and manage storage plugins (zenoh-backend-filesystem, zenoh-backend-rocksdb)
 */
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
        this(EmbeddedRouter.createHandle0());
    }

    private static long createHandle0() {
        return Native.boltffi_embedded_router_new();
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
        this(EmbeddedRouter.createHandle1(config));
    }

    private static long createHandle1(Config config) {
        long _handle = Native.boltffi_embedded_router_start(config.rawHandle());
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
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
        this(EmbeddedRouter.createHandle2(json5Config));
    }

    private static long createHandle2(String json5Config) {
        long _handle = Native.boltffi_embedded_router_start_with_json5(json5Config.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_embedded_router_free(handle);
    }

    /**
     * Start a router with default configuration.
     * Uses router mode with default listen endpoint (tcp/[::]:7447).
     * Plugin loading is enabled with default search dirs (`.` and `~/.zenoh/lib`),
     * but no plugins are configured by default.
     */
    public static EmbeddedRouter startDefault() {
        long _handle = Native.boltffi_embedded_router_start_default();
        if (_handle == 0L) throw new RuntimeException("Factory constructor failed");
        return new EmbeddedRouter(_handle);
    }

    /**
     * Start a router in peer mode (no routing, just mesh connectivity).
     */
    public static EmbeddedRouter startPeer(Config config) {
        long _handle = Native.boltffi_embedded_router_start_peer(config.rawHandle());
        if (_handle == 0L) throw new RuntimeException("Factory constructor failed");
        return new EmbeddedRouter(_handle);
    }

    /**
     * Put data on a key expression via the router's session.
     * Equivalent to zenoh-java: router.session().put(keyExpr, payload)
     */
    public void put(String keyExpr, byte[] payload) {
        byte[] _buf = Native.boltffi_embedded_router_put(handle, keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8), payload);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Delete data on a key expression via the router's session.
     * Equivalent to zenoh-java: router.session().delete(keyExpr)
     */
    public void delete(String keyExpr) {
        byte[] _buf = Native.boltffi_embedded_router_delete(handle, keyExpr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Close the embedded router.
     */
    public void closeRouter() {
        byte[] _buf = Native.boltffi_embedded_router_close(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Check if the router is still running (session not closed).
     */
    public boolean isRunning() {
        return Native.boltffi_embedded_router_is_running(handle);
    }

    /**
     * Get the ZID (Zenoh ID) of this router.
     */
    public String zid() {
        byte[] _buf = Native.boltffi_embedded_router_zid(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        return reader.readString();
    }
}