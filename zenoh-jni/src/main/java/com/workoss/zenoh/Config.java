package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Zenoh configuration - supports JSON5 format compatible with zenohd.
 * Wraps zenoh::Config to provide FFI-safe access.
 */
public final class Config implements AutoCloseable {
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Config(long handle) {
        this.handle = handle;
    }

    /**
     * Default constructor (creates peer mode config).
     * Required by boltffi for opaque type instantiation.
     */
    public Config() {
        this(Config.createHandle0());
    }

    private static long createHandle0() {
        return Native.boltffi_config_new();
    }

    /**
     * Load configuration from a JSON5 file path.
     * Equivalent to zenoh-java: Config.fromFile(path)
     */
    public Config(String path) {
        this(Config.createHandle2(path));
    }

    private static long createHandle2(String path) {
        long _handle = Native.boltffi_config_from_file(path.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Constructor failed");
        return _handle;
    }

    long rawHandle() {
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_config_free(handle);
    }

    /**
     * Create default configuration (peer mode).
     * Equivalent to zenoh-java: Config.default()
     */
    public static Config defaultConfig() {
        long _handle = Native.boltffi_config_default_config();
        if (_handle == 0L) throw new RuntimeException("Factory constructor failed");
        return new Config(_handle);
    }

    /**
     * Load configuration from a JSON5 string.
     * Equivalent to zenoh-java: Config.fromJson5(json5)
     */
    public static Config fromJson5(String json5) {
        long _handle = Native.boltffi_config_from_json5(json5.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_handle == 0L) throw new RuntimeException("Factory constructor failed");
        return new Config(_handle);
    }

    /**
     * Insert a JSON5 value at the given key path.
     * Example: insert_json5("mode", "\"router\"")
     */
    public void insertJson5(String key, String value) {
        byte[] _buf = Native.boltffi_config_insert_json5(handle, key.getBytes(java.nio.charset.StandardCharsets.UTF_8), value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Get a JSON value at the given key path.
     */
    public String getJson(String key) {
        byte[] _buf = Native.boltffi_config_get_json(handle, key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
        return reader.readString();
    }

    /**
     * Set the node mode: "router", "peer", or "client".
     */
    public void setMode(String mode) {
        byte[] _buf = Native.boltffi_config_set_mode(handle, mode.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Set listen endpoints.
     * Example: set_listen_endpoints(vec!["tcp/[::]:7447".to_string()])
     */
    public void setListenEndpoints(java.util.List<String> endpoints) {
        try (
            WireWriter _wire_endpoints = new WireWriter(WireWriter.listWireSize(endpoints, sizeItem0 -> (4 + (4 + (sizeItem0).length() * 3))))
        ) {
            _wire_endpoints.writeI32(endpoints.size()); for (String item0 : endpoints) { _wire_endpoints.writeString(item0); };
            byte[] _buf = Native.boltffi_config_set_listen_endpoints(handle, _wire_endpoints.toBuffer());
            if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
            WireReader reader = new WireReader(_buf);
            if (reader.readI8() != 0) {
                throw new RuntimeException(reader.readString());
            }
        }
    }

    /**
     * Set connect endpoints.
     */
    public void setConnectEndpoints(java.util.List<String> endpoints) {
        try (
            WireWriter _wire_endpoints = new WireWriter(WireWriter.listWireSize(endpoints, sizeItem0 -> (4 + (4 + (sizeItem0).length() * 3))))
        ) {
            _wire_endpoints.writeI32(endpoints.size()); for (String item0 : endpoints) { _wire_endpoints.writeString(item0); };
            byte[] _buf = Native.boltffi_config_set_connect_endpoints(handle, _wire_endpoints.toBuffer());
            if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
            WireReader reader = new WireReader(_buf);
            if (reader.readI8() != 0) {
                throw new RuntimeException(reader.readString());
            }
        }
    }

    /**
     * Enable/disable plugin loading.
     */
    public void setPluginsLoadingEnabled(boolean enabled) {
        byte[] _buf = Native.boltffi_config_set_plugins_loading_enabled(handle, enabled);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Set plugin search directories.
     */
    public void setPluginsSearchDirs(java.util.List<String> dirs) {
        try (
            WireWriter _wire_dirs = new WireWriter(WireWriter.listWireSize(dirs, sizeItem0 -> (4 + (4 + (sizeItem0).length() * 3))))
        ) {
            _wire_dirs.writeI32(dirs.size()); for (String item0 : dirs) { _wire_dirs.writeString(item0); };
            byte[] _buf = Native.boltffi_config_set_plugins_search_dirs(handle, _wire_dirs.toBuffer());
            if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
            WireReader reader = new WireReader(_buf);
            if (reader.readI8() != 0) {
                throw new RuntimeException(reader.readString());
            }
        }
    }

    /**
     * Set up default plugin search directories.
     * Includes:
     * - Current directory "."
     * - User's home zenoh lib directory: ~/.zenoh/lib (macOS/Linux) or %USERPROFILE%\.zenoh\lib (Windows)
     * Additional custom directories can be appended via `add_plugins_search_dir`.
     */
    public void setDefaultPluginsSearchDirs() {
        byte[] _buf = Native.boltffi_config_set_default_plugins_search_dirs(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Add a custom plugin search directory to the existing list.
     */
    public void addPluginsSearchDir(String dir) {
        byte[] _buf = Native.boltffi_config_add_plugins_search_dir(handle, dir.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }

    /**
     * Ensure platform default plugin search directories (`~/.zenoh/lib`)
     * are present in `plugins_loading/search_dirs`. Existing entries are preserved
     * and not duplicated. This is invoked automatically when starting an
     * `EmbeddedRouter` so user configs only need to specify additional directories.
     */
    public void ensureDefaultPluginsSearchDirs() {
        byte[] _buf = Native.boltffi_config_ensure_default_plugins_search_dirs(handle);
        if (_buf == null) throw new RuntimeException("FFI call returned null buffer");
        WireReader reader = new WireReader(_buf);
        if (reader.readI8() != 0) {
            throw new RuntimeException(reader.readString());
        }
    }
}