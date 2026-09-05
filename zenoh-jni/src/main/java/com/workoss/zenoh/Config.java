package com.workoss.zenoh;

import java.util.concurrent.atomic.AtomicBoolean;

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
        this(Config.__boltffiCreateHandle0());
    }

    private static long __boltffiCreateHandle0() {
        return Native.boltffi_init_class_zenoh_ffi_config_config_new();
    }

    /**
     * Load configuration from a JSON5 file path.
     * Equivalent to zenoh-java: Config.fromFile(path)
     */
    public Config(String path) {
        this(Config.__boltffiCreateHandle2(path));
    }

    private static long __boltffiCreateHandle2(String path) {
        WireLease __boltffi_path_wire = WireWriterPool.acquire(WireSizes.string(path));
        try {
    WireWriter __boltffi_path_writer = __boltffi_path_wire.writer();
    __boltffi_path_writer.writeString(path);
    try {
        return Native.boltffi_init_class_zenoh_ffi_config_config_from_file(__boltffi_path_wire.directBuffer(), __boltffi_path_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException("Constructor failed");
    }
} finally {
    __boltffi_path_wire.close();
}
    }

    long rawHandle() {
        if (closed.get()) throw new IllegalStateException("Config is closed");
        return handle;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Native.boltffi_release_class_zenoh_ffi_config_config(this.handle);
    }

    /**
     * Create default configuration (peer mode).
     * Equivalent to zenoh-java: Config.default()
     */
    public static Config defaultConfig() {
        try {
    return new Config(Native.boltffi_init_class_zenoh_ffi_config_config_default_config());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    throw new RuntimeException("Factory constructor failed");
}
    }

    /**
     * Load configuration from a JSON5 string.
     * Equivalent to zenoh-java: Config.fromJson5(json5)
     */
    public static Config fromJson5(String json5) {
        WireLease __boltffi_json5_wire = WireWriterPool.acquire(WireSizes.string(json5));
        try {
    WireWriter __boltffi_json5_writer = __boltffi_json5_wire.writer();
    __boltffi_json5_writer.writeString(json5);
    try {
        return new Config(Native.boltffi_init_class_zenoh_ffi_config_config_from_json5(__boltffi_json5_wire.directBuffer(), __boltffi_json5_wire.size()));
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        throw new RuntimeException("Factory constructor failed");
    }
} finally {
    __boltffi_json5_wire.close();
}
    }

    /**
     * Insert a JSON5 value at the given key path.
     * Example: insert_json5("mode", "&#92;"router&#92;"")
     */
    public void insertJson5(String key, String value) {
        WireLease __boltffi_key_wire = WireWriterPool.acquire(WireSizes.string(key));
        WireLease __boltffi_value_wire = WireWriterPool.acquire(WireSizes.string(value));
        try {
    WireWriter __boltffi_key_writer = __boltffi_key_wire.writer();
    __boltffi_key_writer.writeString(key);
    WireWriter __boltffi_value_writer = __boltffi_value_wire.writer();
    __boltffi_value_writer.writeString(value);
    try {
        Native.boltffi_method_class_zenoh_ffi_config_config_insert_json5(this.rawHandle(), __boltffi_key_wire.directBuffer(), __boltffi_key_wire.size(), __boltffi_value_wire.directBuffer(), __boltffi_value_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_key_wire.close();
    __boltffi_value_wire.close();
}
    }

    /**
     * Get a JSON value at the given key path.
     */
    public String getJson(String key) {
        WireLease __boltffi_key_wire = WireWriterPool.acquire(WireSizes.string(key));
        try {
    WireWriter __boltffi_key_writer = __boltffi_key_wire.writer();
    __boltffi_key_writer.writeString(key);
    try {
        byte[] __boltffi_result = Native.boltffi_method_class_zenoh_ffi_config_config_get_json(this.rawHandle(), __boltffi_key_wire.directBuffer(), __boltffi_key_wire.size());
        WireReader __boltffi_reader = new WireReader(__boltffi_result);
        return __boltffi_reader.readString();
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_key_wire.close();
}
    }

    /**
     * Set the node mode: "router", "peer", or "client".
     */
    public void setMode(String mode) {
        WireLease __boltffi_mode_wire = WireWriterPool.acquire(WireSizes.string(mode));
        try {
    WireWriter __boltffi_mode_writer = __boltffi_mode_wire.writer();
    __boltffi_mode_writer.writeString(mode);
    try {
        Native.boltffi_method_class_zenoh_ffi_config_config_set_mode(this.rawHandle(), __boltffi_mode_wire.directBuffer(), __boltffi_mode_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_mode_wire.close();
}
    }

    /**
     * Set listen endpoints.
     * Example: set_listen_endpoints(vec!["tcp/[::]:7447".to_string()])
     */
    public void setListenEndpoints(java.util.List<String> endpoints) {
        WireLease __boltffi_endpoints_wire = WireWriterPool.acquire(WireSizes.stringSequence(endpoints));
        try {
    WireWriter __boltffi_endpoints_writer = __boltffi_endpoints_wire.writer();
    __boltffi_endpoints_writer.writeStringSequence(endpoints);
    try {
        Native.boltffi_method_class_zenoh_ffi_config_config_set_listen_endpoints(this.rawHandle(), __boltffi_endpoints_wire.directBuffer(), __boltffi_endpoints_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_endpoints_wire.close();
}
    }

    /**
     * Set connect endpoints.
     */
    public void setConnectEndpoints(java.util.List<String> endpoints) {
        WireLease __boltffi_endpoints_wire = WireWriterPool.acquire(WireSizes.stringSequence(endpoints));
        try {
    WireWriter __boltffi_endpoints_writer = __boltffi_endpoints_wire.writer();
    __boltffi_endpoints_writer.writeStringSequence(endpoints);
    try {
        Native.boltffi_method_class_zenoh_ffi_config_config_set_connect_endpoints(this.rawHandle(), __boltffi_endpoints_wire.directBuffer(), __boltffi_endpoints_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_endpoints_wire.close();
}
    }

    /**
     * Enable/disable plugin loading.
     */
    public void setPluginsLoadingEnabled(boolean enabled) {
        try {
    Native.boltffi_method_class_zenoh_ffi_config_config_set_plugins_loading_enabled(this.rawHandle(), enabled);
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Set plugin search directories.
     */
    public void setPluginsSearchDirs(java.util.List<String> dirs) {
        WireLease __boltffi_dirs_wire = WireWriterPool.acquire(WireSizes.stringSequence(dirs));
        try {
    WireWriter __boltffi_dirs_writer = __boltffi_dirs_wire.writer();
    __boltffi_dirs_writer.writeStringSequence(dirs);
    try {
        Native.boltffi_method_class_zenoh_ffi_config_config_set_plugins_search_dirs(this.rawHandle(), __boltffi_dirs_wire.directBuffer(), __boltffi_dirs_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_dirs_wire.close();
}
    }

    /**
     * Set up default plugin search directories.
     * Includes:
     * - Current directory "."
     * - User's home zenoh lib directory: ~/.zenoh/lib (macOS/Linux) or %USERPROFILE%&#92;.zenoh&#92;lib (Windows)
     * Additional custom directories can be appended via `add_plugins_search_dir`.
     */
    public void setDefaultPluginsSearchDirs() {
        try {
    Native.boltffi_method_class_zenoh_ffi_config_config_set_default_plugins_search_dirs(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }

    /**
     * Add a custom plugin search directory to the existing list.
     */
    public void addPluginsSearchDir(String dir) {
        WireLease __boltffi_dir_wire = WireWriterPool.acquire(WireSizes.string(dir));
        try {
    WireWriter __boltffi_dir_writer = __boltffi_dir_wire.writer();
    __boltffi_dir_writer.writeString(dir);
    try {
        Native.boltffi_method_class_zenoh_ffi_config_config_add_plugins_search_dir(this.rawHandle(), __boltffi_dir_wire.directBuffer(), __boltffi_dir_wire.size());
    } catch (BoltFfiErrorBufferException __boltffi_error) {
        WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
        throw new RuntimeException(__boltffi_error_reader.readString());
    }
} finally {
    __boltffi_dir_wire.close();
}
    }

    /**
     * Ensure platform default plugin search directories (`~/.zenoh/lib`)
     * are present in `plugins_loading/search_dirs`. Existing entries are preserved
     * and not duplicated. This is invoked automatically when starting an
     * `EmbeddedRouter` so user configs only need to specify additional directories.
     */
    public void ensureDefaultPluginsSearchDirs() {
        try {
    Native.boltffi_method_class_zenoh_ffi_config_config_ensure_default_plugins_search_dirs(this.rawHandle());
} catch (BoltFfiErrorBufferException __boltffi_error) {
    WireReader __boltffi_error_reader = new WireReader(__boltffi_error.bytes());
    throw new RuntimeException(__boltffi_error_reader.readString());
}
    }
}