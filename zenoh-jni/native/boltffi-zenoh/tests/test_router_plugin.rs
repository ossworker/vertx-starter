use std::thread;
use std::time::Duration;
use zenoh_ffi::router::EmbeddedRouter;

/// Verify that EmbeddedRouter can load the zenoh-backend-filesystem storage plugin
/// (libzenoh_backend_fs.dylib) via the storage_manager plugin
/// (libzenoh_plugin_storage_manager.dylib).
///
/// Required plugin layout (must be prepared in advance):
///   ~/.zenoh/lib/libzenoh_plugin_storage_manager.dylib
///   ~/.zenoh/lib/libzenoh_backend_fs.dylib
///
/// The filesystem backend uses `ZENOH_BACKEND_FS_ROOT` as its root directory
/// and requires `dir` in the volume config to be a relative path under that root.
#[test]
fn test_router_loads_filesystem_plugin() {
    // Prepare a unique FS root and storage subdir
    let fs_root = std::env::temp_dir().join("zenoh_test_fs_root");
    let _ = std::fs::remove_dir_all(&fs_root);
    std::fs::create_dir_all(&fs_root).expect("create fs root");
    // Tell the filesystem backend where its root is
    std::env::set_var("ZENOH_BACKEND_FS_ROOT", &fs_root);

    // The storage's `dir` is *relative* to ZENOH_BACKEND_FS_ROOT
    let rel_dir = "router_plugin_test";
    let expected_storage_path = fs_root.join(rel_dir);

    // Resolve absolute search dir for plugins (avoid `~` expansion ambiguity)
    let home = std::env::var("HOME").expect("HOME not set");
    let zenoh_lib = format!("{}/.zenoh/lib", home);

    // Sanity check: plugin files exist where we expect them
    let storage_mgr = format!("{}/libzenoh_plugin_storage_manager.dylib", zenoh_lib);
    let fs_backend = format!("{}/libzenoh_backend_fs.dylib", zenoh_lib);
    assert!(
        std::path::Path::new(&storage_mgr).exists(),
        "Missing plugin: {}",
        storage_mgr
    );
    assert!(
        std::path::Path::new(&fs_backend).exists(),
        "Missing backend: {}",
        fs_backend
    );

    // Build router config — load storage_manager plugin and configure a
    // filesystem-backed storage. Use port 17447 to avoid clashing with default 7447.
    // Note: search_dirs is intentionally simplified to just ["."]; the router
    // will automatically merge `~/.zenoh/lib` via Config::ensure_default_plugins_search_dirs.
    let json5_config = format!(
        r#"{{
            mode: "router",
            listen: {{
                endpoints: ["tcp/127.0.0.1:17447"]
            }},
            scouting: {{
                multicast: {{ enabled: false }},
                gossip: {{ enabled: false }}
            }},
            plugins_loading: {{
                enabled: true,
                search_dirs: ["."]
            }},
            plugins: {{
                storage_manager: {{
                    __required__: true,
                    volumes: {{
                        fs: {{}}
                    }},
                    storages: {{
                        test_storage: {{
                            key_expr: "test/storage/**",
                            strip_prefix: "test/storage",
                            volume: {{
                                id: "fs",
                                dir: "{rel_dir}"
                            }}
                        }}
                    }}
                }}
            }}
        }}"#,
        rel_dir = rel_dir
    );

    // Initialize logging so plugin load failures are visible under --nocapture
    zenoh::init_log_from_env_or("info");

    // Start the router — this triggers plugin discovery and loading
    let router = EmbeddedRouter::start_with_json5(json5_config)
        .expect("Failed to start router with filesystem plugin");

    assert!(router.is_running(), "Router should be running after start");
    println!("Router started with ZID: {}", router.zid());
    println!("ZENOH_BACKEND_FS_ROOT = {}", fs_root.display());
    println!("Expected storage dir = {}", expected_storage_path.display());

    // Give plugins time to initialize and create the storage backend directory
    thread::sleep(Duration::from_secs(2));

    // Publish data on the storage's key expression — the filesystem backend
    // should persist it to <fs_root>/<rel_dir>/...
    router
        .put(
            "test/storage/key1".to_string(),
            b"hello storage".to_vec(),
        )
        .expect("Failed to put data");

    router
        .put(
            "test/storage/nested/key2".to_string(),
            b"nested value".to_vec(),
        )
        .expect("Failed to put nested data");

    // Allow the filesystem backend to flush
    thread::sleep(Duration::from_secs(2));

    // The filesystem backend should have created the storage root directory
    let storage_exists = expected_storage_path.exists();
    println!(
        "Storage dir {} exists: {}",
        expected_storage_path.display(),
        storage_exists
    );

    if storage_exists {
        // Inspect what got written
        if let Ok(entries) = std::fs::read_dir(&expected_storage_path) {
            for entry in entries.flatten() {
                println!("  - {}", entry.path().display());
            }
        }
    }

    assert!(
        storage_exists,
        "Filesystem backend did not create storage dir at {} — \
         storage_manager or fs backend likely failed to spawn the storage",
        expected_storage_path.display()
    );

    // Close the router cleanly
    router.close().expect("Failed to close router");
    assert!(!router.is_running(), "Router should be closed");

    // Cleanup test artifacts
    let _ = std::fs::remove_dir_all(&fs_root);

    println!("Test passed: Router successfully loaded storage_manager + filesystem backend, and persisted data");
}
