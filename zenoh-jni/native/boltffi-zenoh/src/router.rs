use boltffi::export;
use std::sync::Arc;
use zenoh::Wait;

use crate::config::Config;
use crate::session::Session;

/// An embedded Zenoh router that runs within the application process.
/// Supports loading plugins (storage_manager, filesystem backend, rocksdb backend)
/// via JSON5 configuration compatible with zenohd.
///
/// In router mode, this node can:
/// - Accept connections from peers and clients
/// - Route messages between connected nodes
/// - Load and manage storage plugins (zenoh-backend-filesystem, zenoh-backend-rocksdb)
#[export]
pub struct EmbeddedRouter {
    session: Arc<zenoh::Session>,
}

#[export]
impl EmbeddedRouter {
    /// Default constructor — starts with default peer-mode config.
    /// Required by boltffi for opaque type instantiation.
    pub fn new() -> Self {
        let config = zenoh::Config::default();
        let session = zenoh::open(config).wait()
            .expect("Failed to open default zenoh session");
        Self {
            session: Arc::new(session),
        }
    }

    /// Start an embedded router with the given Config.
    /// The config should have mode set to "router" and plugins configured.
    ///
    /// Example usage from Java:
    /// ```java
    /// Config config = Config.fromFile("router.json5");
    /// EmbeddedRouter router = EmbeddedRouter.start(config);
    /// Session session = router.session();
    /// ```
    pub fn start(config: &Config) -> Result<Self, String> {
        // Auto-merge platform default plugin search dirs (`.` and `~/.zenoh/lib`)
        // so user configs only need to declare additional directories.
        config.ensure_default_plugins_search_dirs()?;
        let cfg = config.take_inner()?;
        let session = zenoh::open(cfg).wait()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            session: Arc::new(session),
        })
    }

    /// Start an embedded router from a JSON5 configuration string.
    /// This is equivalent to loading a zenohd configuration file.
    ///
    /// Example JSON5:
    /// ```json5
    /// {
    ///   mode: "router",
    ///   listen: { endpoints: ["tcp/[::]:7447"] },
    ///   plugins_loading: { enabled: true, search_dirs: ["~/.zenoh/lib"] },
    ///   plugins: {
    ///     storage_manager: {
    ///       volumes: { fs: { backend: "filesystem" } },
    ///       storages: {
    ///         demo: { key_expr: "demo/**", volume: { id: "fs", dir: "data" } }
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    pub fn start_with_json5(json5_config: String) -> Result<Self, String> {
        let config = Config::from_json5(json5_config)?;
        Self::start(&config)
    }

    /// Start a router with default configuration.
    /// Uses router mode with default listen endpoint (tcp/[::]:7447).
    /// Plugin loading is enabled with default search dirs (`.` and `~/.zenoh/lib`),
    /// but no plugins are configured by default.
    pub fn start_default() -> Result<Self, String> {
        let config = Config::default_config()?;
        config.set_mode("router".to_string())?;
        config.set_plugins_loading_enabled(true)?;
        config.set_default_plugins_search_dirs()?;
        Self::start(&config)
    }

    /// Start a router in peer mode (no routing, just mesh connectivity).
    pub fn start_peer(config: &Config) -> Result<Self, String> {
        config.set_mode("peer".to_string())?;
        Self::start(config)
    }

    /// Put data on a key expression via the router's session.
    /// Equivalent to zenoh-java: router.session().put(keyExpr, payload)
    pub fn put(&self, key_expr: String, payload: Vec<u8>) -> Result<(), String> {
        self.session.put(&key_expr, payload).wait()
            .map_err(|e| e.to_string())
    }

    /// Delete data on a key expression via the router's session.
    /// Equivalent to zenoh-java: router.session().delete(keyExpr)
    pub fn delete(&self, key_expr: String) -> Result<(), String> {
        self.session.delete(&key_expr).wait()
            .map_err(|e| e.to_string())
    }

    /// Close the embedded router.
    pub fn close_router(&self) -> Result<(), String> {
        self.session.close().wait()
            .map_err(|e| e.to_string())
    }

    /// Check if the router is still running (session not closed).
    pub fn is_running(&self) -> bool {
        !self.session.is_closed()
    }

    /// Get the ZID (Zenoh ID) of this router.
    pub fn zid(&self) -> String {
        self.session.zid().to_string()
    }
}

// Internal Rust-only API: returning another exported opaque type (`Session`)
// from an `#[export]` block is not supported by boltffi 0.25.3 — only `Self`
// or `Result<Self, E>` is allowed as a factory return. Keep `session()` outside
// the exported impl so other Rust modules can still reuse the wrapped session.
impl EmbeddedRouter {
    /// Get a Session handle from this router (Rust-only).
    /// The session can be used for pub/sub and queries on this router.
    pub fn session(&self) -> Session {
        Session::from_arc(self.session.clone())
    }

    /// Access the underlying shared `zenoh::Session`.
    pub(crate) fn inner(&self) -> &Arc<zenoh::Session> {
        &self.session
    }
}
