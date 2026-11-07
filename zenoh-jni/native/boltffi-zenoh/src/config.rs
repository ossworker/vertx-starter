use boltffi::export;
use std::sync::{Arc, Mutex};

/// Every unicast scheme zenoh 1.x knows; which of them are usable depends on compiled features.
const LINK_PROTOCOL_CANDIDATES: &[&str] = &[
    "tcp", "udp", "tls", "quic", "ws", "unixsock-stream", "unixpipe", "serial", "vsock",
];

/// Zenoh configuration - supports JSON5 format compatible with zenohd.
/// Wraps zenoh::Config to provide FFI-safe access.
#[export]
pub struct Config {
    inner: Arc<Mutex<zenoh::Config>>,
}

#[export]
impl Config {
    /// Default constructor (creates peer mode config).
    /// Required by boltffi for opaque type instantiation.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(zenoh::Config::default())),
        }
    }

    /// Create default configuration (peer mode).
    /// Equivalent to zenoh-java: Config.default()
    pub fn default_config() -> Result<Self, String> {
        let config = zenoh::Config::default();
        Ok(Self {
            inner: Arc::new(Mutex::new(config)),
        })
    }

    /// Load configuration from a JSON5 file path.
    /// Equivalent to zenoh-java: Config.fromFile(path)
    pub fn from_file(path: String) -> Result<Self, String> {
        let config = zenoh::Config::from_file(&path)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(config)),
        })
    }

    /// Load configuration from a JSON5 string.
    /// Equivalent to zenoh-java: Config.fromJson5(json5)
    pub fn from_json5(json5: String) -> Result<Self, String> {
        let config = zenoh::Config::from_json5(&json5)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(config)),
        })
    }

    /// Insert a JSON5 value at the given key path.
    /// Example: insert_json5("mode", "\"router\"")
    pub fn insert_json5(&self, key: String, value: String) -> Result<(), String> {
        let mut config = self.inner.lock().map_err(|e| e.to_string())?;
        config.insert_json5(&key, &value)
            .map_err(|e| e.to_string())
    }

    /// Get a JSON value at the given key path.
    pub fn get_json(&self, key: String) -> Result<String, String> {
        let config = self.inner.lock().map_err(|e| e.to_string())?;
        config.get_json(&key)
            .map_err(|e| e.to_string())
    }

    /// Set the node mode: "router", "peer", or "client".
    pub fn set_mode(&self, mode: String) -> Result<(), String> {
        self.insert_json5("mode".to_string(), format!("\"{}\"", mode))
    }

    /// Set listen endpoints.
    /// Example: set_listen_endpoints(vec!["tcp/[::]:7447".to_string()])
    pub fn set_listen_endpoints(&self, endpoints: Vec<String>) -> Result<(), String> {
        let json_arr = serde_json::to_string(&endpoints)
            .map_err(|e| e.to_string())?;
        self.insert_json5("listen/endpoints".to_string(), json_arr)
    }

    /// Set connect endpoints.
    pub fn set_connect_endpoints(&self, endpoints: Vec<String>) -> Result<(), String> {
        let json_arr = serde_json::to_string(&endpoints)
            .map_err(|e| e.to_string())?;
        self.insert_json5("connect/endpoints".to_string(), json_arr)
    }

    /// Link protocols this native library was compiled with, e.g. `tcp`, `quic`, `ws`.
    /// An endpoint whose scheme is missing here fails at session open with
    /// `Unsupported protocol`, so callers can validate configuration up front.
    pub fn supported_link_protocols() -> Vec<String> {
        LINK_PROTOCOL_CANDIDATES
            .iter()
            .filter(|p| !zenoh_link::LinkKind::new_supported_links(std::iter::once(**p)).is_empty())
            .map(|p| p.to_string())
            .collect()
    }

    /// Restrict the link protocols this node may use (`transport/link/protocols`).
    /// Listen/connect endpoints and scouted locators with any other scheme are rejected.
    pub fn set_link_protocols(&self, protocols: Vec<String>) -> Result<(), String> {
        let json_arr = serde_json::to_string(&protocols)
            .map_err(|e| e.to_string())?;
        self.insert_json5("transport/link/protocols".to_string(), json_arr)
    }

    /// Enable/disable plugin loading.
    pub fn set_plugins_loading_enabled(&self, enabled: bool) -> Result<(), String> {
        self.insert_json5("plugins_loading/enabled".to_string(), enabled.to_string())
    }

    /// Set plugin search directories.
    pub fn set_plugins_search_dirs(&self, dirs: Vec<String>) -> Result<(), String> {
        let json_arr = serde_json::to_string(&dirs)
            .map_err(|e| e.to_string())?;
        self.insert_json5("plugins_loading/search_dirs".to_string(), json_arr)
    }

    /// Set up default plugin search directories.
    /// Includes:
    /// - Current directory "."
    /// - User's home zenoh lib directory: ~/.zenoh/lib (macOS/Linux) or %USERPROFILE%\.zenoh\lib (Windows)
    /// Additional custom directories can be appended via `add_plugins_search_dir`.
    pub fn set_default_plugins_search_dirs(&self) -> Result<(), String> {
        let mut dirs = vec![".".to_string()];

        // Add platform-specific default: ~/.zenoh/lib
        if let Some(home) = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
        {
            let home_path = std::path::PathBuf::from(home);
            let zenoh_lib = home_path.join(".zenoh").join("lib");
            dirs.push(zenoh_lib.to_string_lossy().to_string());
        }

        let json_arr = serde_json::to_string(&dirs)
            .map_err(|e| e.to_string())?;
        self.insert_json5("plugins_loading/search_dirs".to_string(), json_arr)
    }

    /// Add a custom plugin search directory to the existing list.
    pub fn add_plugins_search_dir(&self, dir: String) -> Result<(), String> {
        // Get current dirs, add new one
        let current = self.get_json("plugins_loading/search_dirs".to_string())
            .unwrap_or_else(|_| "[]".to_string());
        let mut dirs: Vec<String> = serde_json::from_str(&current)
            .unwrap_or_default();
        dirs.push(dir);
        let json_arr = serde_json::to_string(&dirs)
            .map_err(|e| e.to_string())?;
        self.insert_json5("plugins_loading/search_dirs".to_string(), json_arr)
    }

    /// Ensure platform default plugin search directories (`~/.zenoh/lib`)
    /// are present in `plugins_loading/search_dirs`. Existing entries are preserved
    /// and not duplicated. This is invoked automatically when starting an
    /// `EmbeddedRouter` so user configs only need to specify additional directories.
    pub fn ensure_default_plugins_search_dirs(&self) -> Result<(), String> {
        let current = self.get_json("plugins_loading/search_dirs".to_string())
            .unwrap_or_else(|_| "[]".to_string());
        let mut dirs: Vec<String> = serde_json::from_str(&current)
            .unwrap_or_default();

        // Include ~/.zenoh/lib (or %USERPROFILE%\.zenoh\lib on Windows)
        if let Some(home) = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
        {
            let home_path = std::path::PathBuf::from(home);
            let zenoh_lib = home_path.join(".zenoh").join("lib")
                .to_string_lossy()
                .to_string();
            if !dirs.iter().any(|d| d == &zenoh_lib) {
                dirs.push(zenoh_lib);
            }
        }

        let json_arr = serde_json::to_string(&dirs)
            .map_err(|e| e.to_string())?;
        self.insert_json5("plugins_loading/search_dirs".to_string(), json_arr)
    }
}

// Internal helper to extract the zenoh Config for opening a session
impl Config {
    pub(crate) fn take_inner(&self) -> Result<zenoh::Config, String> {
        let config = self.inner.lock().map_err(|e| e.to_string())?;
        Ok(config.clone())
    }
}
