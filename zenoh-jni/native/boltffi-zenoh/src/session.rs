use boltffi::export;
use std::sync::Arc;
use zenoh::Wait;

use crate::config::Config;
use crate::router::EmbeddedRouter;

/// A zenoh session - the main entry point for pub/sub and queries.
/// Wraps zenoh::Session which is internally Arc-based and thread-safe.
#[export]
pub struct Session {
    inner: Arc<zenoh::Session>,
}

#[export]
impl Session {
    /// Open a new session with the given configuration.
    /// Equivalent to zenoh-java: Zenoh.open(config)
    ///
    /// Note: This is synchronous because boltffi 0.25 does not export
    /// C symbols for async fn returning Result<Self, String>.
    pub fn open(config: &Config) -> Result<Self, String> {
        let cfg = config.take_inner()?;
        let session = zenoh::open(cfg).wait()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(session),
        })
    }

    /// Share an embedded router's own session: same runtime, listeners and connections, so no
    /// loopback link (and no TLS material for it) is needed. Closing this session closes the
    /// router; release the handle and close the router instead.
    pub fn from_router(router: &EmbeddedRouter) -> Result<Self, String> {
        if router.inner().is_closed() {
            return Err("Embedded router is closed".to_string());
        }
        Ok(Self::from_arc(router.inner().clone()))
    }

    /// Put data on a key expression (shortcut without declaring a publisher).
    /// Equivalent to zenoh-java: session.put(keyExpr, payload)
    pub fn put(&self, key_expr: String, payload: Vec<u8>) -> Result<(), String> {
        self.inner.put(&key_expr, payload).wait()
            .map_err(|e| e.to_string())
    }

    /// Delete data on a key expression.
    /// Equivalent to zenoh-java: session.delete(keyExpr)
    pub fn delete(&self, key_expr: String) -> Result<(), String> {
        self.inner.delete(&key_expr).wait()
            .map_err(|e| e.to_string())
    }

    /// Close the session.
    /// Equivalent to zenoh-java: session.close()
    pub fn close_session(&self) -> Result<(), String> {
        self.inner.close().wait()
            .map_err(|e| e.to_string())
    }

    /// Check if the session is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Get the session's ZID (Zenoh ID) as a string.
    pub fn zid(&self) -> String {
        self.inner.zid().to_string()
    }

    /// Perform a get query on a selector and collect replies.
    /// Equivalent to zenoh-java: session.get(selector)
    pub fn get(&self, selector: String) -> Result<Vec<crate::reply::Reply>, String> {
        let replies = self
            .inner
            .get(&selector)
            .wait()
            .map_err(|e| e.to_string())?;

        let mut results = Vec::new();
        while let Ok(reply) = replies.recv() {
            results.push(crate::reply::Reply::from_zenoh(reply));
        }
        Ok(results)
    }
}

// Internal helper for other modules to access the inner session
impl Session {
    pub(crate) fn inner(&self) -> &Arc<zenoh::Session> {
        &self.inner
    }

    pub(crate) fn from_arc(session: Arc<zenoh::Session>) -> Self {
        Self { inner: session }
    }
}
