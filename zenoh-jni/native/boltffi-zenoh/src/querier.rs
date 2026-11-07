use boltffi::export;
use std::sync::Arc;
use zenoh::Wait;

use crate::reply::Reply;
use crate::session::Session;

/// A Zenoh querier for performing get operations.
/// Equivalent to zenoh-java: Querier
#[export]
pub struct Querier {
    inner: Arc<zenoh::query::Querier<'static>>,
}

#[export]
impl Querier {
    /// Declare a querier on the given session.
    /// Equivalent to zenoh-java: session.declareQuerier(keyExpr)
    pub fn declare(session: &Session, key_expr: String) -> Result<Self, String> {
        Self::declare_on_session(session.inner(), &key_expr)
    }

    /// Perform a get query and collect all replies.
    /// Equivalent to zenoh-java: querier.get()
    pub fn get(&self) -> Result<Vec<Reply>, String> {
        let replies = self.inner.get().wait().map_err(|e| e.to_string())?;

        let mut results = Vec::new();
        while let Ok(reply) = replies.recv() {
            results.push(Reply::from_zenoh(reply));
        }
        Ok(results)
    }

    /// Perform a get query with a specific payload.
    pub fn get_with_payload(&self, payload: Vec<u8>) -> Result<Vec<Reply>, String> {
        let replies = self
            .inner
            .get()
            .payload(payload)
            .wait()
            .map_err(|e| e.to_string())?;

        let mut results = Vec::new();
        while let Ok(reply) = replies.recv() {
            results.push(Reply::from_zenoh(reply));
        }
        Ok(results)
    }

    /// Get the key expression for this querier.
    pub fn get_key_expr(&self) -> String {
        self.inner.key_expr().as_str().to_string()
    }
}

impl Querier {
    pub(crate) fn declare_on_session(
        session: &Arc<zenoh::Session>,
        key_expr: &str,
    ) -> Result<Self, String> {
        let querier = session
            .declare_querier(key_expr.to_string())
            .wait()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(querier),
        })
    }
}
