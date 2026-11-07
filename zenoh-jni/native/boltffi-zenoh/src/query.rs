use boltffi::export;
use std::sync::{Arc, Mutex};
use zenoh::Wait;

use crate::queryable::Queryable;

/// Represents an incoming query received by a Queryable.
/// Provides methods to inspect the query and send replies.
#[export]
pub struct Query {
    inner: Arc<Mutex<Option<zenoh::query::Query>>>,
}

#[export]
impl Query {
    /// Block waiting for the next query on the given queryable.
    /// Returns Err if the queryable has been undeclared or the channel is closed.
    /// Equivalent to zenoh-java: queryable.receiver().recv()
    pub fn recv(queryable: &Queryable) -> Result<Self, String> {
        queryable
            .recv_query()
            .ok_or_else(|| "Queryable undeclared or channel closed".to_string())
    }

    /// Try to receive the next query without blocking.
    /// Returns Err if no query is currently available.
    pub fn try_recv(queryable: &Queryable) -> Result<Self, String> {
        queryable
            .try_recv_query()
            .ok_or_else(|| "No query available".to_string())
    }

    /// Get the key expression of this query.
    pub fn get_key_expr(&self) -> String {
        let guard = self.inner.lock().unwrap();
        match guard.as_ref() {
            Some(q) => q.key_expr().as_str().to_string(),
            None => String::new(),
        }
    }

    /// Get the query parameters (selector part after '?').
    pub fn get_parameters(&self) -> String {
        let guard = self.inner.lock().unwrap();
        match guard.as_ref() {
            Some(q) => q.parameters().as_str().to_string(),
            None => String::new(),
        }
    }

    /// Get the query payload if present.
    pub fn get_payload(&self) -> Option<Vec<u8>> {
        let guard = self.inner.lock().unwrap();
        guard
            .as_ref()
            .and_then(|q| q.payload().map(|p| p.to_bytes().to_vec()))
    }

    /// Reply to this query with a key expression and payload.
    /// Equivalent to zenoh-java: query.reply(keyExpr, payload)
    pub fn reply(&self, key_expr: String, payload: Vec<u8>) -> Result<(), String> {
        let guard = self.inner.lock().map_err(|e| e.to_string())?;
        match guard.as_ref() {
            Some(q) => q
                .reply(&key_expr, payload)
                .wait()
                .map_err(|e| e.to_string()),
            None => Err("Query already consumed".to_string()),
        }
    }

    /// Reply to this query with an error payload.
    /// Equivalent to zenoh-java: query.replyErr(payload)
    pub fn reply_err(&self, payload: Vec<u8>) -> Result<(), String> {
        let guard = self.inner.lock().map_err(|e| e.to_string())?;
        match guard.as_ref() {
            Some(q) => q.reply_err(payload).wait().map_err(|e| e.to_string()),
            None => Err("Query already consumed".to_string()),
        }
    }
}

impl Query {
    pub(crate) fn from_zenoh(query: zenoh::query::Query) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(query))),
        }
    }
}
