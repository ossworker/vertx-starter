use boltffi::export;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use zenoh::Wait;

use crate::query::Query;
use crate::session::Session;

/// A Zenoh queryable that answers queries on a key expression.
/// Equivalent to zenoh-java: Queryable
#[export]
pub struct Queryable {
    inner: Arc<Mutex<Option<zenoh::query::Queryable<()>>>>,
    receiver: Arc<Mutex<Option<mpsc::Receiver<Query>>>>,
    key_expr: String,
    undeclared: AtomicBool,
}

#[export]
impl Queryable {
    /// Declare a queryable on the given session.
    /// Equivalent to zenoh-java: session.declareQueryable(keyExpr)
    pub fn declare(session: &Session, key_expr: String) -> Result<Self, String> {
        Self::declare_on_session(session.inner(), &key_expr)
    }

    /// Get the key expression this queryable is registered on.
    pub fn get_key_expr(&self) -> String {
        self.key_expr.clone()
    }

    /// Check if the queryable has been undeclared.
    pub fn is_undeclared(&self) -> bool {
        self.undeclared.load(Ordering::Relaxed)
    }

    /// Undeclare this queryable.
    pub fn undeclare(&self) -> Result<(), String> {
        if let Ok(mut rx_guard) = self.receiver.lock() {
            rx_guard.take();
        }
        let mut guard = self.inner.lock().map_err(|e| e.to_string())?;
        if let Some(q) = guard.take() {
            q.undeclare().wait().map_err(|e| e.to_string())?;
        }
        self.undeclared.store(true, Ordering::Relaxed);
        Ok(())
    }
}

impl Queryable {
    /// Block waiting for the next query (used by Query::recv static factory).
    pub(crate) fn recv_query(&self) -> Option<Query> {
        // Take the receiver out of the mutex while we wait, so we don't hold
        // the mutex across a blocking call.
        let rx = {
            let mut guard = self.receiver.lock().ok()?;
            guard.take()
        }?;
        let result = rx.recv().ok();
        if let Ok(mut guard) = self.receiver.lock() {
            if guard.is_none() {
                *guard = Some(rx);
            }
        }
        result
    }

    /// Try to receive a query without blocking.
    pub(crate) fn try_recv_query(&self) -> Option<Query> {
        let guard = self.receiver.lock().ok()?;
        let rx = guard.as_ref()?;
        rx.try_recv().ok()
    }

    /// Declare a queryable with a channel-based receiver (internal).
    pub(crate) fn declare_on_session(
        session: &Arc<zenoh::Session>,
        key_expr: &str,
    ) -> Result<Self, String> {
        let (tx, rx) = mpsc::channel::<Query>();

        let queryable = session
            .declare_queryable(key_expr.to_string())
            .callback(move |query| {
                let q = Query::from_zenoh(query);
                let _ = tx.send(q);
            })
            .wait()
            .map_err(|e| e.to_string())?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(queryable))),
            receiver: Arc::new(Mutex::new(Some(rx))),
            key_expr: key_expr.to_string(),
            undeclared: AtomicBool::new(false),
        })
    }
}
