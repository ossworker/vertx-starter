use boltffi::export;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use zenoh::Wait;

use crate::sample::Sample;
use crate::session::Session;

/// A Zenoh subscriber that receives data on a key expression.
/// Uses a callback-fed channel so Java/Kotlin side can poll samples via
/// `recv()` / `try_recv()`.
#[export]
pub struct Subscriber {
    /// Holds the live zenoh subscriber. Dropping it undeclares the subscription.
    inner: Arc<Mutex<Option<zenoh::pubsub::Subscriber<()>>>>,
    /// Receiver side of the sample channel fed by the subscriber callback.
    receiver: Arc<Mutex<Option<mpsc::Receiver<Sample>>>>,
    key_expr: String,
    undeclared: AtomicBool,
}

#[export]
impl Subscriber {
    /// Declare a subscriber on the given session.
    /// Equivalent to zenoh-java: session.declareSubscriber(keyExpr)
    ///
    /// Note: lives in Subscriber's `#[export]` block (returning `Result<Self, _>`)
    /// because boltffi 0.25 only allows `#[export]` methods to return `Self`
    /// for non-`#[data]` opaque types.
    pub fn declare(session: &Session, key_expr: String) -> Result<Self, String> {
        Self::declare_with_channel(session.inner(), &key_expr)
    }

    /// Get the key expression this subscriber is listening on.
    pub fn get_key_expr(&self) -> String {
        self.key_expr.clone()
    }

    /// Check if the subscriber has been undeclared.
    pub fn is_undeclared(&self) -> bool {
        self.undeclared.load(Ordering::Relaxed)
    }

    /// Undeclare this subscriber.
    /// Equivalent to zenoh-java: subscriber.undeclare()
    pub fn undeclare(&self) -> Result<(), String> {
        // Drop the receiver first to unblock any pending recv() callers.
        if let Ok(mut rx_guard) = self.receiver.lock() {
            rx_guard.take();
        }
        let mut guard = self.inner.lock().map_err(|e| e.to_string())?;
        if let Some(sub) = guard.take() {
            sub.undeclare().wait().map_err(|e| e.to_string())?;
        }
        self.undeclared.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Block waiting for the next sample.
    /// Returns None if the subscriber has been undeclared or the channel closed.
    pub fn recv(&self) -> Option<Sample> {
        // Clone the receiver out of the mutex while we wait, so we don't hold
        // the mutex across a blocking call.
        let rx = {
            let mut guard = self.receiver.lock().ok()?;
            guard.take()
        }?;
        let result = rx.recv().ok();
        // Restore the receiver if it is still usable.
        if let Ok(mut guard) = self.receiver.lock() {
            if guard.is_none() {
                *guard = Some(rx);
            }
        }
        result
    }

    /// Try to receive a sample without blocking.
    /// Returns None if no sample is currently available.
    pub fn try_recv(&self) -> Option<Sample> {
        let guard = self.receiver.lock().ok()?;
        let rx = guard.as_ref()?;
        rx.try_recv().ok()
    }
}

impl Subscriber {
    /// Declare a subscriber and return the wrapper plus the receiver-backed
    /// channel that gets samples from the underlying zenoh callback.
    pub(crate) fn declare_with_channel(
        session: &Arc<zenoh::Session>,
        key_expr: &str,
    ) -> Result<Self, String> {
        let (tx, rx) = mpsc::channel::<Sample>();

        let subscriber = session
            .declare_subscriber(key_expr.to_string())
            .callback(move |sample| {
                let s: Sample = sample.into();
                let _ = tx.send(s);
            })
            .wait()
            .map_err(|e| e.to_string())?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(subscriber))),
            receiver: Arc::new(Mutex::new(Some(rx))),
            key_expr: key_expr.to_string(),
            undeclared: AtomicBool::new(false),
        })
    }

    /// Declare a subscriber that forwards every sample into the given
    /// user-supplied callback (kept for non-FFI Rust callers).
    #[allow(dead_code)]
    pub(crate) fn declare_on_session_with_callback<F>(
        session: &Arc<zenoh::Session>,
        key_expr: &str,
        callback: F,
    ) -> Result<Self, String>
    where
        F: Fn(Sample) + Send + Sync + 'static,
    {
        let subscriber = session
            .declare_subscriber(key_expr.to_string())
            .callback(move |sample| {
                let s: Sample = sample.into();
                callback(s);
            })
            .wait()
            .map_err(|e| e.to_string())?;

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(subscriber))),
            receiver: Arc::new(Mutex::new(None)),
            key_expr: key_expr.to_string(),
            undeclared: AtomicBool::new(false),
        })
    }
}
