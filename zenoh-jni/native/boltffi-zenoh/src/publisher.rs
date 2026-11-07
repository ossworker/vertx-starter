use boltffi::export;
use std::sync::Arc;
use zenoh::Wait;

use crate::session::Session;

/// A Zenoh publisher for a specific key expression.
/// Equivalent to zenoh-java: Publisher
#[export]
pub struct Publisher {
    inner: Arc<zenoh::pubsub::Publisher<'static>>,
}

#[export]
impl Publisher {
    /// Declare a publisher on the given session.
    /// Equivalent to zenoh-java: session.declarePublisher(keyExpr)
    ///
    /// Note: lives in Publisher's `#[export]` block (returning `Result<Self, _>`)
    /// because boltffi 0.25 only allows `#[export]` methods to return `Self`
    /// for non-`#[data]` opaque types.
    pub fn declare(
        session: &Session,
        key_expr: String,
        congestion_control: Option<u8>,
        priority: Option<u8>,
        express: Option<bool>,
        reliability: Option<u8>,
    ) -> Result<Self, String> {
        declare_publisher_on_session(
            session.inner(),
            &key_expr,
            congestion_control,
            priority,
            express,
            reliability,
        )
    }

    /// Put data through this publisher.
    /// Equivalent to zenoh-java: publisher.put(payload)
    pub fn put(&self, payload: Vec<u8>) -> Result<(), String> {
        self.inner.put(payload).wait().map_err(|e| e.to_string())
    }

    /// Put data with a specific encoding string (e.g. "text/plain", "application/json").
    pub fn put_with_encoding(&self, payload: Vec<u8>, encoding: String) -> Result<(), String> {
        self.inner
            .put(payload)
            .encoding(zenoh::bytes::Encoding::from(encoding.as_str()))
            .wait()
            .map_err(|e| e.to_string())
    }

    /// Delete data through this publisher.
    /// Equivalent to zenoh-java: publisher.delete()
    pub fn delete(&self) -> Result<(), String> {
        self.inner.delete().wait().map_err(|e| e.to_string())
    }

    /// Undeclare this publisher.
    /// Note: real undeclare requires consuming the inner publisher; the FFI
    /// model exposes only `&self`, so the publisher is effectively undeclared
    /// when its underlying handle is dropped.
    pub fn undeclare(&self) -> Result<(), String> {
        Ok(())
    }

    /// Get the key expression this publisher is associated with.
    pub fn get_key_expr(&self) -> String {
        self.inner.key_expr().as_str().to_string()
    }
}

impl Publisher {
    /// Create from a zenoh publisher (called internally by Session)
    pub(crate) fn from_zenoh(publisher: zenoh::pubsub::Publisher<'static>) -> Self {
        Self {
            inner: Arc::new(publisher),
        }
    }
}

/// Declare a publisher on a session for the given key expression.
/// This is a helper to be called from Session.
pub(crate) fn declare_publisher_on_session(
    session: &Arc<zenoh::Session>,
    key_expr: &str,
    congestion_control: Option<u8>,
    priority: Option<u8>,
    express: Option<bool>,
    reliability: Option<u8>,
) -> Result<Publisher, String> {
    let key_expr_owned = key_expr.to_string();
    let mut builder = session.declare_publisher(key_expr_owned);

    if let Some(cc) = congestion_control {
        builder = builder.congestion_control(match cc {
            1 => zenoh::qos::CongestionControl::Block,
            2 => zenoh::qos::CongestionControl::BlockFirst,
            _ => zenoh::qos::CongestionControl::Drop,
        });
    }

    if let Some(p) = priority {
        builder = builder.priority(match p {
            1 => zenoh::qos::Priority::Background,
            2 => zenoh::qos::Priority::DataLow,
            4 => zenoh::qos::Priority::DataHigh,
            5 => zenoh::qos::Priority::InteractiveLow,
            6 => zenoh::qos::Priority::InteractiveHigh,
            7 => zenoh::qos::Priority::RealTime,
            _ => zenoh::qos::Priority::Data,
        });
    }

    if let Some(exp) = express {
        builder = builder.express(exp);
    }

    if let Some(r) = reliability {
        builder = builder.reliability(match r {
            1 => zenoh::qos::Reliability::Reliable,
            _ => zenoh::qos::Reliability::BestEffort,
        });
    }

    let publisher = builder.wait().map_err(|e| e.to_string())?;

    Ok(Publisher::from_zenoh(publisher))
}
