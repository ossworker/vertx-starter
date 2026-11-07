use boltffi::export;
use std::sync::Arc;

/// Key expression for pub/sub addressing
#[export]
pub struct KeyExpr {
    inner: Arc<zenoh::key_expr::KeyExpr<'static>>,
}

#[export]
impl KeyExpr {
    /// Create a new key expression
    pub fn new(expr: String) -> Result<Self, String> {
        let ke = zenoh::key_expr::KeyExpr::try_from(expr)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(ke),
        })
    }

    /// Get string representation
    pub fn as_str(&self) -> String {
        self.inner.as_str().to_string()
    }

    /// Check if this key expression intersects with another
    pub fn intersects(&self, other: &KeyExpr) -> bool {
        self.inner.intersects(other.inner.as_ref())
    }

    /// Check if this key expression includes another
    pub fn includes(&self, other: &KeyExpr) -> bool {
        self.inner.includes(other.inner.as_ref())
    }
}
