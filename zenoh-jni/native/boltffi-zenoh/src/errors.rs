/// Unified error type for FFI boundary
#[derive(Debug, Clone)]
pub struct ZenohError {
    pub message: String,
}

impl ZenohError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { message: msg.into() }
    }
}

impl std::fmt::Display for ZenohError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ZenohError {}

impl From<zenoh::Error> for ZenohError {
    fn from(e: zenoh::Error) -> Self {
        Self::new(e.to_string())
    }
}

/// Helper to convert any error to String for FFI boundary
pub fn to_ffi_error<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}
