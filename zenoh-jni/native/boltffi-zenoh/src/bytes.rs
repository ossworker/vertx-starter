use boltffi::export;

/// Zero-copy bytes wrapper for FFI
#[export]
pub struct ZBytes {
    data: Vec<u8>,
}

#[export]
impl ZBytes {
    /// Create from byte array
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { data: bytes }
    }

    /// Create from string
    pub fn from_string(s: String) -> Self {
        Self { data: s.into_bytes() }
    }

    /// Get as byte array
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// Get as UTF-8 string (returns empty string if invalid UTF-8)
    pub fn to_utf8_string(&self) -> String {
        String::from_utf8_lossy(&self.data).to_string()
    }

    /// Get the length in bytes
    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl From<zenoh::bytes::ZBytes> for ZBytes {
    fn from(zb: zenoh::bytes::ZBytes) -> Self {
        let bytes: Vec<u8> = zb.to_bytes().to_vec();
        Self { data: bytes }
    }
}

impl From<&ZBytes> for zenoh::bytes::ZBytes {
    fn from(zb: &ZBytes) -> Self {
        zenoh::bytes::ZBytes::from(zb.data.clone())
    }
}
