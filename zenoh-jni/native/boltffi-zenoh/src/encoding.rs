use boltffi::export;

/// Encoding descriptor for payload data
#[export]
pub struct Encoding {
    id: u16,
    schema: Option<String>,
}

#[export]
impl Encoding {
    /// Create encoding from id and optional schema
    pub fn new(id: u16, schema: Option<String>) -> Self {
        Self { id, schema }
    }

    /// Get the encoding ID
    pub fn get_id(&self) -> u16 {
        self.id
    }

    /// Get the optional schema
    pub fn get_schema(&self) -> Option<String> {
        self.schema.clone()
    }

    // Predefined constants as static methods
    pub fn text_plain() -> Self {
        Self { id: 1, schema: None }
    }

    pub fn application_octet_stream() -> Self {
        Self { id: 0, schema: None }
    }

    pub fn application_json() -> Self {
        Self { id: 4, schema: None }
    }

    pub fn text_json() -> Self {
        Self { id: 5, schema: None }
    }

    pub fn application_integer() -> Self {
        Self { id: 6, schema: None }
    }

    pub fn application_float() -> Self {
        Self { id: 7, schema: None }
    }

    pub fn application_xml() -> Self {
        Self { id: 14, schema: None }
    }

    pub fn application_yaml() -> Self {
        Self { id: 15, schema: None }
    }
}

impl From<&Encoding> for zenoh::bytes::Encoding {
    fn from(enc: &Encoding) -> Self {
        // Map our encoding IDs to zenoh encoding
        match enc.id {
            0 => zenoh::bytes::Encoding::APPLICATION_OCTET_STREAM,
            1 => zenoh::bytes::Encoding::TEXT_PLAIN,
            4 => zenoh::bytes::Encoding::APPLICATION_JSON,
            14 => zenoh::bytes::Encoding::APPLICATION_XML,
            15 => zenoh::bytes::Encoding::APPLICATION_YAML,
            _ => zenoh::bytes::Encoding::APPLICATION_OCTET_STREAM,
        }
    }
}

impl From<&zenoh::bytes::Encoding> for Encoding {
    fn from(enc: &zenoh::bytes::Encoding) -> Self {
        let s = enc.to_string();
        // Simple mapping based on string representation
        if s.contains("text/plain") {
            Self::text_plain()
        } else if s.contains("application/json") {
            Self::application_json()
        } else if s.contains("application/xml") {
            Self::application_xml()
        } else {
            Self::application_octet_stream()
        }
    }
}
