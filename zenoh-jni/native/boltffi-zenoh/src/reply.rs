use boltffi::data;

/// A reply received from a query (get operation).
/// Can be either a success (contains Sample) or an error.
/// Pure `#[data]` type — fields are wire-encoded and accessible through
/// generated bindings. No `#[export]` so boltffi treats it as a value type.
#[data]
pub struct Reply {
    pub success: bool,
    // Success fields
    pub sample_key_expr: Option<String>,
    pub sample_payload: Option<Vec<u8>>,
    pub sample_encoding: Option<String>,
    pub sample_kind: Option<u8>,
    pub sample_timestamp: Option<String>,
    // Error fields
    pub error_payload: Option<Vec<u8>>,
    pub error_encoding: Option<String>,
}

impl Reply {
    pub(crate) fn from_zenoh(reply: zenoh::query::Reply) -> Self {
        match reply.result() {
            Ok(sample) => {
                let kind = match sample.kind() {
                    zenoh::sample::SampleKind::Put => 0u8,
                    zenoh::sample::SampleKind::Delete => 1u8,
                };
                Self {
                    success: true,
                    sample_key_expr: Some(sample.key_expr().as_str().to_string()),
                    sample_payload: Some(sample.payload().to_bytes().to_vec()),
                    sample_encoding: Some(sample.encoding().to_string()),
                    sample_kind: Some(kind),
                    sample_timestamp: sample.timestamp().map(|ts| ts.to_string()),
                    error_payload: None,
                    error_encoding: None,
                }
            }
            Err(err) => Self {
                success: false,
                sample_key_expr: None,
                sample_payload: None,
                sample_encoding: None,
                sample_kind: None,
                sample_timestamp: None,
                error_payload: Some(err.payload().to_bytes().to_vec()),
                error_encoding: Some(err.encoding().to_string()),
            },
        }
    }
}
