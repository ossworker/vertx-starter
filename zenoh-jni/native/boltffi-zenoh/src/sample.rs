use boltffi::data;

/// A data sample received from zenoh.
/// Pure `#[data]` type — fields are wire-encoded and accessible through
/// generated bindings. No `#[export]` so boltffi treats it as a value type.
#[data]
pub struct Sample {
    pub key_expr: String,
    pub payload: Vec<u8>,
    pub encoding: String,
    pub kind: u8,  // 0 = PUT, 1 = DELETE
    pub timestamp: Option<String>,
    pub qos_priority: u8,
    pub qos_congestion_control: u8,
    pub qos_express: bool,
}

impl From<zenoh::sample::Sample> for Sample {
    fn from(s: zenoh::sample::Sample) -> Self {
        let kind = match s.kind() {
            zenoh::sample::SampleKind::Put => 0u8,
            zenoh::sample::SampleKind::Delete => 1u8,
        };

        let timestamp = s.timestamp().map(|ts| ts.to_string());

        let payload: Vec<u8> = s.payload().to_bytes().to_vec();

        Self {
            key_expr: s.key_expr().as_str().to_string(),
            payload,
            encoding: s.encoding().to_string(),
            kind,
            timestamp,
            qos_priority: s.priority() as u8,
            qos_congestion_control: match s.congestion_control() {
                zenoh::qos::CongestionControl::Drop => 0,
                zenoh::qos::CongestionControl::Block => 1,
                zenoh::qos::CongestionControl::BlockFirst => 2,
            },
            qos_express: s.express(),
        }
    }
}
