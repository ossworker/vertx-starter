use boltffi::data;

/// Congestion control strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[data]
pub enum CongestionControl {
    Drop = 0,
    Block = 1,
    BlockFirst = 2,
}

/// Message priority
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[data]
pub enum Priority {
    Background = 1,
    DataLow = 2,
    Data = 3,
    DataHigh = 4,
    InteractiveLow = 5,
    InteractiveHigh = 6,
    RealTime = 7,
}

/// Reliability level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[data]
pub enum Reliability {
    BestEffort = 0,
    Reliable = 1,
}

impl From<CongestionControl> for zenoh::qos::CongestionControl {
    fn from(cc: CongestionControl) -> Self {
        match cc {
            CongestionControl::Drop => zenoh::qos::CongestionControl::Drop,
            CongestionControl::Block => zenoh::qos::CongestionControl::Block,
            CongestionControl::BlockFirst => zenoh::qos::CongestionControl::BlockFirst,
        }
    }
}

impl From<zenoh::qos::CongestionControl> for CongestionControl {
    fn from(cc: zenoh::qos::CongestionControl) -> Self {
        match cc {
            zenoh::qos::CongestionControl::Drop => CongestionControl::Drop,
            zenoh::qos::CongestionControl::Block => CongestionControl::Block,
            zenoh::qos::CongestionControl::BlockFirst => CongestionControl::BlockFirst,
        }
    }
}

impl From<Priority> for zenoh::qos::Priority {
    fn from(p: Priority) -> Self {
        match p {
            Priority::Background => zenoh::qos::Priority::Background,
            Priority::DataLow => zenoh::qos::Priority::DataLow,
            Priority::Data => zenoh::qos::Priority::Data,
            Priority::DataHigh => zenoh::qos::Priority::DataHigh,
            Priority::InteractiveLow => zenoh::qos::Priority::InteractiveLow,
            Priority::InteractiveHigh => zenoh::qos::Priority::InteractiveHigh,
            Priority::RealTime => zenoh::qos::Priority::RealTime,
        }
    }
}

impl From<zenoh::qos::Priority> for Priority {
    fn from(p: zenoh::qos::Priority) -> Self {
        match p {
            zenoh::qos::Priority::Background => Priority::Background,
            zenoh::qos::Priority::DataLow => Priority::DataLow,
            zenoh::qos::Priority::Data => Priority::Data,
            zenoh::qos::Priority::DataHigh => Priority::DataHigh,
            zenoh::qos::Priority::InteractiveLow => Priority::InteractiveLow,
            zenoh::qos::Priority::InteractiveHigh => Priority::InteractiveHigh,
            zenoh::qos::Priority::RealTime => Priority::RealTime,
            _ => Priority::Data, // Default fallback
        }
    }
}
