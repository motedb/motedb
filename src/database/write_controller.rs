//! Write Controller — backpressure and rate limiting for high-frequency writes.
//!
//! # Architecture
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │              WriteController                  │
//! │                                               │
//! │  ┌─────────────┐   ┌─────────────────────┐   │
//! │  │ Token Bucket │   │ L0 SSTable Monitor  │   │
//! │  │ (rate limit) │   │ (compaction lag)    │   │
//! │  └──────┬──────┘   └──────────┬──────────┘   │
//! │         │                     │               │
//! │         └──────┬──────────────┘               │
//! │                ↓                              │
//! │  ┌──────────────────────────────────┐        │
//! │  │ BackpressureSignal               │        │
//! │  │  Normal → accept writes          │        │
//! │  │  SlowDown → sleep + retry        │        │
//! │  │  Stop → reject writes            │        │
//! │  └──────────────────────────────────┘        │
//! └──────────────────────────────────────────────┘
//! ```

/// Configuration for the write controller
#[derive(Debug, Clone)]
pub struct WriteControllerConfig {
    /// Maximum writes per second (0 = unlimited)
    pub max_writes_per_sec: u64,

    /// L0 SSTable count → SlowDown signal
    pub l0_slowdown_threshold: usize,

    /// L0 SSTable count → Stop signal
    pub l0_stop_threshold: usize,

    /// SlowDown sleep duration in microseconds
    pub slowdown_sleep_us: u64,

    /// Maximum retry attempts before rejecting
    pub max_retries: usize,
}

impl Default for WriteControllerConfig {
    fn default() -> Self {
        Self {
            max_writes_per_sec: 0,
            l0_slowdown_threshold: 8,
            l0_stop_threshold: 16,
            slowdown_sleep_us: 1000, // 1ms
            max_retries: 10,
        }
    }
}

impl WriteControllerConfig {
    /// Config for robotics (10K writes/sec)
    pub fn for_robotics() -> Self {
        Self {
            max_writes_per_sec: 10_000,
            l0_slowdown_threshold: 6,
            l0_stop_threshold: 12,
            slowdown_sleep_us: 500,
            max_retries: 20,
        }
    }

    /// Unlimited writes (no rate limiting, only L0 monitoring)
    pub fn unlimited() -> Self {
        Self {
            max_writes_per_sec: 0,
            ..Default::default()
        }
    }
}

/// Result of a backpressure check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureSignal {
    /// Normal operation, writes accepted
    Normal,
    /// Approaching limits, writes should slow down
    SlowDown,
    /// At capacity, writes should be rejected
    Stop,
}

impl std::fmt::Display for BackpressureSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "Normal"),
            Self::SlowDown => write!(f, "SlowDown"),
            Self::Stop => write!(f, "Stop"),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = WriteControllerConfig::default();
        assert_eq!(config.max_writes_per_sec, 0);
        assert_eq!(config.l0_slowdown_threshold, 8);
        assert_eq!(config.l0_stop_threshold, 16);
    }

    #[test]
    fn test_robotics_config() {
        let config = WriteControllerConfig::for_robotics();
        assert_eq!(config.max_writes_per_sec, 10_000);
        assert_eq!(config.l0_slowdown_threshold, 6);
        assert_eq!(config.l0_stop_threshold, 12);
    }

    #[test]
    fn test_signal_display() {
        assert_eq!(format!("{}", BackpressureSignal::Normal), "Normal");
        assert_eq!(format!("{}", BackpressureSignal::SlowDown), "SlowDown");
        assert_eq!(format!("{}", BackpressureSignal::Stop), "Stop");
    }
}
