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

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

/// Write controller with token bucket rate limiting and L0 monitoring.
pub struct WriteController {
    /// Configuration
    config: WriteControllerConfig,

    /// Token bucket: available write tokens (can go negative = debt)
    tokens: AtomicI64,

    /// Last refill timestamp (nanos since controller creation)
    last_refill_ns: AtomicU64,

    /// Creation time for nanos calculation
    created_at: Instant,
}

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

    /// Config for high-throughput logging (100K writes/sec)
    pub fn for_logging() -> Self {
        Self {
            max_writes_per_sec: 100_000,
            l0_slowdown_threshold: 12,
            l0_stop_threshold: 24,
            slowdown_sleep_us: 100,
            max_retries: 5,
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

impl WriteController {
    /// Create a new write controller with the given config
    pub fn new(config: WriteControllerConfig) -> Self {
        let initial_tokens = if config.max_writes_per_sec > 0 {
            config.max_writes_per_sec as i64 // Start with 1 second worth of tokens
        } else {
            i64::MAX
        };

        Self {
            config,
            tokens: AtomicI64::new(initial_tokens),
            last_refill_ns: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Check if `count` writes are allowed, consuming tokens from the bucket.
    ///
    /// Returns:
    /// - `BackpressureSignal::Normal` if writes are allowed
    /// - `BackpressureSignal::SlowDown` if caller should sleep briefly
    /// - `BackpressureSignal::Stop` if writes should be rejected
    pub fn check(&self, count: usize) -> BackpressureSignal {
        if self.config.max_writes_per_sec == 0 {
            return BackpressureSignal::Normal;
        }

        // Refill tokens based on elapsed time
        self.refill_tokens();

        // Try to consume tokens
        let needed = count as i64;
        let current = self.tokens.load(Ordering::Relaxed);

        if current >= needed {
            // Enough tokens: consume and proceed
            self.tokens.fetch_sub(needed, Ordering::Relaxed);
            BackpressureSignal::Normal
        } else if current >= 0 {
            // Some tokens available but not enough: allow but signal slowdown
            self.tokens.fetch_sub(needed, Ordering::Relaxed);
            BackpressureSignal::SlowDown
        } else {
            // In debt: reject
            BackpressureSignal::Stop
        }
    }

    /// Check L0 SSTable count and return appropriate signal.
    /// Should be called alongside `check()` for full backpressure picture.
    pub fn check_l0(&self, l0_count: usize) -> BackpressureSignal {
        if l0_count >= self.config.l0_stop_threshold {
            BackpressureSignal::Stop
        } else if l0_count >= self.config.l0_slowdown_threshold {
            BackpressureSignal::SlowDown
        } else {
            BackpressureSignal::Normal
        }
    }

    /// Get combined backpressure signal from both rate limiter and L0 monitor.
    /// Takes the more restrictive signal.
    pub fn check_combined(&self, count: usize, l0_count: usize) -> BackpressureSignal {
        let rate_signal = self.check(count);
        let l0_signal = self.check_l0(l0_count);

        // Return the more restrictive signal
        match (rate_signal, l0_signal) {
            (BackpressureSignal::Stop, _) | (_, BackpressureSignal::Stop) => BackpressureSignal::Stop,
            (BackpressureSignal::SlowDown, _) | (_, BackpressureSignal::SlowDown) => BackpressureSignal::SlowDown,
            _ => BackpressureSignal::Normal,
        }
    }

    /// Sleep duration for SlowDown signal (in microseconds)
    pub fn slowdown_duration_us(&self) -> u64 {
        self.config.slowdown_sleep_us
    }

    /// Max retry attempts before giving up
    pub fn max_retries(&self) -> usize {
        self.config.max_retries
    }

    /// Refill tokens based on elapsed time (token bucket algorithm)
    fn refill_tokens(&self) {
        let now_ns = self.elapsed_nanos();
        let last_ns = self.last_refill_ns.load(Ordering::Relaxed);

        if now_ns <= last_ns {
            return;
        }

        let elapsed_ns = now_ns - last_ns;

        // Try to claim the refill slot (CAS to prevent double-refill)
        if self.last_refill_ns.compare_exchange(
            last_ns,
            now_ns,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ).is_err() {
            return; // Another thread already refilled
        }

        // Calculate tokens to add: (elapsed_ns * rate_per_sec) / 1_000_000_000
        let tokens_to_add = if self.config.max_writes_per_sec > 0 {
            ((elapsed_ns as u128 * self.config.max_writes_per_sec as u128) / 1_000_000_000) as i64
        } else {
            return;
        };

        // Cap at 2x burst rate to prevent token accumulation
        let max_tokens = (self.config.max_writes_per_sec * 2) as i64;
        let current = self.tokens.load(Ordering::Relaxed);
        let new_total = (current + tokens_to_add).min(max_tokens);
        self.tokens.store(new_total, Ordering::Relaxed);
    }

    /// Get elapsed nanoseconds since controller creation
    fn elapsed_nanos(&self) -> u64 {
        self.created_at.elapsed().as_nanos() as u64
    }

    /// Get current available tokens (for diagnostics)
    pub fn available_tokens(&self) -> i64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get the configuration
    pub fn config(&self) -> &WriteControllerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unlimited_rate() {
        let ctrl = WriteController::new(WriteControllerConfig::unlimited());
        // Should always be Normal when unlimited
        assert_eq!(ctrl.check(100_000), BackpressureSignal::Normal);
        assert_eq!(ctrl.check(1_000_000), BackpressureSignal::Normal);
    }

    #[test]
    fn test_l0_monitoring() {
        let ctrl = WriteController::new(WriteControllerConfig::for_robotics());

        assert_eq!(ctrl.check_l0(3), BackpressureSignal::Normal);
        assert_eq!(ctrl.check_l0(8), BackpressureSignal::SlowDown);
        assert_eq!(ctrl.check_l0(15), BackpressureSignal::Stop);
    }

    #[test]
    fn test_combined_signal() {
        let ctrl = WriteController::new(WriteControllerConfig::unlimited());

        // Rate is fine, L0 is fine
        assert_eq!(ctrl.check_combined(100, 2), BackpressureSignal::Normal);

        // Rate is fine, L0 is slow (8 >= slowdown_threshold=8)
        assert_eq!(ctrl.check_combined(100, 8), BackpressureSignal::SlowDown);

        // Rate is fine, L0 is stop (20 >= stop_threshold=16)
        assert_eq!(ctrl.check_combined(100, 20), BackpressureSignal::Stop);
    }

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
