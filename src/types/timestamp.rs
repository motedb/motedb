//! Timestamp data type implementation

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Timestamp data type (microseconds since Unix epoch)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp {
    /// Microseconds since Unix epoch
    micros: i64,
}

impl Timestamp {
    /// Create a timestamp from microseconds
    pub fn from_micros(micros: i64) -> Self {
        Self { micros }
    }

    /// Create a timestamp from milliseconds
    pub fn from_millis(millis: i64) -> Self {
        Self {
            micros: millis * 1000,
        }
    }

    /// Create a timestamp from seconds
    pub fn from_secs(secs: i64) -> Self {
        Self {
            micros: secs * 1_000_000,
        }
    }

    /// Get current timestamp
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        
        Self {
            micros: duration.as_micros() as i64,
        }
    }

    /// Get timestamp in microseconds
    pub fn as_micros(&self) -> i64 {
        self.micros
    }

    /// Get timestamp in milliseconds
    pub fn as_millis(&self) -> i64 {
        self.micros / 1000
    }

    /// Get timestamp in seconds
    pub fn as_secs(&self) -> i64 {
        self.micros / 1_000_000
    }
    
    /// Get raw value (microseconds) - used for generic access
    pub fn value(&self) -> i64 {
        self.micros
    }

    /// Check if timestamp is in range
    pub fn in_range(&self, start: Timestamp, end: Timestamp) -> bool {
        self.micros >= start.micros && self.micros <= end.micros
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_creation() {
        let ts = Timestamp::from_secs(1000);
        assert_eq!(ts.as_secs(), 1000);
        assert_eq!(ts.as_millis(), 1_000_000);
        assert_eq!(ts.as_micros(), 1_000_000_000);
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Timestamp::from_secs(100);
        let ts2 = Timestamp::from_secs(200);
        
        assert!(ts1 < ts2);
        assert!(ts2 > ts1);
    }

    #[test]
    fn test_timestamp_range() {
        let start = Timestamp::from_secs(100);
        let end = Timestamp::from_secs(200);
        let middle = Timestamp::from_secs(150);
        let before = Timestamp::from_secs(50);
        
        assert!(middle.in_range(start, end));
        assert!(!before.in_range(start, end));
    }

    #[test]
    fn test_timestamp_now() {
        let ts = Timestamp::now();
        assert!(ts.as_secs() > 0);
    }
}
