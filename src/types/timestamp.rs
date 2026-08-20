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

    /// Create a timestamp from milliseconds (saturating on overflow)
    pub fn from_millis(millis: i64) -> Self {
        Self {
            micros: millis.saturating_mul(1000),
        }
    }

    /// Create a timestamp from seconds (saturating on overflow)
    pub fn from_secs(secs: i64) -> Self {
        Self {
            micros: secs.saturating_mul(1_000_000),
        }
    }

    /// Get current timestamp
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        Self {
            micros: duration.as_micros() as i64,
        }
    }

    /// Get timestamp in microseconds
    pub fn as_micros(&self) -> i64 {
        self.micros
    }

    /// Get timestamp as u64 for indexing, clamping negative values to 0.
    /// Pre-epoch timestamps don't exist in practice, but this prevents
    /// silent corruption if a negative value leaks through.
    pub fn as_micros_u64(&self) -> u64 {
        self.micros.max(0) as u64
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

    /// Parse an ISO 8601 date/datetime string into a Timestamp.
    /// Supports: "2024-01-15", "2024-01-15 10:30:00", "2024-01-15T10:30:00",
    /// and integer microseconds. Returns None if the string doesn't match.
    /// Used for comparing TIMESTAMP columns against text literals in WHERE
    /// clauses (e.g. `WHERE ts > '2024-01-01'`).
    pub fn parse_iso(s: &str) -> Option<Timestamp> {
        // Try parsing as integer microseconds first (numeric timestamp).
        if let Ok(micros) = s.parse::<i64>() {
            return Some(Timestamp::from_micros(micros));
        }
        // Split date and optional time.
        let (date_part, time_part) = if let Some(idx) = s.find(['T', ' ']) {
            (&s[..idx], Some(&s[idx + 1..]))
        } else {
            (s, None)
        };
        // Parse date: YYYY-MM-DD
        let dparts: Vec<&str> = date_part.split('-').collect();
        if dparts.len() != 3 {
            return None;
        }
        let year: i32 = dparts[0].parse().ok()?;
        let month: u32 = dparts[1].parse().ok()?;
        let day: u32 = dparts[2].parse().ok()?;
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        // Parse time: HH:MM:SS (seconds optional)
        let (hour, min, sec) = if let Some(tp) = time_part {
            let tparts: Vec<&str> = tp.split(':').collect();
            let h: u32 = tparts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
            let m: u32 = tparts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let s: u32 = tparts
                .get(2)
                .and_then(|s| s.split('.').next().and_then(|n| n.parse().ok()))
                .unwrap_or(0);
            (h, m, s)
        } else {
            (0, 0, 0)
        };
        // Convert to Unix epoch microseconds using Howard Hinnant's algorithm.
        let days = days_from_civil(year, month, day)?;
        let micros = days as i64 * 86_400_000_000
            + hour as i64 * 3_600_000_000
            + min as i64 * 60_000_000
            + sec as i64 * 1_000_000;
        Some(Timestamp::from_micros(micros))
    }
}

/// Howard Hinnant's days_from_civil algorithm — converts (y, m, d) to days
/// since the Unix epoch (1970-01-01). Returns None for invalid dates.
fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i64> {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days = era as i64 * 146_097 + doe as i64 - 719_468;
    Some(days)
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
