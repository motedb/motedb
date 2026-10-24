//! DATE data type — days since the Unix epoch (i32), calendar-accurate to
//! ±5.8M years. Introduced for calendar semantics (SQL `DATE` columns)
//! where TIMESTAMP's micro precision and timezone-naive rendering are the
//! wrong abstraction; comparisons and storage are plain i32 ordering.

use serde::{Deserialize, Serialize};

/// Days since 1970-01-01.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    days: i32,
}

const DAYS_PER_400Y: i32 = 146_097; // 400 years = 146097 days (Gregorian)

impl Date {
    pub fn from_days(days: i32) -> Self {
        Self { days }
    }

    pub fn days(&self) -> i32 {
        self.days
    }

    /// Parse `YYYY-MM-DD` (also accepts `YYYY-MM-DDTHH:MM:SS…`, truncating
    /// to the date part — parquet/pyarrow round-trips feed both).
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let date_part = s.split(['T', ' ']).next()?;
        let mut it = date_part.split('-');
        let y: i32 = it.next()?.parse().ok()?;
        let m: i32 = it.next()?.parse().ok()?;
        let d: i32 = it.next()?.parse().ok()?;
        if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        // 🔒 Per-month day validation via calendar round-trip: `2024-02-30`
        // and `2024-04-31` used to pass the flat 1..=31 gate (then silently
        // normalized to Mar 1 / May 1 by days_from_civil). A real calendar
        // date survives civil_from_days(days_from_civil(y, m, d)) unchanged.
        let (ry, rm, rd) = civil_from_days(days_from_civil(y, m, d));
        if (ry, rm, rd) != (y, m, d) {
            return None;
        }
        Some(Self {
            days: days_from_civil(y, m, d),
        })
    }

    /// Render as `YYYY-MM-DD` (proleptic Gregorian).
    pub fn to_iso(&self) -> String {
        let (y, m, d) = civil_from_days(self.days);
        format!("{y:04}-{m:02}-{d:02}")
    }

    pub fn year(&self) -> i32 {
        civil_from_days(self.days).0
    }
    pub fn month(&self) -> i32 {
        civil_from_days(self.days).1
    }
    pub fn day(&self) -> i32 {
        civil_from_days(self.days).2
    }

    pub fn today_utc() -> Self {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        Self {
            days: (secs.div_euclid(86_400)) as i32,
        }
    }
}

impl std::fmt::Display for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_iso())
    }
}

/// Howard Hinnant's `days_from_civil` — O(1), no leap-year loops.
fn days_from_civil(y: i32, m: i32, d: i32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // [0, 11] (Mar=0)
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * DAYS_PER_400Y + doe - 719_468 // shift epoch to 1970-01-01
}

/// Inverse of `days_from_civil`.
fn civil_from_days(z: i32) -> (i32, i32, i32) {
    let z = z + 719_468;
    let era = z.div_euclid(DAYS_PER_400Y);
    let doe = z - era * DAYS_PER_400Y; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_roundtrip() {
        assert_eq!(Date::from_days(0).to_iso(), "1970-01-01");
        for s in [
            "1970-01-01",
            "2026-10-22",
            "2000-02-29", // leap
            "1900-03-01", // non-leap century
            "0001-01-01",
            "9999-12-31",
        ] {
            let d = Date::parse(s).unwrap();
            assert_eq!(d.to_iso(), s, "roundtrip {s}");
        }
    }

    #[test]
    fn ordering_is_day_ordering() {
        assert!(Date::parse("2026-10-22").unwrap() > Date::parse("2026-10-21").unwrap());
        assert!(Date::parse("2025-01-1").is_some()); // lenient zero-pad
        assert_eq!(Date::parse("2026-1-1").unwrap().to_iso(), "2026-01-01");
    }

    #[test]
    fn truncates_datetime_strings() {
        assert_eq!(
            Date::parse("2026-10-22T23:59:59").unwrap().to_iso(),
            "2026-10-22"
        );
        assert_eq!(
            Date::parse("2026-10-22 10:00").unwrap().to_iso(),
            "2026-10-22"
        );
    }
}
