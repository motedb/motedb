//! TTL (Time-To-Live) Garbage Collection for time-series data.
//!
//! # Architecture
//! ```text
//! TTL GC Thread (runs every check_interval_secs)
//!     ↓
//! Scan table_registry for tables with TTL
//!     ↓
//! For each TimeSeries table with TTL:
//!     1. Calculate cutoff timestamp = now - TTL
//!     2. Scan timestamp index for row_ids < cutoff
//!     3. Batch delete via LSM delete_range()
//!     ↓
//! Reclaim disk space via compaction
//! ```

use crate::types::Timestamp;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for TTL garbage collection
#[derive(Debug, Clone)]
pub struct TTLGCConfig {
    /// How often to check for expired data (seconds)
    pub check_interval_secs: u64,
    /// Maximum rows to delete per GC cycle (limits CPU spike)
    pub max_rows_per_cycle: usize,
}

impl Default for TTLGCConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 60,
            max_rows_per_cycle: 10_000,
        }
    }
}

impl TTLGCConfig {
    /// Robotics preset: frequent checks, small batches
    pub fn for_robotics() -> Self {
        Self {
            check_interval_secs: 30,
            max_rows_per_cycle: 5_000,
        }
    }
}

/// TTL GC background thread handle
pub struct TTLGCThread {
    /// Thread handle
    handle: Option<std::thread::JoinHandle<()>>,
    /// Stop signal
    should_stop: Arc<AtomicBool>,
}

impl TTLGCThread {
    /// Start the TTL GC background thread.
    ///
    /// The thread periodically scans tables with TTL policies and
    /// deletes expired data using `delete_range()`.
    pub fn start(
        db: Arc<crate::MoteDB>,
        config: TTLGCConfig,
    ) -> Self {
        let should_stop = Arc::new(AtomicBool::new(false));
        let stop_clone = should_stop.clone();

        let handle = std::thread::Builder::new()
            .name("motedb-ttl-gc".to_string())
            .spawn(move || {
                ttl_gc_loop(db, config, stop_clone);
            })
            .expect("Failed to start TTL GC thread");

        Self {
            handle: Some(handle),
            should_stop,
        }
    }

    /// Stop the TTL GC thread
    pub fn stop(&mut self) {
        self.should_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TTLGCThread {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Main TTL GC loop
fn ttl_gc_loop(db: Arc<crate::MoteDB>, config: TTLGCConfig, should_stop: Arc<AtomicBool>) {
    loop {
        if should_stop.load(Ordering::Relaxed) {
            break;
        }

        std::thread::sleep(Duration::from_secs(config.check_interval_secs));

        if should_stop.load(Ordering::Relaxed) {
            break;
        }

        // Run GC cycle
        if let Err(e) = run_gc_cycle(&db, &config) {
            debug_log!("[TTL-GC] Error during GC cycle: {}", e);
        }
    }
}

/// Run one GC cycle: scan all tables with TTL and delete expired data
fn run_gc_cycle(db: &Arc<crate::MoteDB>, config: &TTLGCConfig) -> crate::Result<()> {
    let tables = db.table_registry.list_tables()?;
    let now_micros = Timestamp::now().as_micros() as u64;

    for table_name in &tables {
        let schema = match db.table_registry.get_table(table_name) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let ttl = match schema.ttl {
            Some(ttl) => ttl,
            None => continue,
        };

        let cutoff_micros = now_micros.saturating_sub(ttl.as_secs() * 1_000_000);

        // TimeSeries tables: use columnar store GC (O(1) segment file deletion)
        if schema.table_type == crate::types::TableType::TimeSeries {
            if let Err(e) = db.columnar_store.gc_expired(table_name, cutoff_micros as i64) {
                debug_log!("[TTL-GC] Columnar GC failed for '{}': {}", table_name, e);
            }
            continue;
        }

        // Get expired row_ids from timestamp index
        let expired_row_ids = {
            let ts_index = db.timestamp_index.read();
            ts_index.range(&0, &cutoff_micros)
                .unwrap_or_default()
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>()
        };

        if expired_row_ids.is_empty() {
            continue;
        }

        // Limit per cycle
        let to_delete: Vec<_> = expired_row_ids
            .into_iter()
            .take(config.max_rows_per_cycle)
            .collect();

        debug_log!(
            "[TTL-GC] Table '{}': deleting {} expired rows (TTL={})",
            table_name,
            to_delete.len(),
            ttl
        );

        // Batch delete via individual tombstones (for correctness across partitions)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Build composite keys for the table
        let mut count = 0;
        for row_id in to_delete {
            let composite_key = db.make_composite_key(table_name, row_id);
            db.lsm_engine.delete(composite_key, timestamp)?;
            count += 1;
        }

        if count > 0 {
            debug_log!("[TTL-GC] Table '{}': deleted {} rows", table_name, count);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TTLDuration;

    #[test]
    fn test_ttl_config_defaults() {
        let config = TTLGCConfig::default();
        assert_eq!(config.check_interval_secs, 60);
        assert_eq!(config.max_rows_per_cycle, 10_000);
    }

    #[test]
    fn test_ttl_config_robotics() {
        let config = TTLGCConfig::for_robotics();
        assert_eq!(config.check_interval_secs, 30);
        assert_eq!(config.max_rows_per_cycle, 5_000);
    }

    #[test]
    fn test_ttl_duration_display() {
        assert_eq!(format!("{}", TTLDuration::from_days(7)), "7d");
        assert_eq!(format!("{}", TTLDuration::from_hours(12)), "12h");
        assert_eq!(format!("{}", TTLDuration::from_mins(30)), "30m");
        assert_eq!(format!("{}", TTLDuration::from_secs(3600)), "1h");
    }
}
