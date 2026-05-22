//! Transaction Recovery Types
//!
//! The active WAL recovery path lives in `src/database/core.rs` (search for
//! "Replay WAL records"). It replays committed WAL records into the LSM engine
//! and skips uncommitted data. Non-committed data in WAL is simply skipped,
//! which is correct for MoteDB's architecture where `insert_row_with_txn`
//! buffers in write_set and only flushes to LSM on commit.

/// Recovery report (reserved for future telemetry)
#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub total_wal_records: usize,
    pub committed_txns: usize,
    pub aborted_txns: usize,
    pub recovery_time_ms: u64,
    pub errors: Vec<String>,
}
