//! Transaction Recovery
//!
//! The active WAL recovery path lives in `src/database/core.rs` (search for
//! "Replay WAL records"). It replays committed WAL records into the LSM engine
//! and skips uncommitted data.
