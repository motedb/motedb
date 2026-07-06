//! Fuzz target: WAL recovery on arbitrary file bytes.
//!
//! The WAL (`src/txn/wal.rs`) persists records in a binary frame format:
//!   [u32 total_len][u64 lsn][u32 crc32c][u32 record_len][record bytes...]
//! On open/recover it reads frames, verifies CRC32C, and decodes records,
//! skipping corrupted frames. This target writes arbitrary bytes to a `.wal`
//! file and runs the recovery path, asserting:
//!   - no panic / abort / out-of-bounds (the main risk for a binary parser on
//!     a truncated/corrupted file, e.g. a crash mid-write)
//!   - recover() either returns Ok or a clean Err, never unwinds.
//!
//! This simulates the real crash-recovery scenario: a power loss mid-write
//! leaves a partially-written, corrupted WAL on disk, and the next open must
//! not crash — it must skip the bad tail and continue.

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Write;
use std::path::PathBuf;

fuzz_target!(|data: &[u8]| {
    // Build a unique path under the OS temp dir. We never read the recovered
    // records — we only care that the recovery path doesn't panic. The path is
    // unique per (process, iteration) so concurrent fuzz workers don't collide.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let mut wal_path: PathBuf = std::env::temp_dir();
    wal_path.push(format!("motedb_fuzz_wal_{}_{}", std::process::id(), n));
    std::fs::create_dir_all(&wal_path).ok();

    // WALManager expects partition files under <base>/wal/part_*.wal. We write
    // the fuzzer bytes directly as partition 0's content — recovery reads it.
    let wal_dir = wal_path.join("wal");
    std::fs::create_dir_all(&wal_dir).ok();
    let part_path = wal_dir.join("part_0.wal");

    // Write the arbitrary bytes as the WAL file content.
    if let Ok(mut f) = std::fs::File::create(&part_path) {
        let _ = f.write_all(data);
        let _ = f.sync_all();
    }

    // Open the WAL manager (1 partition) — this runs the per-partition open
    // which scans frames, verifies CRCs, and decodes records. Must not panic.
    let manager = match motedb::txn::WALManager::open(&wal_path, 1) {
        Ok(m) => m,
        Err(_) => return, // an open error on corrupted bytes is acceptable
    };

    // recover() re-reads and decodes all records. Must not panic.
    let _ = manager.recover();

    // Best-effort cleanup; ignore errors (OS reaps its temp dir eventually).
    let _ = manager.shutdown();
    let _ = std::fs::remove_dir_all(&wal_path);
});
