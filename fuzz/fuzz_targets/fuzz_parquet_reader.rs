//! Fuzz target: Parquet reader (`IMPORT PARQUET`) on arbitrary file bytes.
//!
//! `src/export/parquet.rs` hands fuzzer-controlled bytes to the `parquet`
//! crate's `ParquetRecordBatchReaderBuilder` and then maps every Arrow type
//! into MoteDB rows (including fixed_size_list vectors, timestamps in four
//! units, nested lists, decimals). A truncated or hand-corrupted file must
//! surface as a clean `Err` — never a panic — the same contract the WAL
//! recovery target asserts. This is the crash-mid-write / hostile-file
//! scenario for the interop path added in §5 (Parquet round-trip).

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Write;
use std::path::PathBuf;

fuzz_target!(|data: &[u8]| {
    // Unique scratch dir per (process, iteration): parallel fuzz workers
    // must not collide, and the DB directory must start empty.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let mut dir: PathBuf = std::env::temp_dir();
    dir.push(format!("motedb_fuzz_pq_{}_{}", std::process::id(), n));
    std::fs::create_dir_all(&dir).ok();

    let pq_path = dir.join("in.parquet");
    if let Ok(mut f) = std::fs::File::create(&pq_path) {
        let _ = f.write_all(data);
    }

    // Import through the SQL statement — the real interop surface
    // (`IMPORT PARQUET '<path>' INTO t`). We do not inspect the result; the
    // contract is only "Ok or clean Err, no panic". Import auto-creates the
    // target table from the file schema when the file parses.
    let db = motedb::Database::create(dir.join("fuzz.mote"));
    if let Ok(db) = db {
        let stmt = format!("IMPORT PARQUET '{}' INTO fuzz_t", pq_path.display());
        let _ = db.execute(&stmt);
        // When import succeeded, reading everything back must also be
        // panic-free (decode side of the mapped Arrow types).
        let _ = db.execute("SELECT * FROM fuzz_t").and_then(|r| r.materialize());
    }

    let _ = std::fs::remove_dir_all(&dir);
});
