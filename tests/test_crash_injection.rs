//! Kill -9 crash-injection loop: spawn a writer child, SIGKILL it at a
//! random moment mid-workload, reopen the database, and verify WAL recovery.
//!
//! The child is this same test binary re-exec'd with `--exact crash_child_mode`
//! (the classic self-exec pattern — no separate example/bin target to build),
//! driven by MOTEDB_CRASH_CHILD_* env vars.
//!
//! This is the durability contract an edge device (robot losing power, or a
//! supervisor OOM-killing the process) actually depends on:
//!   1. Prefix:     committed rows form a contiguous prefix 0..=k — a row is
//!                  never partially applied and a later row never survives an
//!                  earlier lost one (transaction ordering).
//!   2. Durability: every write the database ACKNOWLEDGED (execute() returned
//!                  Ok, recorded in the sidecar journal by the child before
//!                  the kill) is present with its exact payload. Torn values
//!                  or lost acks are durability bugs.
//!
//! SIGKILL semantics: the OS page cache survives process death, so anything
//! the child wrote via write(2) — WAL records and journal lines alike — is
//! observable after the kill. This tests process-crash recovery (the common
//! case), not power loss.
//!
//! CI budget: ~12 iterations at ~50-400ms each. Raise with MOTEDB_CRASH_ITERS
//! for longer local soaks (e.g. MOTEDB_CRASH_ITERS=200).

use std::io::{Read as _, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

// ── Child mode ────────────────────────────────────────────────────────────

/// Runs only when the binary is re-exec'd by the parent test with the
/// MOTEDB_CRASH_CHILD_DB env var set. Performs the insert workload, acking
/// each acknowledged write to the journal before continuing.
#[test]
fn crash_child_mode() {
    let db_path: PathBuf = match std::env::var_os("MOTEDB_CRASH_CHILD_DB") {
        Some(p) => p.into(),
        None => return, // normal suite run — not a child invocation
    };
    let journal_path: PathBuf = std::env::var_os("MOTEDB_CRASH_CHILD_JOURNAL")
        .expect("journal env")
        .into();
    let iterations: usize = std::env::var("MOTEDB_CRASH_CHILD_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(_) => Database::create(&db_path).expect("create db"),
    };
    db.execute(
        "CREATE TABLE IF NOT EXISTS crash_rows \
         (id INTEGER PRIMARY KEY, tag TEXT, val INTEGER)",
    )
    .expect("create table");

    let mut journal = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .expect("open journal");

    for i in 0..iterations {
        db.execute(&format!(
            "INSERT INTO crash_rows VALUES ({i}, 'row-{i}', {})",
            i as i64 * 7 + 3
        ))
        .expect("insert survived ack");
        // Ack AFTER execute() returned Ok. One write syscall per line — the
        // kernel page cache keeps it across SIGKILL.
        writeln!(journal, "{i}").expect("journal append");
        journal.flush().expect("journal flush");
    }

    let _ = db.close();
    std::process::exit(0);
}

// ── Parent: the kill loop ─────────────────────────────────────────────────

fn expected_val(i: i64) -> i64 {
    i * 7 + 3
}

#[test]
fn test_kill9_mid_write_recovers_with_prefix_and_durability() {
    let iterations: usize = std::env::var("MOTEDB_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);

    let mut rng_state: u64 = 0x9E37_79B9_7F4A_7C15; // fixed seed: reproducible
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        ((*state % 120) + 5) as u64 // 5..125ms
    };

    let mut killed_runs = 0;
    let mut completed_runs = 0;

    for iter in 0..iterations {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("crash.mote");
        let journal_path = dir.path().join("acked.txt");

        let mut child = Command::new(std::env::current_exe().expect("current exe"))
            .arg("crash_child_mode")
            .arg("--exact")
            .env("MOTEDB_CRASH_CHILD_DB", &db_path)
            .env("MOTEDB_CRASH_CHILD_JOURNAL", &journal_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn crash child");

        let delay = Duration::from_millis(next_delay_ms(&mut rng_state));
        std::thread::sleep(delay);

        // SIGKILL (TerminateProcess on Windows). If the child already
        // finished all 10k rows within the delay, this is a no-crash control
        // run — still verify the invariants.
        let was_killed = child.kill().is_ok();
        let _ = child.wait();

        // ── Reopen and verify ────────────────────────────────────────────
        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(e) => panic!("iter {}: reopen after crash failed: {}", iter, e),
        };

        // The kill may have landed before CREATE TABLE's atomic catalog
        // persist completed. With an empty journal nothing was acked, so a
        // missing table is legitimate (nothing was promised). With acked
        // inserts, a missing table WOULD be a durability bug — fall through
        // and let the query fail loudly in that case.
        let acked_early = read_journal(&journal_path);
        let rows = match db.query("SELECT id, tag, val FROM crash_rows ORDER BY id") {
            Ok(rows) => rows,
            Err(e) if e.to_string().contains("not found") && acked_early.is_empty() => {
                drop(db);
                killed_runs += if was_killed { 1 } else { 0 };
                continue;
            }
            Err(e) => panic!("iter {}: query after recovery failed: {}", iter, e),
        };

        let mut rows = rows;

        rows.sort_by_key(|r| match r.first() {
            Some(Value::Integer(i)) => *i,
            _ => i64::MIN,
        });

        // Invariant 1: contiguous prefix 0..=k, exact payloads.
        for (k, row) in rows.iter().enumerate() {
            let id = k as i64;
            assert_eq!(
                row,
                &vec![
                    Value::Integer(id),
                    Value::text(format!("row-{}", id)),
                    Value::Integer(expected_val(id)),
                ],
                "iter {}: row {} corrupted after crash recovery",
                iter,
                id
            );
        }
        let stored_k = rows.len() as i64 - 1; // max id, or -1 if empty

        // Invariant 2: every acked write survived (journal already read above
        // as acked_early — includes torn-tail filtering).
        if let Some(&max_acked) = acked_early.last() {
            assert!(
                stored_k >= max_acked,
                "iter {}: durability violation — journal acked up to id {} but DB only has up to {}",
                iter,
                max_acked,
                stored_k
            );
        }

        drop(db);

        if was_killed {
            killed_runs += 1;
        } else {
            completed_runs += 1;
        }
    }

    // The test is only meaningful if we actually killed the child in at
    // least some runs (delays are short enough that this always holds, but
    // assert so a silent regression to no-op doesn't pass unnoticed).
    assert!(
        killed_runs > 0,
        "never managed to kill the child — test degraded to a no-op"
    );
    eprintln!(
        "crash injection done: {} killed, {} completed-early (of {})",
        killed_runs, completed_runs, iterations
    );
}

/// Read the sidecar journal of acknowledged ids (one per line, in order).
fn read_journal(path: &std::path::Path) -> Vec<i64> {
    let mut s = String::new();
    match std::fs::File::open(path) {
        Ok(mut f) => {
            f.read_to_string(&mut s).expect("journal readable");
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => panic!("journal read failed: {}", e),
    }
    // The final line may be torn (kill mid-write) — parse only complete
    // lines; a torn line means the ack never completed, so it makes no
    // durability promise.
    s.lines()
        .filter(|l| l.parse::<i64>().is_ok())
        .map(|l| l.parse::<i64>().unwrap())
        .collect()
}
