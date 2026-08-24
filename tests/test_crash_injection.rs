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
    let mode = std::env::var("MOTEDB_CRASH_CHILD_MODE").unwrap_or_default();

    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(_) => Database::create(&db_path).expect("create db"),
    };
    let mut journal = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .expect("open journal");

    // Ack AFTER execute()/commit returned Ok. One write syscall per line —
    // the kernel page cache keeps it across SIGKILL.
    match mode.as_str() {
        // INSERT-only: sequential ids, fixed payload.
        "" | "insert" => {
            db.execute(
                "CREATE TABLE IF NOT EXISTS crash_rows \
                 (id INTEGER PRIMARY KEY, tag TEXT, val INTEGER)",
            )
            .expect("create table");
            for i in 0..iterations {
                db.execute(&format!(
                    "INSERT INTO crash_rows VALUES ({i}, 'row-{i}', {})",
                    i as i64 * 7 + 3
                ))
                .expect("insert survived ack");
                writeln!(journal, "{i}").expect("journal append");
                journal.flush().expect("journal flush");
            }
        }
        // Mixed INSERT/UPDATE/DELETE: exercises WAL replay of updates
        // (newest-wins re-append) and delete tombstones. Journal records the
        // op sequence; the parent verifies the recovered state equals some
        // simulation prefix that covers every acked op.
        "update_delete" => {
            db.execute("CREATE TABLE IF NOT EXISTS crash_ud (id INTEGER PRIMARY KEY, val INTEGER)")
                .expect("create table");
            for i in 0..iterations {
                db.execute(&format!("INSERT INTO crash_ud VALUES ({i}, 0)"))
                    .expect("insert");
                writeln!(journal, "I {i}").unwrap();
                journal.flush().unwrap();
                db.execute(&format!("UPDATE crash_ud SET val = val + 1 WHERE id = {i}"))
                    .expect("update 1");
                writeln!(journal, "U {i}").unwrap();
                journal.flush().unwrap();
                db.execute(&format!("UPDATE crash_ud SET val = val + 1 WHERE id = {i}"))
                    .expect("update 2");
                writeln!(journal, "U {i}").unwrap();
                journal.flush().unwrap();
                // Delete a row a few iterations back (its updates already
                // acked) — kill windows around this test resurrection.
                if i >= 3 && i % 3 == 0 {
                    let victim = i - 3;
                    db.execute(&format!("DELETE FROM crash_ud WHERE id = {victim}"))
                        .expect("delete");
                    writeln!(journal, "D {victim}").unwrap();
                    journal.flush().unwrap();
                }
            }
        }
        // Explicit transactions: 5 rows per BEGIN/COMMIT. The parent
        // verifies per-transaction atomicity (all 5 rows or none) plus a
        // contiguous prefix of committed transactions.
        "txn" => {
            db.execute(
                "CREATE TABLE IF NOT EXISTS crash_txn (id INTEGER PRIMARY KEY, txn INTEGER)",
            )
            .expect("create table");
            for j in 0..iterations {
                db.execute("BEGIN").expect("begin");
                let base = j * 5;
                db.execute(&format!(
                    "INSERT INTO crash_txn VALUES ({base}, {j}), ({}, {j}), ({}, {j}), ({}, {j}), ({}, {j})",
                    base + 1, base + 2, base + 3, base + 4
                ))
                .expect("batch insert");
                db.execute("COMMIT").expect("commit");
                writeln!(journal, "T {j}").unwrap();
                journal.flush().unwrap();
            }
        }
        // TimeSeries workload: monotonically increasing timestamps at a fixed
        // step. The parent verifies a contiguous timestamp prefix with exact
        // values (exercises the columnar_store.replay_row recovery path,
        // which the other modes never touch).
        "ts" => {
            db.execute("CREATE TABLE IF NOT EXISTS m (ts TIMESTAMP, v FLOAT) TIMESERIES(ts)")
                .expect("create table");
            for i in 0..iterations {
                let ts = 1_000_000i64 * (i as i64 + 1);
                db.execute(&format!("INSERT INTO m VALUES ({ts}, {})", i as f64 / 7.0))
                    .expect("insert");
                writeln!(journal, "{i}").unwrap();
                journal.flush().unwrap();
            }
        }
        // Concurrent multi-writer: 3 threads, disjoint PK ranges
        // (t*100_000 + i). Each thread acks to its OWN journal file
        // (acked.txt.t{t}) so appends never interleave. The parent verifies
        // a contiguous per-thread prefix and acked durability.
        "concurrent" => {
            db.execute(
                "CREATE TABLE IF NOT EXISTS cw (id INTEGER PRIMARY KEY, w INTEGER, val INTEGER)",
            )
            .expect("create table");
            let db_arc = std::sync::Arc::new(db);
            let journal_base: std::path::PathBuf = journal_path.clone();
            let mut handles = Vec::new();
            for t in 0..3i64 {
                let db = std::sync::Arc::clone(&db_arc);
                let jp = journal_base.with_file_name(format!(
                    "{}.t{t}",
                    journal_base.file_name().unwrap().to_string_lossy()
                ));
                handles.push(std::thread::spawn(move || {
                    let mut j = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(jp)
                        .expect("per-thread journal");
                    for i in 0..iterations as i64 {
                        let id = t * 100_000 + i;
                        db.execute(&format!(
                            "INSERT INTO cw VALUES ({id}, {t}, {})",
                            t * 1000 + i
                        ))
                        .expect("insert");
                        writeln!(j, "{id}").unwrap();
                        j.flush().unwrap();
                    }
                }));
            }
            for h in handles {
                h.join().expect("writer thread");
            }
            // Last Arc reference — close cleanly if we were never killed.
            if let Some(db) = std::sync::Arc::try_unwrap(db_arc).ok() {
                let _ = db.close();
            }
            std::process::exit(0);
        }
        // Upsert workload: 8 keys, each round does ON CONFLICT DO UPDATE
        // accumulate (+1), OR REPLACE (rewrite), OR IGNORE (no-op) in a
        // deterministic rotation. Journal "U k" / "R k" per op; the parent
        // simulates prefixes and requires the recovered state to match one.
        "upsert" => {
            db.execute("CREATE TABLE IF NOT EXISTS u (id INTEGER PRIMARY KEY, val INTEGER)")
                .expect("create table");
            // Base rows.
            for k in 0..8i64 {
                db.execute(&format!("INSERT INTO u VALUES ({k}, 0)"))
                    .expect("base insert");
                writeln!(journal, "I {k}").unwrap();
                journal.flush().unwrap();
            }
            for i in 0..iterations as i64 {
                let k = i % 8;
                match (i / 8) % 3 {
                    0 => {
                        db.execute(&format!(
                            "INSERT INTO u VALUES ({k}, 1) \
                             ON CONFLICT (id) DO UPDATE SET val = val + excluded.val"
                        ))
                        .expect("upsert accum");
                        writeln!(journal, "U {k}").unwrap();
                    }
                    1 => {
                        db.execute(&format!("INSERT OR REPLACE INTO u VALUES ({k}, 5)"))
                            .expect("replace");
                        writeln!(journal, "R {k}").unwrap();
                    }
                    _ => {
                        db.execute(&format!("INSERT OR IGNORE INTO u VALUES ({k}, -100)"))
                            .expect("ignore");
                        writeln!(journal, "G {k}").unwrap();
                    }
                }
                journal.flush().unwrap();
            }
        }
        other => panic!("unknown crash child mode: {other}"),
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

// ── Parent: UPDATE/DELETE workload ────────────────────────────────────────

/// Spawn the child in `mode`, sleep, SIGKILL. Returns whether the kill landed
/// before the child completed.
fn spawn_kill_child(
    db_path: &std::path::Path,
    journal_path: &std::path::Path,
    delay_ms: u64,
    mode: &str,
) -> bool {
    let mut child = Command::new(std::env::current_exe().expect("current exe"))
        .arg("crash_child_mode")
        .arg("--exact")
        .env("MOTEDB_CRASH_CHILD_DB", db_path)
        .env("MOTEDB_CRASH_CHILD_JOURNAL", journal_path)
        .env("MOTEDB_CRASH_CHILD_MODE", mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn crash child");
    std::thread::sleep(Duration::from_millis(delay_ms));
    let killed = child.kill().is_ok();
    let _ = child.wait();
    killed
}

/// Deterministic op sequence of the child's update_delete mode: per id i
/// → I i, U i, U i; and D (i-3) whenever i >= 3 && i % 3 == 0. Mirrors the
/// child loop in crash_child_mode — keep in sync.
fn ud_sequence_upto(n_iters: usize, limit: usize) -> Vec<(u8, i64)> {
    let mut v = Vec::with_capacity(limit);
    for i in 0..n_iters as i64 {
        for op in [b'I', b'U', b'U'] {
            v.push((op, i));
            if v.len() >= limit {
                return v;
            }
        }
        if i >= 3 && i % 3 == 0 {
            v.push((b'D', i - 3));
            if v.len() >= limit {
                return v;
            }
        }
    }
    v
}

#[test]
fn test_kill9_update_delete_recovers_exactly() {
    let iterations: usize = std::env::var("MOTEDB_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    let mut rng_state: u64 = 0x0123_4567_89AB_CDEF;
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        ((*state % 150) + 5) as u64
    };

    let mut killed_runs = 0;
    for iter in 0..iterations {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("crash.mote");
        let journal_path = dir.path().join("acked.txt");
        let delay = next_delay_ms(&mut rng_state);
        if spawn_kill_child(&db_path, &journal_path, delay, "update_delete") {
            killed_runs += 1;
        }

        let journal = read_journal_lines(&journal_path);
        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(e) => panic!("iter {}: reopen failed: {}", iter, e),
        };
        // Kill may precede CREATE TABLE — fine iff nothing was acked.
        let rows: std::collections::HashMap<i64, i64> =
            match db.query("SELECT id, val FROM crash_ud") {
                Ok(rs) => rs
                    .into_iter()
                    .filter_map(|r| match (r.first(), r.get(1)) {
                        (Some(Value::Integer(i)), Some(Value::Integer(v))) => Some((*i, *v)),
                        _ => None,
                    })
                    .collect(),
                Err(e) if e.to_string().contains("not found") && journal.is_empty() => {
                    drop(db);
                    continue;
                }
                Err(e) => panic!("iter {}: query failed: {}", iter, e),
            };
        drop(db);

        // Acked op count = complete journal lines ("I n"/"U n"/"D n").
        let acked = journal
            .iter()
            .filter(|l| matches!(l.split_whitespace().next(), Some("I" | "U" | "D")))
            .count();

        // The op sequence is DETERMINISTIC (child and this test share the
        // file — keep them in sync), so we can simulate prefixes beyond the
        // journal: an op may have executed and reached the WAL while its
        // journal ack line was still torn. Simulate up to acked + 2.
        let ops = ud_sequence_upto(10_000, acked + 3);
        let mut state: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        let mut states = vec![state.clone()];
        for (op, id) in ops.iter().take(acked + 3) {
            match op {
                b'I' => {
                    state.insert(*id, 0);
                }
                b'U' => {
                    *state.entry(*id).or_insert(0) += 1;
                }
                b'D' => {
                    state.remove(id);
                }
                _ => {}
            }
            states.push(state.clone());
        }
        let upper = std::cmp::min(acked + 2, states.len() - 1);
        let matched = (acked..=upper).find(|&k| states[k] == rows);
        assert!(
            matched.is_some(),
            "iter {}: recovered state ({:?}, {} rows) matches no simulation prefix in [{}, {}] around {} acked ops — lost/duplicated/phantom writes after crash",
            iter,
            rows,
            rows.len(),
            acked,
            upper,
            acked
        );
    }
    assert!(
        killed_runs > 0,
        "never killed the child — test degraded to a no-op"
    );
    eprintln!("update_delete crash injection: {killed_runs} kills verified");
}

// ── Parent: explicit-transaction workload ─────────────────────────────────

#[test]
fn test_kill9_txn_atomicity_and_prefix() {
    let iterations: usize = std::env::var("MOTEDB_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let mut rng_state: u64 = 0xF00D_BEEF_CAFE_1234;
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        ((*state % 200) + 10) as u64
    };

    let mut killed_runs = 0;
    for iter in 0..iterations {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("crash.mote");
        let journal_path = dir.path().join("acked.txt");
        let delay = next_delay_ms(&mut rng_state);
        if spawn_kill_child(&db_path, &journal_path, delay, "txn") {
            killed_runs += 1;
        }

        let journal = read_journal_lines(&journal_path);
        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(e) => panic!("iter {}: reopen failed: {}", iter, e),
        };
        let rows = match db.query("SELECT txn FROM crash_txn") {
            Ok(rs) => rs,
            Err(e) if e.to_string().contains("not found") && journal.is_empty() => {
                drop(db);
                continue;
            }
            Err(e) => panic!("iter {}: query failed: {}", iter, e),
        };
        drop(db);

        // Group row counts per txn tag.
        let mut counts: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
        for r in &rows {
            if let Some(Value::Integer(j)) = r.first() {
                *counts.entry(*j).or_insert(0) += 1;
            }
        }
        // Atomicity: every partial group is a bug (torn transaction).
        for (j, n) in &counts {
            assert!(
                *n == 5 || *n == 0,
                "iter {}: txn {} has {} rows (expected 0 or 5) — torn transaction after crash",
                iter,
                j,
                n
            );
        }
        // Prefix: full groups form exactly {0..=m}.
        let max_full = counts
            .iter()
            .filter(|(_, &n)| n == 5)
            .map(|(&j, _)| j)
            .max();
        if let Some(m) = max_full {
            for j in 0..=m {
                assert_eq!(
                    counts.get(&j).copied().unwrap_or(0),
                    5,
                    "iter {}: txn {} missing but txn {m} present — hole in committed prefix",
                    iter,
                    j
                );
            }
        }
        // Durability: every acked COMMIT is fully present.
        for line in &journal {
            if let Some(j) = line.strip_prefix("T ") {
                let j: i64 = j.parse().unwrap();
                assert_eq!(
                    counts.get(&j).copied().unwrap_or(0),
                    5,
                    "iter {}: acked txn {j} lost after crash",
                    iter
                );
            }
        }
    }
    assert!(
        killed_runs > 0,
        "never killed the child — test degraded to a no-op"
    );
    eprintln!("txn crash injection: {killed_runs} kills verified");
}

// ── Parent: TimeSeries workload ────────────────────────────────────────────

#[test]
fn test_kill9_timeseries_prefix_recovery() {
    let iterations: usize = std::env::var("MOTEDB_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    let mut rng_state: u64 = 0xAAAA_BBBB_CCCC_DDDD;
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        ((*state % 150) + 5) as u64
    };

    let mut killed_runs = 0;
    for iter in 0..iterations {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("crash.mote");
        let journal_path = dir.path().join("acked.txt");
        let delay = next_delay_ms(&mut rng_state);
        if spawn_kill_child(&db_path, &journal_path, delay, "ts") {
            killed_runs += 1;
        }

        let journal = read_journal_lines(&journal_path);
        let acked: usize = journal
            .iter()
            .filter(|l| l.parse::<usize>().is_ok())
            .count();

        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(e) => panic!("iter {}: reopen failed: {}", iter, e),
        };
        let rs = match db.query("SELECT ts, v FROM m ORDER BY ts") {
            Ok(rs) => rs,
            Err(e) if e.to_string().contains("not found") && journal.is_empty() => {
                drop(db);
                continue;
            }
            Err(e) => panic!("iter {}: query failed: {}", iter, e),
        };

        // Prefix: rows are ts = step×(i+1) for a contiguous i-prefix, exact v.
        for (k, row) in rs.iter().enumerate() {
            let i = k as i64;
            let want_ts = 1_000_000 * (i + 1);
            let want_v = i as f64 / 7.0;
            match (&row[0], &row[1]) {
                (Value::Timestamp(t), Value::Float(v)) => {
                    assert_eq!(
                        t.as_micros(),
                        want_ts,
                        "iter {}: row {} ts broken (gap or corrupt)",
                        iter,
                        i
                    );
                    assert!(
                        (v - want_v).abs() < 1e-12,
                        "iter {}: row {} value corrupted: {v} != {want_v}",
                        iter,
                        i
                    );
                }
                other => panic!("iter {}: row {} wrong types: {:?}", iter, i, other),
            }
        }
        // Durability: acked inserts all present.
        assert!(
            rs.len() >= acked,
            "iter {}: durability violation — {acked} acked but {} recovered",
            iter,
            rs.len()
        );
        drop(db);
    }
    assert!(
        killed_runs > 0,
        "never killed the child — test degraded to a no-op"
    );
    eprintln!("timeseries crash injection: {killed_runs} kills verified");
}

// ── Parent: concurrent multi-writer ────────────────────────────────────────

#[test]
fn test_kill9_concurrent_writers_prefix_per_thread() {
    let iterations: usize = std::env::var("MOTEDB_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let mut rng_state: u64 = 0x0F0E_0D0C_0B0A_0908;
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        ((*state % 250) + 20) as u64 // 20..270ms — let threads interleave
    };

    let mut killed_runs = 0;
    for iter in 0..iterations {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("crash.mote");
        let journal_path = dir.path().join("acked.txt");
        let delay = next_delay_ms(&mut rng_state);
        if spawn_kill_child(&db_path, &journal_path, delay, "concurrent") {
            killed_runs += 1;
        }

        // Per-thread journals.
        let mut thread_acked: Vec<i64> = Vec::new();
        for t in 0..3 {
            let jp = journal_path.with_file_name(format!("acked.txt.t{t}"));
            thread_acked.push(read_journal(&jp).last().copied().unwrap_or(-1));
        }

        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(e) => panic!("iter {}: reopen failed: {}", iter, e),
        };
        let rs = match db.query("SELECT id, w, val FROM cw") {
            Ok(rs) => rs,
            Err(e) if e.to_string().contains("not found") => {
                drop(db);
                continue;
            }
            Err(e) => panic!("iter {}: query failed: {}", iter, e),
        };
        drop(db);

        // Group ids by writer; verify contiguous prefix + exact payload.
        let mut by_thread: [Vec<i64>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        let mut seen_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for row in &rs {
            let (id, w, val) = match (&row[0], &row[1], &row[2]) {
                (Value::Integer(i), Value::Integer(w), Value::Integer(v)) => (*i, *w, *v),
                o => panic!("iter {}: bad row {o:?}", iter),
            };
            assert!(
                (0..3).contains(&w),
                "iter {}: unknown writer {w} — cross-contamination",
                iter
            );
            assert!(seen_ids.insert(id), "iter {}: duplicate id {id}", iter);
            let t = id / 100_000;
            assert_eq!(t, w, "iter {}: id {id} under writer {w}", iter);
            let i = id % 100_000;
            assert_eq!(val, w * 1000 + i, "iter {}: payload corrupt for {id}", iter);
            by_thread[w as usize].push(i);
        }
        for (t, idxs) in by_thread.iter().enumerate() {
            let mut sorted = idxs.clone();
            sorted.sort_unstable();
            // Contiguous prefix 0..k.
            for (k, i) in sorted.iter().enumerate() {
                assert_eq!(*i, k as i64, "iter {}: writer {t} has a gap at {k}", iter);
            }
            let prefix_len = sorted.len() as i64 - 1;
            let acked_i = if thread_acked[t] < 0 {
                -1
            } else {
                thread_acked[t] % 100_000
            };
            assert!(
                prefix_len >= acked_i,
                "iter {}: writer {t} durability — acked {acked_i} but prefix is {prefix_len}",
                iter,
            );
        }
    }
    assert!(
        killed_runs > 0,
        "never killed the child — test degraded to a no-op"
    );
    eprintln!("concurrent crash injection: {killed_runs} kills verified");
}

// ── Parent: upsert workload ────────────────────────────────────────────────

#[test]
fn test_kill9_upsert_recovers_exactly() {
    let iterations: usize = std::env::var("MOTEDB_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let mut rng_state: u64 = 0x1234_5678_9ABC_DEF0;
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        ((*state % 200) + 10) as u64
    };

    let mut killed_runs = 0;
    for iter in 0..iterations {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("crash.mote");
        let journal_path = dir.path().join("acked.txt");
        let delay = next_delay_ms(&mut rng_state);
        if spawn_kill_child(&db_path, &journal_path, delay, "upsert") {
            killed_runs += 1;
        }

        let journal = read_journal_lines(&journal_path);
        let db =
            Database::open(&db_path).unwrap_or_else(|e| panic!("iter {}: reopen: {}", iter, e));
        let rs = match db.query("SELECT id, val FROM u") {
            Ok(rs) => rs,
            Err(e) if e.to_string().contains("not found") && journal.is_empty() => {
                drop(db);
                continue;
            }
            Err(e) => panic!("iter {}: query: {}", iter, e),
        };
        drop(db);
        let state: std::collections::HashMap<i64, i64> = rs
            .into_iter()
            .filter_map(|r| match (&r[0], &r[1]) {
                (Value::Integer(k), Value::Integer(v)) => Some((*k, *v)),
                _ => None,
            })
            .collect();

        // Replay the acked journal to the state at the ack point.
        let ops: Vec<(u8, i64)> = journal
            .iter()
            .filter_map(|l| {
                let mut it = l.split_whitespace();
                match (it.next()?, it.next()?.parse::<i64>().ok()) {
                    ("I", Some(k)) => Some((b'I', k)),
                    ("U", Some(k)) => Some((b'U', k)),
                    ("R", Some(k)) => Some((b'R', k)),
                    ("G", Some(k)) => Some((b'G', k)),
                    _ => None,
                }
            })
            .collect();
        let mut sim: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for (op, k) in &ops {
            apply_upsert_op(&mut sim, *op, *k, false);
        }

        // Durability contract: everything acked is reflected; up to a couple
        // of UNACKED in-flight statements may ALSO have partially or fully
        // landed. OR REPLACE is two WAL records (delete+insert) — killed
        // mid-statement it can leave the delete applied without the insert
        // (key absent). Enumerate skip/full/half applications of the next
        // deterministic ops and require the state to match one.
        let matched = {
            let mut candidates: Vec<std::collections::HashMap<i64, i64>> = vec![sim.clone()];
            // Absolute sequence position of the next op: 8 base inserts,
            // then the workload rotation.
            let mut frontier = vec![sim.clone()];
            for step in 0..2 {
                let seq_idx = (ops.len() + step) as i64;
                let (op, k) = if seq_idx < 8 {
                    (b'I', seq_idx)
                } else {
                    upsert_op_at(seq_idx - 8)
                };
                let mut next_frontier = Vec::new();
                for base in &frontier {
                    // skip
                    next_frontier.push(base.clone());
                    // full
                    let mut full = base.clone();
                    apply_upsert_op(&mut full, op, k, false);
                    next_frontier.push(full);
                    // half (only R has an observable intermediate: delete
                    // applied, insert not → key absent)
                    if op == b'R' {
                        let mut half = base.clone();
                        half.remove(&k);
                        next_frontier.push(half);
                    }
                }
                candidates.extend(next_frontier.iter().cloned());
                frontier = next_frontier;
            }
            candidates.into_iter().find(|c| *c == state)
        };
        assert!(
            matched.is_some(),
            "iter {}: upsert state {:?} matches no durability-legal outcome of {} acked ops",
            iter,
            state,
            ops.len()
        );
    }
    assert!(killed_runs > 0, "never killed the child");
    eprintln!("upsert crash injection: {killed_runs} kills verified");
}

/// Deterministic upsert-workload op at workload index i (mirrors the child:
/// key = i % 8; rotation (i/8)%3 → 0: DO UPDATE +1, 1: OR REPLACE 5, 2: IGNORE).
fn upsert_op_at(i: i64) -> (u8, i64) {
    let k = i % 8;
    match (i / 8) % 3 {
        0 => (b'U', k),
        1 => (b'R', k),
        _ => (b'G', k),
    }
}

/// Apply one workload op to a simulated state. `half` is unused for now
/// (half-states are modeled at the enumeration site).
fn apply_upsert_op(sim: &mut std::collections::HashMap<i64, i64>, op: u8, k: i64, _half: bool) {
    match op {
        b'I' => {
            sim.insert(k, 0);
        }
        b'U' => *sim.entry(k).or_insert(0) += 1,
        b'R' => {
            sim.insert(k, 5);
        }
        b'G' => {}
        _ => {}
    }
}

/// Journal lines with torn-tail filtering (raw strings, no numeric parse).
fn read_journal_lines(path: &std::path::Path) -> Vec<String> {
    let mut s = String::new();
    match std::fs::File::open(path) {
        Ok(mut f) => {
            use std::io::Read;
            f.read_to_string(&mut s).expect("journal readable");
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("journal read failed: {}", e),
    }
    s.lines()
        // Drop a trailing line without a newline (torn mid-write ack).
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect()
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
