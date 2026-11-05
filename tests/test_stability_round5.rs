//! Round-5 stability probes (v0.17.0 pre-release): crash-injection with
//! SECONDARY INDEXES and CHECKPOINTS in the kill loop, repeated
//! crash→reopen→continue cycles on one directory, reopen-cycle hygiene
//! (segments / manifest / open time stay bounded), and a concurrent mixed
//! workload with periodic checkpoints.
//!
//! The existing kill-9 suite covers WAL prefix/durability on bare tables;
//! these tests add what stability actually depends on in the field: after a
//! kill, the derived structures (column index, vector index, FTS) must agree
//! with the recovered rows, and the directory must survive being killed
//! again and again without accumulating garbage or losing the prefix.
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use motedb::types::Value;
use motedb::{Database, QueryResult};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}
fn n(db: &Database, sql: &str) -> i64 {
    match &rows(db, sql)[0][0] {
        Value::Integer(i) => *i,
        other => panic!("non-int {other:?} for {sql}"),
    }
}

// ── Workload model ────────────────────────────────────────────────────────
//
// Deterministic per-id op order (the child follows it exactly; the journal
// records each acked statement):
//   I i                        (insert id i)
//   U i-1    iff i % 4 == 0 && i > 0   (tag='tagU', val += 100, emb last = 2)
//   D i-7    iff i % 7 == 0 && i >= 7  (delete)
//   C i      iff i % 50 == 0           (checkpoint — no state change)
// A kill can lose at most ONE ack (the statement that ran but whose journal
// line was not written yet), so the recovered state must equal the
// simulation of the journal, or that plus exactly the next op — and the
// verifier re-journals that one op so later cycles still model the state.

#[derive(Clone, Debug, PartialEq)]
struct St {
    tag: String,
    val: i64,
    /// last element of the 4-dim vector (the first three are [id, 2*id, 3*id])
    last: i64,
}

fn base_state(id: i64) -> St {
    St {
        tag: format!("tag{}", id.rem_euclid(10)),
        val: id,
        last: 1,
    }
}
fn emb_of(id: i64, s: &St) -> String {
    format!("'[{}, {}, {}, {}]'", id, id * 2, id * 3, s.last)
}

type Sim = BTreeMap<i64, St>;

fn apply_op(sim: &mut Sim, op: &str) {
    let mut it = op.split_whitespace();
    let tag = it.next().unwrap_or_default();
    let arg: i64 = it.next().unwrap_or("0").parse().expect("journal op arg");
    match tag {
        "I" => {
            sim.insert(arg, base_state(arg));
        }
        "U" => {
            if let Some(s) = sim.get_mut(&arg) {
                let v = s.val;
                *s = St {
                    tag: "tagU".into(),
                    val: v + 100,
                    last: 2,
                };
            }
        }
        "D" => {
            sim.remove(&arg);
        }
        // Parent-side continuation write: tagX row ([id, 2·id, 3·id, 1]).
        "X" => {
            sim.insert(
                arg,
                St {
                    tag: "tagX".into(),
                    val: arg,
                    last: 1,
                },
            );
        }
        _ => {} // C — checkpoint ack, no state change
    }
}

fn ops_for(i: i64) -> Vec<String> {
    let mut v = vec![format!("I {i}")];
    if i % 4 == 0 && i > 0 {
        v.push(format!("U {}", i - 1));
    }
    if i % 7 == 0 && i >= 7 {
        v.push(format!("D {}", i - 7));
    }
    if i % 50 == 0 {
        v.push(format!("C {i}"));
    }
    v
}

/// The id whose op-list contains `last`, then the next op in the global order.
/// `next_start` = the child's resume cursor (last journaled "I" id + 1) —
/// needed because after a parent-written "X <id>" line the child's in-flight
/// op is always its FIRST op `I <next_start>`; modeling anything else would
/// miss the "executed but un-journaled insert right after an X line" shape
/// (负载下 45ms 杀点实测踩中过: db 多出未记账行、候选包络不含它)。
fn next_op_after(last: &str, next_start: i64) -> Option<String> {
    if last.is_empty() {
        return Some(format!("I {next_start}"));
    }
    if last.starts_with("X ") {
        // X 是父进程写的 tagX 行，不推进子进程的 I 游标; 杀点若落在
        // 子进程首个 execute 与 journal 写之间，未记账操作必为
        // I <next_start>（child 循环总以 INSERT 开头）。
        return Some(format!("I {next_start}"));
    }
    let mut it = last.split_whitespace();
    let t = it.next()?;
    let a: i64 = it.next()?.parse().ok()?;
    let i = match t {
        "I" => a,
        "U" => a + 1,
        "D" => a + 7,
        "C" => a,
        _ => return None,
    };
    let seq = ops_for(i);
    let pos = seq.iter().position(|x| x == last)?;
    seq.get(pos + 1)
        .cloned()
        .or_else(|| Some(format!("I {}", i + 1)))
}

fn read_journal(path: &Path) -> Vec<String> {
    let s = std::fs::read_to_string(path).unwrap_or_default();
    // 🔒 kill -9 可在 writeln 的底层 write 之间撕裂最后一条（无换行的
    // 残尾，甚至半截 token——实测 InvalidDigit）。撕裂尾行不可信：丢弃。
    let complete_end = s.rfind('\n').map(|i| i + 1).unwrap_or(0);
    s[..complete_end]
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect()
}

fn simulate(journal: &[String]) -> Sim {
    let mut sim = Sim::new();
    for op in journal {
        apply_op(&mut sim, op.as_str());
    }
    sim
}

fn db_state(db: &Database) -> BTreeMap<i64, (String, i64)> {
    rows(db, "SELECT id, tag, val FROM s")
        .into_iter()
        .map(|r| match (&r[0], &r[1], &r[2]) {
            (Value::Integer(i), Value::Text(t), Value::Integer(v)) => {
                (*i, (t.as_str().to_string(), *v))
            }
            other => panic!("bad row {other:?}"),
        })
        .collect()
}

fn sim_to_pairs(sim: &Sim) -> BTreeMap<i64, (String, i64)> {
    sim.iter()
        .map(|(k, s)| (*k, (s.tag.clone(), s.val)))
        .collect()
}

// ── Child mode ────────────────────────────────────────────────────────────

#[test]
fn stability_child_mode() {
    let db_path: PathBuf = match std::env::var_os("MOTEDB_STAB_CHILD_DB") {
        Some(p) => p.into(),
        None => return, // normal suite run — not a child invocation
    };
    let journal_path: PathBuf = std::env::var("MOTEDB_STAB_CHILD_JOURNAL")
        .expect("journal env")
        .into();
    let start: i64 = std::env::var("MOTEDB_STAB_CHILD_START")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let iterations: i64 = std::env::var("MOTEDB_STAB_CHILD_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);

    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(_) => Database::create(&db_path).expect("create db"),
    };
    db.execute("CREATE TABLE IF NOT EXISTS s (id INT PRIMARY KEY, tag TEXT, body TEXT, emb VECTOR(4), val INT)")
        .expect("create table");
    // On a resumed directory these already exist — their errors are fine.
    let _ = db.execute("CREATE INDEX s_tag ON s (tag) USING COLUMN");
    let _ = db.execute("CREATE VECTOR INDEX s_emb ON s (emb)");
    let _ = db.execute("CREATE TEXT INDEX s_body ON s (body)");

    let mut journal = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .expect("journal");

    let mut i = start;
    let end = start.saturating_add(iterations);
    while i < end {
        // A kill between execute() and the journal write leaves the row
        // present but un-journaled; the retry then hits the PK while the row
        // already holds exactly these values → count it as acked.
        let ins = db.execute(&format!(
            "INSERT INTO s VALUES ({i}, 'tag{}', 'doc{i} word{}', '[{}, {}, {}, 1]', {i})",
            i.rem_euclid(10),
            i.rem_euclid(5),
            i,
            i * 2,
            i * 3
        ));
        match ins {
            Ok(_) => {}
            Err(e) => assert!(
                format!("{e}").contains("Duplicate primary key"),
                "insert {i}: {e}"
            ),
        }
        writeln!(journal, "I {i}").unwrap();
        journal.flush().unwrap();

        if i % 4 == 0 && i > 0 {
            db.execute(&format!(
                "UPDATE s SET tag = 'tagU', val = val + 100, emb = '[{}, {}, {}, 2]' WHERE id = {}",
                i - 1,
                (i - 1) * 2,
                (i - 1) * 3,
                i - 1
            ))
            .expect("update");
            writeln!(journal, "U {}", i - 1).unwrap();
            journal.flush().unwrap();
        }
        if i % 7 == 0 && i >= 7 {
            db.execute(&format!("DELETE FROM s WHERE id = {}", i - 7))
                .expect("delete");
            writeln!(journal, "D {}", i - 7).unwrap();
            journal.flush().unwrap();
        }
        if i % 50 == 0 {
            db.checkpoint().expect("checkpoint");
            writeln!(journal, "C {i}").unwrap();
            journal.flush().unwrap();
        }
        i += 1;
    }
    let _ = db.close();
}

// ── Parent: crash → reopen → verify, repeated on one directory ───────────

fn spawn_kill_child(db_path: &Path, journal_path: &Path, start: i64, delay_ms: u64) -> bool {
    let mut child = Command::new(std::env::current_exe().expect("current exe"))
        .arg("stability_child_mode")
        .arg("--exact")
        .env("MOTEDB_STAB_CHILD_DB", db_path)
        .env("MOTEDB_STAB_CHILD_JOURNAL", journal_path)
        .env("MOTEDB_STAB_CHILD_START", start.to_string())
        .env("MOTEDB_STAB_CHILD_ITERS", "200000")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn stability child");
    std::thread::sleep(Duration::from_millis(delay_ms));
    let killed = child.kill().is_ok();
    let _ = child.wait();
    killed
}

fn append_journal(path: &Path, line: &str) {
    let mut j = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .unwrap();
    writeln!(j, "{line}").unwrap();
    j.flush().unwrap();
}

/// Full post-recovery verification: rows, COUNT, column index, vector index
/// (index path forced via MOTE_VEC_INDEX_MIN_ROWS), FTS, doctor.
fn verify_recovered(db: &Database, sim: &Sim, phase: &str) {
    assert_eq!(db_state(db), sim_to_pairs(sim), "{phase}: recovered rows");
    assert_eq!(
        n(db, "SELECT COUNT(*) FROM s"),
        sim.len() as i64,
        "{phase}: COUNT(*)"
    );

    // Column index agreement for every tag value in play.
    let mut tags: Vec<String> = (0..10).map(|t| format!("tag{t}")).collect();
    tags.push("tagU".into());
    tags.push("tagX".into());
    for t in tags {
        let expected: Vec<i64> = sim
            .iter()
            .filter(|(_, s)| s.tag == t)
            .map(|(k, _)| *k)
            .collect();
        let mut got: Vec<i64> = rows(db, &format!("SELECT id FROM s WHERE tag = '{t}'"))
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i,
                o => panic!("{o:?}"),
            })
            .collect();
        got.sort_unstable();
        assert_eq!(got, expected, "{phase}: column index WHERE tag = '{t}'");
    }

    // Vector search: the top-1 for a live row's own vector is that row.
    let sample: Vec<(&i64, &St)> = sim
        .iter()
        .take(3)
        .chain(
            sim.iter()
                .next_back()
                .into_iter()
                .chain(sim.iter().rev().take(2).rev()),
        )
        .collect();
    for (k, s) in sample {
        let top = rows(
            db,
            &format!(
                "SELECT id FROM s ORDER BY emb <-> {} LIMIT 1",
                emb_of(*k, s)
            ),
        );
        if top.len() != 1 || top[0][0] != Value::Integer(*k) {
            // Diagnose: same query with the index routed OFF (brute force).
            let saved = std::env::var("MOTE_VEC_INDEX_MIN_ROWS").ok();
            std::env::remove_var("MOTE_VEC_INDEX_MIN_ROWS");
            let brute = rows(
                db,
                &format!(
                    "SELECT id FROM s ORDER BY emb <-> {} LIMIT 3",
                    emb_of(*k, s)
                ),
            );
            if let Some(v) = saved {
                std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", v);
            }
            let wide = rows(
                db,
                &format!(
                    "SELECT id FROM s ORDER BY emb <-> {} LIMIT 40",
                    emb_of(*k, s)
                ),
            );
            let vec_read = rows(db, &format!("SELECT emb FROM s WHERE id = {k}"));
            let cnt = n(db, "SELECT COUNT(*) FROM s");
            panic!(
                "{phase}: knn top-1 for {k} => {top:?} (index-routed); brute top-3 => {brute:?}; \
                 brute top-40 => {wide:?}; PK-read emb => {vec_read:?}; COUNT={cnt}"
            );
        }
    }

    // FTS: each sampled live row matches its unique token; deleted ids stay
    // gone.
    for k in sim.keys().take(2).chain(sim.keys().rev().take(2)) {
        let hits = rows(db, &format!("SELECT id FROM s WHERE MATCH(body, 'doc{k}')"));
        assert_eq!(hits.len(), 1, "{phase}: FTS doc{k} => {hits:?}");
        assert_eq!(hits[0][0], Value::Integer(*k));
    }
    for k in [0i64, 7, 14, 21] {
        if !sim.contains_key(&k) {
            assert_eq!(
                rows(db, &format!("SELECT id FROM s WHERE MATCH(body, 'doc{k}')")).len(),
                0,
                "{phase}: FTS must not resurrect deleted doc{k}"
            );
        }
    }

    // doctor: no FAIL.
    let report = db.doctor();
    let fails: Vec<String> = report
        .checks
        .iter()
        .filter(|c| c.status == motedb::database::doctor::DoctorStatus::Fail)
        .map(|c| format!("{}: {}", c.name, c.detail))
        .collect();
    assert!(fails.is_empty(), "{phase}: doctor FAILs: {fails:?}");
}

#[test]
fn test_kill9_with_indexes_and_repeated_cycles() {
    // Route SQL KNN through the vector index even at this small scale.
    std::env::set_var("MOTE_VEC_INDEX_MIN_ROWS", "100");
    let cycles: usize = std::env::var("MOTEDB_STAB_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let mut rng_state: u64 = 0xC0FF_EECA_FE12_3456;
    let next_delay_ms = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        (*state % 350) + 40
    };

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("stab.mote");
    let journal_path = dir.path().join("acked.txt");

    for cycle in 0..cycles {
        let journal = read_journal(&journal_path);
        let start = journal
            .iter()
            .rev()
            .find(|l| l.starts_with("I "))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<i64>().ok())
            .map(|max_i| max_i + 1)
            .unwrap_or(0);
        let delay = next_delay_ms(&mut rng_state);
        let killed = spawn_kill_child(&db_path, &journal_path, start, delay);
        eprintln!(
            "[stab] cycle {cycle}: killed={killed} start={start} delay={delay}ms journal_lines={}",
            read_journal(&journal_path).len()
        );

        let journal = read_journal(&journal_path);
        // An empty journal (killed before the first ack) is a legitimate
        // outcome under CPU contention — the empty state is still verified
        // below against [nothing | one un-acked insert].
        let last = journal.last().cloned().unwrap_or_default();
        let mut sim = simulate(&journal);
        // The recovered state may additionally contain the one un-acked op.
        let mut candidates = vec![(sim.clone(), None::<String>)];
        if let Some(next) = next_op_after(&last, start) {
            let mut s2 = sim.clone();
            apply_op(&mut s2, &next);
            candidates.push((s2, Some(next)));
        }
        let db = Database::open(&db_path).expect("reopen after kill");
        let actual = db_state(&db);
        let matched = candidates.iter().find(|(c, _)| actual == sim_to_pairs(c));
        let Some((accepted, unacked)) = matched else {
            let sim_ids: Vec<i64> = sim.keys().copied().collect();
            let db_ids: Vec<i64> = actual.keys().copied().collect();
            let mut diff: Vec<String> = Vec::new();
            for (k, v) in &actual {
                match sim.get(k) {
                    None => diff.push(format!("db-only id {k} => {v:?}")),
                    Some(st) if &(st.tag.clone(), st.val) != v => {
                        diff.push(format!("id {k}: db {v:?} vs sim {st:?}"))
                    }
                    _ => {}
                }
            }
            for k in sim.keys() {
                if !actual.contains_key(k) {
                    diff.push(format!("sim-only id {k} => {:?}", sim.get(k)));
                }
            }
            panic!(
                "cycle {cycle}: recovered state matches neither candidate \
                 (journal {} lines, last {last:?}; sim {} rows {sim_ids:?}; db {} rows {db_ids:?}); diff: {diff:?}",
                journal.len(),
                sim.len(),
                actual.len(),
            );
        };
        sim = accepted.clone();
        // Re-journal the verified-but-un-acked op so later cycles model it.
        if let Some(op) = unacked {
            append_journal(&journal_path, op);
        }
        verify_recovered(&db, &sim, &format!("cycle {cycle}"));
        // The directory must keep accepting work on the reopened handle too,
        // not only via a fresh child.
        let extra: i64 = 9_000_000 + cycle as i64;
        // emb_of models the vector as [id, 2·id, 3·id, last] — insert exactly
        // that so the KNN self-match below queries the row's real vector.
        let extra_state = St {
            tag: "tagX".into(),
            val: extra,
            last: 1,
        };
        db.execute(&format!(
            "INSERT INTO s VALUES ({extra}, 'tagX', 'doc{extra} word9', {}, {extra})",
            emb_of(extra, &extra_state)
        ))
        .expect("write on reopened handle");
        append_journal(&journal_path, &format!("X {extra}"));
        sim.insert(extra, extra_state);
        verify_recovered(&db, &sim, &format!("cycle {cycle} after extra write"));
        let _ = db.close();
    }
}

// ── Reopen-cycle hygiene: segments / manifest / open time stay bounded ────

#[test]
fn test_reopen_cycles_keep_directory_bounded() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cycles.mote");
    let cycles = 40u64;
    let mut last_open_ms = 0u128;
    for c in 0..cycles {
        let t0 = Instant::now();
        let db = if c == 0 {
            let db = Database::create(&path).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, tag TEXT)")
                .unwrap();
            db
        } else {
            Database::open(&path).unwrap()
        };
        last_open_ms = t0.elapsed().as_millis();
        for i in 0..200i64 {
            let id = c as i64 * 200 + i;
            db.execute(&format!(
                "INSERT INTO t VALUES ({id}, {id}, 'tag{}')",
                id.rem_euclid(8)
            ))
            .unwrap();
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let t0 = Instant::now();
    let db = Database::open(&path).unwrap();
    let cold_open_ms = t0.elapsed().as_millis();
    assert_eq!(n(&db, "SELECT COUNT(*) FROM t"), (cycles as i64) * 200);
    // Compaction must have kept the live segment set small (threshold is 3).
    let seg_dir = path.join("columnar_ms").join("t");
    let segs: Vec<_> = std::fs::read_dir(&seg_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
        .collect();
    assert!(
        segs.len() <= 6,
        "{} segment files after {cycles} checkpoint cycles",
        segs.len()
    );
    // The manifest log stays bounded relative to live state.
    let manifest_len = std::fs::metadata(path.join("columnar_ms").join("MANIFEST"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(
        manifest_len < 256 * 1024,
        "manifest grew to {manifest_len} bytes"
    );
    // Open time must stay in the "lazy load" regime, not degrade into a
    // full-table rescan per open.
    assert!(last_open_ms < 1_500, "warm open took {last_open_ms}ms");
    assert!(cold_open_ms < 1_500, "cold open took {cold_open_ms}ms");
    let _ = db.close();
}

// ── Concurrent mixed workload with periodic checkpoints ───────────────────

#[test]
fn test_concurrent_mixed_workload_with_checkpoints() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = TempDir::new().unwrap();
    let db = std::sync::Arc::new(Database::create(dir.path().join("conc.mote")).unwrap());
    db.execute("CREATE TABLE bank (id INT PRIMARY KEY, acct INT, amt INT)")
        .unwrap();
    for id in 0..50i64 {
        db.execute(&format!("INSERT INTO bank VALUES ({id}, {}, 100)", id % 5))
            .unwrap();
    }
    db.execute("CREATE INDEX bank_acct ON bank (acct) USING COLUMN")
        .unwrap();
    db.execute("CREATE TABLE audit (id INT PRIMARY KEY, note TEXT)")
        .unwrap();
    let total0: i64 = n(&db, "SELECT SUM(amt) FROM bank");

    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    // 4 writers: transfer 1 between two random accounts (only when the
    // source has funds), plus insert/delete churn on the audit table.
    for w in 0..4u64 {
        let db = db.clone();
        let stop = stop.clone();
        let mut k = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 1);
        let mut audit_id = w * 1_000_000;
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                k ^= k << 13;
                k ^= k >> 7;
                k ^= k << 17;
                let a = (k % 50) as i64;
                let b = ((k >> 8) % 50) as i64;
                if a != b {
                    let r = db
                        .execute(&format!(
                            "UPDATE bank SET amt = amt - 1 WHERE id = {a} AND amt > 0"
                        ))
                        .unwrap_or_else(|e| panic!("debit failed: {e}"));
                    if r.affected_rows() == 1 {
                        db.execute(&format!("UPDATE bank SET amt = amt + 1 WHERE id = {b}"))
                            .unwrap_or_else(|e| panic!("credit failed: {e}"));
                    }
                }
                audit_id += 1;
                db.execute(&format!("INSERT INTO audit VALUES ({audit_id}, 'w{w}')"))
                    .unwrap_or_else(|e| panic!("audit insert failed: {e}"));
                if audit_id % 97 == 0 {
                    db.execute(&format!("DELETE FROM audit WHERE id = {}", audit_id - 96))
                        .unwrap_or_else(|e| panic!("audit delete failed: {e}"));
                }
            }
        }));
    }
    // 2 readers + a checkpointer.
    for _ in 0..2u64 {
        let db = db.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                assert_eq!(n(&db, "SELECT COUNT(*) FROM bank"), 50, "row count drifted");
                let by_acct: i64 = n(&db, "SELECT COUNT(*) FROM bank WHERE acct = 3");
                assert!(
                    (0..=50).contains(&by_acct),
                    "indexed count out of range: {by_acct}"
                );
                let _ = rows(&db, "SELECT acct, SUM(amt) FROM bank GROUP BY acct");
            }
        }));
    }
    {
        let db = db.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.checkpoint();
                std::thread::sleep(Duration::from_millis(25));
            }
        }));
    }

    std::thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // Invariant: transfers conserve the total, no negative balances.
    assert_eq!(n(&db, "SELECT SUM(amt) FROM bank"), total0, "total drifted");
    assert_eq!(
        n(&db, "SELECT COUNT(*) FROM bank WHERE amt < 0"),
        0,
        "amt < 0"
    );
    let _ = db.close();

    // And it survives a reopen with the invariant intact.
    let db = Database::open(dir.path().join("conc.mote")).unwrap();
    assert_eq!(
        n(&db, "SELECT SUM(amt) FROM bank"),
        total0,
        "total after reopen"
    );
    assert_eq!(n(&db, "SELECT COUNT(*) FROM bank"), 50);
    let _ = db.close();
}
