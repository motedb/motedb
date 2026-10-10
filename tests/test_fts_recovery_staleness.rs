//! Probe: does the `indexed == live_rows` invariant hold for tables with
//! UPDATE / DELETE / NULL history? Decides whether the crash-recovery
//! staleness predicate can compare text-index total_docs against live rows.
//!
//! Also exercises v0.12.9 recovery behavior end-to-end:
//!   open #1 (clean close, marker present)  → NO rebuild (marker gate)
//!   open #2 (marker removed = crash guess) → rebuild, exact doc count
//! Run with --nocapture and MOTEDB_CZDBG=1-in-process to see [czdbg] lines.
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn count(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    match &rows[0][0] {
        motedb::types::Value::Integer(i) => *i,
        other => panic!("expected integer, got {other:?}"),
    }
}

fn verify_matches(db: &Database, label: &str) {
    let live = count(db, "SELECT COUNT(*) FROM t");
    let m_doc = count(db, "SELECT COUNT(*) FROM t WHERE MATCH(body, 'doc')");
    let m_delta = count(db, "SELECT COUNT(*) FROM t WHERE MATCH(body, 'delta')");
    let m_beta = count(db, "SELECT COUNT(*) FROM t WHERE MATCH(body, 'beta')");
    println!(
        "[{label}] live={live} MATCH doc={m_doc} (want 30) delta={m_delta} (want 50) beta={m_beta} (want 30)"
    );
    assert_eq!(live, 85, "[{label}] live rows");
    assert_eq!(m_doc, 30, "[{label}] never-updated rows must match 'doc'");
    assert_eq!(m_delta, 50, "[{label}] updated rows must match 'delta'");
    assert_eq!(
        m_beta, 30,
        "[{label}] never-updated rows still match 'beta'"
    );
}

#[test]
fn probe_total_docs_vs_live_rows() {
    std::env::set_var("MOTEDB_CZDBG", "1");
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("probe.mote");

    {
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(&db_path, config).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("CREATE TEXT INDEX t_body ON t(body)").unwrap();
        for i in 0..100 {
            db.execute(&format!(
                "INSERT INTO t (id, body) VALUES ({i}, 'alpha beta doc {i}')"
            ))
            .unwrap();
        }
        // Multi-round updates over the same ids: segment rollover + versions.
        for round in 0..3 {
            for i in 0..50 {
                db.execute(&format!(
                    "UPDATE t SET body = 'gamma delta v{round} row {i}' WHERE id = {i}"
                ))
                .unwrap();
            }
        }
        for i in 80..100 {
            db.execute(&format!("DELETE FROM t WHERE id = {i}"))
                .unwrap();
        }
        // Five NULL-body rows: legitimately unindexed (live 85, indexed 80).
        for i in 100..105 {
            db.execute(&format!("INSERT INTO t (id, body) VALUES ({i}, NULL)"))
                .unwrap();
        }
        verify_matches(&db, "pre-close");
    } // clean Drop → flush + marker

    let marker = db_path.join("fts_indexes.fresh");
    println!("clean-close marker exists: {}", marker.exists());
    assert!(marker.exists(), "clean Drop must stamp fts_indexes.fresh");

    // open #1: clean reopen — marker gates the stale check, NO rebuild.
    {
        let db = Database::open(&db_path).unwrap();
        // Consumed at open START (a crash mid-open must never leave a stale
        // marker claiming freshness for a LATER open); the closing Drop at
        // the end of this scope re-stamps it.
        assert!(
            !marker.exists(),
            "open must consume fts_indexes.fresh (crash-mid-open safety)"
        );
        verify_matches(&db, "clean-reopen");
    }

    // Simulate crash evidence: marker absent, data current. The stale check
    // must run; NULL discrepancy (80 vs 85) forces a rebuild whose [czdbg]
    // line reports the exact rebuilt doc count (the multi-version answer:
    // 80 ⇒ no duplicate live keys across segments; >80 ⇒ build double-counts).
    std::fs::remove_file(&marker).unwrap();
    {
        let db = Database::open(&db_path).unwrap();
        verify_matches(&db, "no-marker-reopen");
    }

    // Recovery idempotence: the rebuilt state must itself survive a clean
    // close → open cycle exactly (a rebuild that APPENDED into stale state
    // instead of resetting would double-count here).
    {
        let db = Database::open(&db_path).unwrap();
        verify_matches(&db, "post-recovery-clean-reopen");
    }
}
