//! EXISTS / NOT EXISTS subqueries (row-level queries). Aggregate+correlated-
//! EXISTS is an explicit error (honest gap), NOT a silent 0 — see the guard
//! in execute_select_internal.
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<motedb::types::Value>> {
    db.execute(sql)
        .unwrap()
        .materialize()
        .unwrap()
        .select_rows()
        .map(|(_, r)| r.to_vec())
        .unwrap_or_default()
}

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER)")
        .unwrap();
    for i in 1..=20 {
        db.execute(&format!("INSERT INTO t (id, a) VALUES ({i}, {})", i % 7))
            .unwrap();
    }
    for i in 1..=10 {
        let tref = if i % 3 == 0 {
            "NULL".into()
        } else {
            (i % 8).to_string()
        };
        db.execute(&format!("INSERT INTO u (id, t_id) VALUES ({i}, {tref})"))
            .unwrap();
    }
    (db, dir)
}

#[test]
fn correlated_exists_filters_rows() {
    let (db, _d) = setup();
    // Ground truth via the equivalent IN (u's non-NULL t_id domain).
    let exists_rows = rows(
        &db,
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.a) ORDER BY id",
    );
    let in_rows = rows(
        &db,
        "SELECT id FROM t WHERE a IN (SELECT t_id FROM u WHERE t_id IS NOT NULL) ORDER BY id",
    );
    assert!(!exists_rows.is_empty(), "EXISTS must match something");
    assert_eq!(exists_rows, in_rows, "EXISTS ≡ equivalent IN");
}

#[test]
fn not_exists_is_complement() {
    let (db, _d) = setup();
    let e = rows(
        &db,
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.a)",
    );
    let ne = rows(
        &db,
        "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.t_id = t.a)",
    );
    let all = rows(&db, "SELECT id FROM t");
    assert_eq!(
        e.len() + ne.len(),
        all.len(),
        "EXISTS + NOT EXISTS = all rows"
    );
}

#[test]
fn uncorrelated_exists() {
    let (db, _d) = setup();
    let e = rows(&db, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u)");
    assert_eq!(e.len(), 20);
    let none = rows(
        &db,
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE t_id = -999)",
    );
    assert_eq!(none.len(), 0);
}

#[test]
fn aggregate_with_correlated_exists_errors_loudly() {
    // 🔁 Now SUPPORTED (routed to the materialized path): must be CORRECT,
    // not erroring. The old test asserted the honest-gap error; the gap was
    // closed by guarding the aggregate fast paths (they silently returned
    // 0 on subquery-containing WHERE clauses).
    let (db, _d) = setup();
    let res = db
        .execute("SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.a)")
        .expect("aggregate + correlated EXISTS is supported");
    let m = res.materialize().unwrap();
    let n_row = m
        .select_rows()
        .map(|(_, r)| r[0].clone())
        .unwrap_or_default();
    let truth = db
        .execute("SELECT COUNT(*) FROM t WHERE a IN (SELECT t_id FROM u)")
        .unwrap()
        .materialize()
        .unwrap();
    let _ = truth;
    // Cross-check against the equivalent IN form (same setup data).
    let in_count = rows(
        &db,
        "SELECT COUNT(*) FROM t WHERE a IN (SELECT t_id FROM u)",
    );
    assert_eq!(
        m.select_rows().map(|(_, r)| r.len()).unwrap_or(0),
        1,
        "aggregate returns one row"
    );
    match (&n_row[0], &in_count[0][0]) {
        (motedb::types::Value::Integer(a), motedb::types::Value::Integer(b)) => {
            assert_eq!(a, b, "COUNT(EXISTS) must equal the equivalent IN count")
        }
        other => panic!("type mismatch {other:?}"),
    }
}
