//! Bug Hunt v53 — TIMESTAMP WHERE comparisons + JOIN+HAVING aggregates.
//!
//! Two bugs fixed:
//!
//! 1. **TIMESTAMP column comparisons with string literals returned empty.**
//!    `WHERE ts > '2024-01-01'`, `WHERE ts = '2024-01-15T10:30:00'`, etc.
//!    always returned no rows. Two root causes:
//!    a. Value PartialOrd/PartialEq had no (Timestamp, Text) arm — fell
//!       through to None/false. Fixed by parsing the text as ISO date.
//!    b. The col-segment scan decoded TIMESTAMP filter columns as None
//!       (the filter-col decode only handled Integer/Float/Boolean), so
//!       the predicate always received None. Added Timestamp decode.
//!    Also added Timestamp::parse_iso (public) and fixed two ColumnarSeg
//!    decode sites that read TIMESTAMP columns as Integer instead of
//!    Timestamp (affecting SELECT projection).
//!
//! 2. **JOIN + GROUP BY + HAVING with aggregate expression returned empty.**
//!    `SELECT c.name, SUM(o.amt) AS total ... HAVING SUM(o.amt) > 100`
//!    returned nothing. The JOIN apply_group_by HAVING path didn't compute
//!    aggregates referenced in HAVING but not in the SELECT list (the
//!    evaluator's aggregate lookup failed → every group skipped). Fixed by
//!    computing HAVING-only aggregates (same logic as try_apply_group_by_positional).

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    rows(db.execute(sql).unwrap().materialize().unwrap())
}

fn events_db() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE events(id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute(
        "INSERT INTO events VALUES \
         (1, '2024-01-15T10:30:00'), \
         (2, '2024-06-20T14:00:00'), \
         (3, '2023-12-25T08:00:00')",
    )
    .unwrap();
    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────
// TIMESTAMP WHERE comparisons with string literals
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_timestamp_gt_string() {
    let (db, _d) = events_db();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts > '2024-01-01T00:00:00' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_timestamp_lt_string() {
    let (db, _d) = events_db();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts < '2024-01-01T00:00:00' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_timestamp_eq_string() {
    let (db, _d) = events_db();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts = '2024-01-15T10:30:00'",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_timestamp_gte_string() {
    let (db, _d) = events_db();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts >= '2023-12-25T08:00:00' ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    );
}

#[test]
fn test_timestamp_ne_string() {
    let (db, _d) = events_db();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts <> '2024-01-15T10:30:00' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_timestamp_eq_micros_int() {
    let (db, _d) = events_db();
    // 2024-01-15T10:30:00 = 1705314600000000 micros
    let r = q(&db, "SELECT id FROM events WHERE ts = 1705314600000000");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_timestamp_between() {
    let (db, _d) = events_db();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts BETWEEN '2024-01-01' AND '2024-12-31' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_timestamp_order_by() {
    let (db, _d) = events_db();
    let r = q(&db, "SELECT id FROM events ORDER BY ts");
    // chronologically: 2023-12-25 (3), 2024-01-15 (1), 2024-06-20 (2)
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(1)],
            vec![Value::Integer(2)]
        ]
    );
}

#[test]
fn test_timestamp_select_decoded_as_timestamp() {
    // The column should decode as Timestamp, not Integer.
    let (db, _d) = events_db();
    let r = q(&db, "SELECT ts FROM events WHERE id = 1");
    assert_eq!(r.len(), 1);
    assert!(
        matches!(&r[0][0], Value::Timestamp(_)),
        "expected Timestamp, got {:?}",
        r[0][0]
    );
}

#[test]
fn test_timestamp_after_checkpoint() {
    // Same behavior after checkpoint (col-segment store path).
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE events(id INT PRIMARY KEY, ts TIMESTAMP)")
            .unwrap();
        db.execute(
            "INSERT INTO events VALUES (1, '2024-01-15T10:30:00'), (2, '2023-12-25T08:00:00')",
        )
        .unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(
        &db,
        "SELECT id FROM events WHERE ts > '2024-01-01' ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// JOIN + GROUP BY + HAVING with aggregate expressions
// ─────────────────────────────────────────────────────────────────────────

fn join_db() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, name TEXT, region TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust_id INT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice','US'),(2,'Bob','EU'),(3,'Carol','US')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50),(4,3,75)")
        .unwrap();
    (db, dir)
}

#[test]
fn test_join_having_sum_agg() {
    let (db, _d) = join_db();
    // Alice total = 300, Bob = 50, Carol = 75. HAVING SUM > 100 → Alice.
    let r = q(
        &db,
        "SELECT c.name, SUM(o.amt) AS total \
         FROM customers c JOIN orders o ON c.id = o.cust_id \
         GROUP BY c.name HAVING SUM(o.amt) > 100 ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("Alice".into()), Value::Integer(300)]]
    );
}

#[test]
fn test_join_having_count_agg() {
    let (db, _d) = join_db();
    // Alice has 2 orders, Bob 1, Carol 1. HAVING COUNT > 1 → Alice.
    let r = q(
        &db,
        "SELECT c.name, COUNT(o.id) \
         FROM customers c JOIN orders o ON c.id = o.cust_id \
         GROUP BY c.name HAVING COUNT(o.id) > 1 ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("Alice".into()), Value::Integer(2)]]
    );
}

#[test]
fn test_join_having_avg_agg() {
    let (db, _d) = join_db();
    // Alice avg = 150, Bob = 50, Carol = 75. HAVING AVG > 100 → Alice.
    let r = q(
        &db,
        "SELECT c.name, AVG(o.amt) \
         FROM customers c JOIN orders o ON c.id = o.cust_id \
         GROUP BY c.name HAVING AVG(o.amt) > 100 ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("Alice".into()), Value::Float(150.0)]]
    );
}

#[test]
fn test_join_having_min_max_agg() {
    let (db, _d) = join_db();
    // MIN per customer: Alice=100, Bob=50, Carol=75. HAVING MIN > 60 → Alice (100) and Carol (75).
    let r = q(
        &db,
        "SELECT c.name, MIN(o.amt) \
         FROM customers c JOIN orders o ON c.id = o.cust_id \
         GROUP BY c.name HAVING MIN(o.amt) > 60 ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("Alice".into()), Value::Integer(100)],
            vec![Value::text("Carol".into()), Value::Integer(75)],
        ]
    );
}

#[test]
fn test_join_having_no_match() {
    let (db, _d) = join_db();
    // No customer has SUM > 1000.
    let r = q(
        &db,
        "SELECT c.name FROM customers c JOIN orders o ON c.id = o.cust_id \
         GROUP BY c.name HAVING SUM(o.amt) > 1000",
    );
    assert!(r.is_empty());
}
