//! Bug Hunt v72 — nineteenth round: partial batch failure, DROP, UNION type checks, edges.

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

fn affected(db: &Database, sql: &str) -> usize {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Modification { affected_rows } => affected_rows,
        _ => 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Batch INSERT: does a failure roll back all rows or leave partial?
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_batch_insert_partial_failure_atomicity() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Row 3 has a non-numeric text into INT → should fail the whole batch.
    let res = db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,'bad'),(4,40)");
    // Document: either the whole batch fails (atomic) or partial succeeds.
    // Most SQL DBs: whole statement fails → 0 rows inserted.
    match res {
        Ok(_) => {
            // If it succeeded (coerced 'bad' somehow), count rows.
            let _ = q(&db, "SELECT COUNT(*) FROM t");
        }
        Err(_) => {
            // Atomic: no rows should be present.
            let r = q(&db, "SELECT COUNT(*) FROM t");
            assert_eq!(
                r,
                vec![vec![Value::Integer(0)]],
                "failed batch INSERT should be atomic (0 rows), got {:?}",
                r
            );
        }
    }
}

#[test]
fn test_batch_insert_duplicate_pk_in_batch() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Two rows with same PK in one batch → should fail.
    let res = db.execute("INSERT INTO t VALUES (1,10),(1,20)");
    assert!(res.is_err(), "duplicate PK within batch should fail");
    // No row should exist (atomic).
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(0)]],
        "failed batch should leave 0 rows"
    );
}

#[test]
fn test_batch_insert_then_reinsert_after_failure() {
    // After a failed batch, a valid INSERT should work (no phantom PKs).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let _ = db.execute("INSERT INTO t VALUES (1,10),(1,20)"); // fails (dup PK)
                                                              // Re-insert id=1 — should succeed.
    db.execute("INSERT INTO t VALUES (1,99)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(99)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DROP TABLE behavior
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_drop_table_then_recreate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("DROP TABLE t").unwrap();
    // Recreate with same name.
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(0)]],
        "recreated table should be empty"
    );
}

#[test]
fn test_drop_table_then_select_errors() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("DROP TABLE t").unwrap();
    let res = db.execute("SELECT * FROM t");
    assert!(res.is_err(), "SELECT from dropped table should error");
}

#[test]
fn test_drop_table_with_index() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("DROP TABLE t").unwrap();
    // Recreate and re-query — no stale index interference.
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,20)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UNION column count / type checks
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_union_column_count_mismatch() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(x INT)").unwrap();
    db.execute("CREATE TABLE b(x INT, y INT)").unwrap();
    let res = db.execute("SELECT x FROM a UNION SELECT x, y FROM b");
    assert!(
        res.is_err(),
        "UNION with different column counts should error"
    );
}

#[test]
fn test_union_all_column_count_ok() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(x INT, y INT)").unwrap();
    db.execute("CREATE TABLE b(x INT, y INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,2)").unwrap();
    db.execute("INSERT INTO b VALUES (3,4)").unwrap();
    let r = q(
        &db,
        "SELECT x, y FROM a UNION ALL SELECT x, y FROM b ORDER BY x",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(4)]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction + failed INSERT (rollback semantics)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_txn_failed_insert_then_query() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN").unwrap();
    // A failed INSERT inside txn should not corrupt the txn.
    let _ = db.execute("INSERT INTO t VALUES (2,'bad')");
    // Valid insert should still work in the txn.
    db.execute("INSERT INTO t VALUES (3,30)").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn test_txn_commit_after_failed_insert() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let _ = db.execute("INSERT INTO t VALUES (2,'bad')"); // fails
    db.execute("INSERT INTO t VALUES (3,30)").unwrap();
    db.execute("COMMIT").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Reopen after DROP + recreate (persistence)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_drop_recreate_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10)").unwrap();
        db.execute("DROP TABLE t").unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,20)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple tables interaction
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_multiple_tables_independent() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").unwrap();
    db.execute("INSERT INTO b VALUES (1,20)").unwrap();
    let ra = q(&db, "SELECT v FROM a WHERE id = 1");
    let rb = q(&db, "SELECT v FROM b WHERE id = 1");
    assert_eq!(ra, vec![vec![Value::Integer(10)]]);
    assert_eq!(rb, vec![vec![Value::Integer(20)]]);
}

#[test]
fn test_cross_table_same_pk_no_conflict() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (1)").unwrap();
    // Same PK in different tables — no conflict.
    let ra = q(&db, "SELECT COUNT(*) FROM a");
    let rb = q(&db, "SELECT COUNT(*) FROM b");
    assert_eq!(ra, vec![vec![Value::Integer(1)]]);
    assert_eq!(rb, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Large IN list (performance + correctness boundary)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_large_in_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(50),(100)")
        .unwrap();
    // IN with 20 values including some matches.
    let list: Vec<String> = (1..=20).map(|i| i.to_string()).collect();
    let sql = format!(
        "SELECT id FROM t WHERE id IN ({}) ORDER BY id",
        list.join(",")
    );
    let r = q(&db, &sql);
    // matches: 1,2,3 (1-20 range); 50,100 not in list.
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE setting PK to same value (no-op)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_pk_to_same_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // UPDATE id = 1 WHERE id = 1 — should succeed (no real change).
    db.execute("UPDATE t SET id = 1 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT id, v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)]]);
}

#[test]
fn test_update_pk_to_same_value_with_other_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // SET id = 1 (same), v = 20.
    db.execute("UPDATE t SET id = 1, v = 20 WHERE id = 1")
        .unwrap();
    let r = q(&db, "SELECT id, v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE then INSERT with same PK (full cycle)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_delete_insert_same_pk_cycle() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    for cycle in 0..5 {
        db.execute(&format!("INSERT INTO t VALUES (1,{})", cycle * 10))
            .unwrap();
        let r = q(&db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r, vec![vec![Value::Integer(cycle * 10)]]);
        let n = affected(&db, "DELETE FROM t WHERE id = 1");
        assert_eq!(n, 1);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Empty string handling in aggregates
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_distinct_empty_string_vs_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,''),(2,''),(3,NULL),(4,'x')")
        .unwrap();
    // COUNT(DISTINCT s): '', 'x' → 2 (NULL excluded, '' counted once).
    let r = q(&db, "SELECT COUNT(DISTINCT s) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_groupby_empty_string() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(s TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('',10),('',20),('x',30)")
        .unwrap();
    let r = q(&db, "SELECT s, SUM(v) FROM t GROUP BY s ORDER BY s");
    assert_eq!(
        r,
        vec![
            vec![Value::text("".into()), Value::Integer(30)],
            vec![Value::text("x".into()), Value::Integer(30)]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// CASE with aggregate in condition
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_select_case_on_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // CASE on SUM(v): sum=30 > 25 → 'big'.
    let r = q(
        &db,
        "SELECT CASE WHEN SUM(v) > 25 THEN 'big' ELSE 'small' END FROM t",
    );
    assert_eq!(r, vec![vec![Value::text("big".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Whitespace-only text handling
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_whitespace_text_preserved() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'   ')").unwrap();
    let r = q(&db, "SELECT LENGTH(s) FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over single row
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_aggregate_single_row() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,42)").unwrap();
    let r = q(
        &db,
        "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Float(42.0),
            Value::Integer(42),
            Value::Integer(42)
        ]]
    );
}
