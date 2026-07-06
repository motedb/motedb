//! Semantic correctness edge cases — the small relational behaviors an
//! embedded database must get exactly right.
//!
//! Focuses on areas that broad coverage frequently gets subtly wrong:
//! - Aggregates over an empty table / empty result set
//! - LIMIT 0 (zero-row projection, must still report column schema)
//! - Multi-column ORDER BY with mixed ASC/DESC and tie-breakers
//! - Comparison operators returning correct type (Integer vs Float)
//! - NULL semantics in comparisons (NULL is never equal, even to NULL)
//! - Aggregate correctness with WHERE filtering all rows

use motedb::types::Value;
use motedb::{DBConfig, Database, QueryResult};
use tempfile::TempDir;

fn make_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?}", std::mem::discriminant(&other)),
    }
}

fn one(db: &Database, sql: &str) -> Vec<Value> {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected exactly one row for: {}", sql);
    r.into_iter().next().unwrap()
}

// ═══════════════════════════════════════════════════════════════════════
// Aggregates over an empty table — must return NULL (or 0 for COUNT),
// never an empty result set, never an error.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn count_empty_table_is_zero() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE e (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let r = one(&db, "SELECT COUNT(*) FROM e");
    assert_eq!(r[0], Value::Integer(0));
}

#[test]
fn sum_avg_min_max_empty_table_is_null() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE e (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // SUM/AVG/MIN/MAX over no rows are NULL in standard SQL.
    let r = one(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM e");
    assert_eq!(r[0], Value::Null, "SUM over empty should be NULL");
    assert_eq!(r[1], Value::Null, "AVG over empty should be NULL");
    assert_eq!(r[2], Value::Null, "MIN over empty should be NULL");
    assert_eq!(r[3], Value::Null, "MAX over empty should be NULL");
}

#[test]
fn aggregates_with_where_matching_nothing() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=5 {
        db.insert_row("t", vec![Value::Integer(i), Value::Integer(i * 10)])
            .unwrap();
    }
    // WHERE matches nothing.
    let r = one(
        &db,
        "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM t WHERE id > 100",
    );
    assert_eq!(r[0], Value::Integer(0));
    assert_eq!(r[1], Value::Null);
    assert_eq!(r[2], Value::Null);
    assert_eq!(r[3], Value::Null);
}

#[test]
fn avg_integer_division_yields_float() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // v = 1,2,3 → avg = 2.0 (a Float, not truncated integer division).
    db.insert_row("t", vec![Value::Integer(1), Value::Integer(1)])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(2), Value::Integer(2)])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(3), Value::Integer(3)])
        .unwrap();
    let r = one(&db, "SELECT AVG(v) FROM t");
    match r[0] {
        Value::Float(f) => assert!((f - 2.0).abs() < 1e-9, "AVG = {}, want 2.0", f),
        ref other => panic!("AVG should be Float, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// LIMIT 0 — must return a header with zero data rows.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn limit_zero_returns_no_rows() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=10 {
        db.insert_row("t", vec![Value::Integer(i), Value::Integer(i)])
            .unwrap();
    }
    let r = rows(&db, "SELECT * FROM t LIMIT 0");
    assert!(r.is_empty(), "LIMIT 0 must return zero rows");
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-column ORDER BY with mixed ASC/DESC.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn order_by_multi_column_mixed_direction() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, grp INT, v INT)")
        .unwrap();
    let data = [
        (1, 1, 30),
        (2, 1, 10),
        (3, 1, 20),
        (4, 2, 5),
        (5, 2, 50),
        (6, 2, 25),
    ];
    for (id, grp, v) in data {
        db.insert_row(
            "t",
            vec![Value::Integer(id), Value::Integer(grp), Value::Integer(v)],
        )
        .unwrap();
    }

    // grp ASC, v DESC → (1,30),(1,20),(1,10),(2,50),(2,25),(2,5)
    let r = rows(&db, "SELECT id, grp, v FROM t ORDER BY grp ASC, v DESC");
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![1, 3, 2, 5, 6, 4]);

    // grp DESC, v ASC → (2,5),(2,25),(2,50),(1,10),(1,20),(1,30)
    let r = rows(&db, "SELECT id FROM t ORDER BY grp DESC, v ASC");
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![4, 6, 5, 2, 3, 1]);
}

#[test]
fn order_by_tiebreaker_stable_secondary() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    // All rows share a=5; secondary key b ASC must decide order.
    for (id, b) in [(1, 30), (2, 10), (3, 20), (4, 10)] {
        db.insert_row(
            "t",
            vec![Value::Integer(id), Value::Integer(5), Value::Integer(b)],
        )
        .unwrap();
    }
    let r = rows(&db, "SELECT id FROM t ORDER BY a, b ASC");
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    // b=10 (ids 2,4 in insertion order), then b=20 (id 3), then b=30 (id 1).
    assert_eq!(ids[2], 3, "b=20 should come third");
    assert_eq!(ids[3], 1, "b=30 should come last");
    assert_eq!(ids[0], 2, "first b=10 by insertion order");
}

// ═══════════════════════════════════════════════════════════════════════
// NULL comparison semantics — NULL = NULL is unknown/false in WHERE.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn null_does_not_equal_null_in_where() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.insert_row("t", vec![Value::Integer(1), Value::Null])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(2), Value::Null])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(3), Value::Integer(7)])
        .unwrap();

    // WHERE v = NULL must match zero rows under standard SQL semantics.
    let r = rows(&db, "SELECT id FROM t WHERE v = NULL");
    assert!(r.is_empty(), "v = NULL must not match NULL rows");

    // IS NULL is the correct way.
    let r = rows(&db, "SELECT id FROM t WHERE v IS NULL");
    assert_eq!(r.len(), 2);
}

#[test]
fn is_not_null_filters_nulls() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.insert_row("t", vec![Value::Integer(1), Value::Null])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(2), Value::Integer(7)])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(3), Value::Null])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(4), Value::Integer(9)])
        .unwrap();

    let r = rows(&db, "SELECT id FROM t WHERE v IS NOT NULL ORDER BY id");
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![2, 4]);
}

// ═══════════════════════════════════════════════════════════════════════
// Comparison operator correctness on Integer columns (must not decode as Float).
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn integer_comparisons_exact() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in -2..=2 {
        db.insert_row("t", vec![Value::Integer(i + 10), Value::Integer(i * 100)])
            .unwrap();
    }
    // v values: -200, -100, 0, 100, 200
    let r = rows(&db, "SELECT v FROM t WHERE v >= 0 ORDER BY v");
    let vals: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            ref o => panic!("expected Integer, got {:?}", o),
        })
        .collect();
    assert_eq!(vals, vec![0, 100, 200]);

    let r = rows(&db, "SELECT v FROM t WHERE v < 0 ORDER BY v");
    let vals: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(vals, vec![-200, -100]);
}

#[test]
fn integer_values_returned_as_integer_not_float() {
    // Regression: Integer columns were once decoded as Float.
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.insert_row("t", vec![Value::Integer(1), Value::Integer(42)])
        .unwrap();
    let r = one(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Integer(42));
    // Explicitly not a Float.
    assert!(!matches!(r[0], Value::Float(_)));
}

// ═══════════════════════════════════════════════════════════════════════
// DISTINCT on multiple columns.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn distinct_multi_column() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    let pairs = [(1, 1), (1, 2), (1, 1), (2, 1), (2, 1), (3, 9)];
    for (i, (a, b)) in pairs.iter().enumerate() {
        db.insert_row(
            "t",
            vec![
                Value::Integer(i as i64 + 1),
                Value::Integer(*a),
                Value::Integer(*b),
            ],
        )
        .unwrap();
    }
    let r = rows(&db, "SELECT DISTINCT a, b FROM t ORDER BY a, b");
    let got: Vec<(i64, i64)> = r
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Integer(a), Value::Integer(b)) => (*a, *b),
            _ => panic!(),
        })
        .collect();
    // (1,1),(1,2),(2,1),(3,9)
    assert_eq!(got, vec![(1, 1), (1, 2), (2, 1), (3, 9)]);
}

// ═══════════════════════════════════════════════════════════════════════
// OFFSET pagination.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn limit_offset_pagination() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=10 {
        db.insert_row("t", vec![Value::Integer(i)]).unwrap();
    }
    let page = |offset: usize, limit: usize| -> Vec<i64> {
        rows(
            &db,
            &format!(
                "SELECT id FROM t ORDER BY id LIMIT {} OFFSET {}",
                limit, offset
            ),
        )
        .into_iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect()
    };
    assert_eq!(page(0, 3), vec![1, 2, 3]);
    assert_eq!(page(3, 3), vec![4, 5, 6]);
    assert_eq!(page(9, 3), vec![10], "partial last page");
    assert!(page(10, 3).is_empty(), "offset at boundary → empty");
    assert!(page(20, 3).is_empty(), "offset beyond → empty");
}

// ═══════════════════════════════════════════════════(*)════════════════
// COUNT(*) vs COUNT(column) — COUNT(col) skips NULLs.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn count_star_vs_count_column_with_nulls() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.insert_row("t", vec![Value::Integer(1), Value::Integer(10)])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(2), Value::Null])
        .unwrap();
    db.insert_row("t", vec![Value::Integer(3), Value::Integer(30)])
        .unwrap();

    let star = one(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(star[0], Value::Integer(3));

    let col = one(&db, "SELECT COUNT(v) FROM t");
    // COUNT(v) must ignore the NULL → 2, not 3.
    match col[0] {
        Value::Integer(n) => assert_eq!(n, 2, "COUNT(v) should skip NULLs"),
        ref o => panic!("expected Integer, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// SUM/MIN/MAX correctness with negatives.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn aggregates_with_negative_and_mixed() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for (i, v) in [10, -5, 7, -3, 0].iter().enumerate() {
        db.insert_row("t", vec![Value::Integer(i as i64 + 1), Value::Integer(*v)])
            .unwrap();
    }
    let sum = one(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sum[0], Value::Integer(10 - 5 + 7 - 3 + 0));
    assert_eq!(sum[0], Value::Integer(9));

    let min = one(&db, "SELECT MIN(v) FROM t");
    assert_eq!(min[0], Value::Integer(-5));

    let max = one(&db, "SELECT MAX(v) FROM t");
    assert_eq!(max[0], Value::Integer(10));
}

// ═══════════════════════════════════════════════════════════════════════
// DDL lifecycle: DROP then recreate with same name.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn drop_then_recreate_same_name() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.insert_row("t", vec![Value::Integer(1), Value::Integer(100)])
        .unwrap();
    assert_eq!(one(&db, "SELECT COUNT(*) FROM t")[0], Value::Integer(1));

    db.execute("DROP TABLE t").unwrap();
    // Recreate with a different schema — old data must not leak through.
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, label TEXT)")
        .unwrap();
    assert_eq!(one(&db, "SELECT COUNT(*) FROM t")[0], Value::Integer(0));
    db.insert_row("t", vec![Value::Integer(1), Value::Text("hi".into())])
        .unwrap();
    let r = one(&db, "SELECT label FROM t WHERE id = 1");
    assert_eq!(r[0], Value::Text("hi".into()));
}

// ═══════════════════════════════════════════════════════════════════════
// UPDATE all rows (no WHERE) and UPDATE setting multiple columns.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn update_all_rows_and_multi_column() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    for i in 1..=4 {
        db.insert_row(
            "t",
            vec![Value::Integer(i), Value::Integer(0), Value::Integer(0)],
        )
        .unwrap();
    }
    // UPDATE without WHERE affects every row.
    db.execute("UPDATE t SET a = 99").unwrap();
    let r = rows(&db, "SELECT a FROM t ORDER BY id");
    assert!(r.iter().all(|row| row[0] == Value::Integer(99)));

    // Multi-column update with WHERE.
    db.execute("UPDATE t SET a = 1, b = 2 WHERE id = 2")
        .unwrap();
    let r = one(&db, "SELECT a, b FROM t WHERE id = 2");
    assert_eq!(r[0], Value::Integer(1));
    assert_eq!(r[1], Value::Integer(2));

    // Other rows untouched.
    let r = one(&db, "SELECT a, b FROM t WHERE id = 3");
    assert_eq!(r[0], Value::Integer(99));
    assert_eq!(r[1], Value::Integer(0));
}

// ═══════════════════════════════════════════════════════════════════════
// DELETE all rows.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn delete_all_rows() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=5 {
        db.insert_row("t", vec![Value::Integer(i), Value::Integer(i)])
            .unwrap();
    }
    db.execute("DELETE FROM t").unwrap();
    assert_eq!(one(&db, "SELECT COUNT(*) FROM t")[0], Value::Integer(0));
    // Re-insert should work (table still exists, empty).
    db.insert_row("t", vec![Value::Integer(1), Value::Integer(42)])
        .unwrap();
    assert_eq!(
        one(&db, "SELECT v FROM t WHERE id = 1")[0],
        Value::Integer(42)
    );
}
