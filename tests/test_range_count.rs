//! Regression test for range-predicate / compound-filter COUNT correctness.
//!
//! `SELECT COUNT(*) FROM t WHERE col OP N` must honor the operator (> >= < <= !=),
//! and AND-compounds like `cat = 3 AND amount > 150` must return the right count.
//! Previously col_segment_multi_aggregate discarded the operator and always
//! compared with equality, silently turning `id > 49000` into `id == 49000`
//! (returning 0 rows).

use motedb::sql::QueryResult;
use motedb::Database;
use tempfile::TempDir;

fn count(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    match r {
        QueryResult::Select { rows, .. } => rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| {
                if let motedb::types::Value::Integer(i) = v {
                    Some(*i)
                } else {
                    None
                }
            })
            .unwrap_or(-999),
        _ => -999,
    }
}

#[test]
fn test_range_count_after_flush() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat INTEGER)")
        .unwrap();

    // Insert 200 rows: id 1..200, cat = id % 10
    for i in 1..=200i64 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 10))
            .unwrap();
    }

    // Force data into the columnar store.
    db.flush().unwrap();
    db.wait_for_indexes_ready();

    // Range predicates on PK
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id > 195"),
        5,
        "id > 195"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id >= 195"),
        6,
        "id >= 195"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id < 5"),
        4,
        "id < 5"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id <= 5"),
        5,
        "id <= 5"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id != 1"),
        199,
        "id != 1"
    );

    // Range predicates on non-PK column
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE cat > 7"),
        40,
        "cat > 7"
    ); // cat 8,9 → 20 each
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE cat >= 8"),
        40,
        "cat >= 8"
    );

    // AND compound filter
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE cat = 3 AND id > 150"),
        5, // cat=3 at id 153,163,173,183,193 → 5
        "cat = 3 AND id > 150"
    );
}
