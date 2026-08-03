//! Bug Hunt v96 — round 23: transaction isolation edges, ORDER BY with
//! mixed NULL/non-NULL at scale, GROUP BY with multiple aggregates including
//! COUNT(DISTINCT), and consistency of the recent fixes under varied data.

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

fn sorted_int(r: &[Vec<Value>]) -> Vec<i64> {
    let mut v: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.get(0) {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction: UPDATE within txn visible to same connection.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_txn_update_visibility() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    // Within txn, see updated value.
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(
        r,
        vec![vec![Value::Integer(99)]],
        "updated value visible within txn"
    );
    db.execute("COMMIT").unwrap();
    let r2 = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r2, vec![vec![Value::Integer(99)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction: DELETE within txn visible.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_txn_delete_visibility() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1)]],
        "delete visible within txn"
    );
    db.execute("COMMIT").unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction rollback restores UPDATE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_txn_rollback_update() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(10)]], "UPDATE rolled back");
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction rollback restores DELETE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_txn_rollback_delete() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]], "DELETE rolled back");
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with COUNT(DISTINCT) and SUM and AVG together.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_multi_agg_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT, w INT)")
        .unwrap();
    db.execute(
        "INSERT INTO t VALUES (1,'a',10,1),(2,'a',20,1),(3,'a',10,2),(4,'b',5,1),(5,'b',5,2)",
    )
    .unwrap();
    let mut r = q(
        &db,
        "SELECT cat, COUNT(*), COUNT(DISTINCT v), SUM(w), AVG(v) FROM t GROUP BY cat",
    );
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    // a: COUNT=3, COUNT(DISTINCT v)={10,20}=2, SUM(w)=1+1+2=4, AVG(v)=(10+20+10)/3=13.33.
    // b: COUNT=2, COUNT(DISTINCT v)={5}=1, SUM(w)=1+2=3, AVG(v)=(5+5)/2=5.
    assert_eq!(r.len(), 2);
    let a = &r[0]; // 'a'
    assert_eq!(a[1], Value::Integer(3));
    assert_eq!(a[2], Value::Integer(2));
    assert_eq!(a[3], Value::Integer(4));
    let b = &r[1]; // 'b'
    assert_eq!(b[1], Value::Integer(2));
    assert_eq!(b[2], Value::Integer(1));
    assert_eq!(b[3], Value::Integer(3));
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY with NULLs at scale (many rows).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_nulls_scale() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Mix of NULL and non-NULL.
    for i in 1..=10 {
        let v = if i % 3 == 0 {
            "NULL".to_string()
        } else {
            format!("{}", i * 10)
        };
        db.execute(&format!("INSERT INTO t VALUES ({},{})", i, v))
            .unwrap();
    }
    let r = q(&db, "SELECT id FROM t ORDER BY v ASC, id ASC");
    // Engine sorts NULLs FIRST in ASC. Non-NULL v: 10,20,40,50,70,80,100 (ids 1,2,4,5,7,8,10). NULL: ids 3,6,9.
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 10, "all 10 rows present");
    // NULLs first (ids 3,6,9), then non-NULLs in v-then-id order.
    let nulls_first = &[3i64, 6, 9];
    let non_null_after = &[1i64, 2, 4, 5, 7, 8, 10];
    assert_eq!(&ids[..3], nulls_first, "NULLs sort first in ASC");
    assert_eq!(
        &ids[3..],
        non_null_after,
        "non-NULL v in ASC order after NULLs"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY then ORDER BY selected aggregate DESC with ties.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_order_agg_ties() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    // Two cats with same SUM.
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'b',10),(3,'a',20),(4,'b',20)")
        .unwrap();
    let r = q(
        &db,
        "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY SUM(v) DESC, cat ASC",
    );
    // Both SUM=30. Tie broken by cat ASC: a, b.
    assert_eq!(
        r,
        vec![
            vec![Value::Text("a".into()), Value::Integer(30)],
            vec![Value::Text("b".into()), Value::Integer(30)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with NOT (compound expression).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_not_compound() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20),(2,30,5),(3,10,5)")
        .unwrap();
    // NOT (a > 20 AND b > 10) → id1 (not(10>20 and ..)=TRUE), id2 (not(30>20 and 5>10)=not(false)=TRUE), id3 (not(10>20..)=TRUE).
    // Actually all: id1 a=10 not>20 → AND false → not=true. id2 a=30>20 but b=5 not>10 → AND false → not=true. id3 a=10 → false → not=true.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE NOT (a > 20 AND b > 10)"));
    assert_eq!(
        r,
        vec![1, 2, 3],
        "no row has both a>20 AND b>10, so all pass NOT"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Float column aggregate with mixed int/float storage.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_float_aggregate_mixed() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1.5),(2,2.5),(3,4.0)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Float(f) => assert!((f - 8.0).abs() < 1e-9, "SUM=8.0, got {}", f),
        other => panic!("SUM float = 8.0, got {:?}", other),
    }
    match &r[0][1] {
        Value::Float(f) => assert!((f - (8.0 / 3.0)).abs() < 1e-9, "AVG=8/3, got {}", f),
        other => panic!("AVG float, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in WHERE with BETWEEN.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_between() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    // v BETWEEN (SELECT MIN(v)+5) AND (SELECT MAX(v)-5).
    // MIN+5=15, MAX-5=35. BETWEEN 15 AND 35 → id2(20), id3(30).
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v BETWEEN (SELECT MIN(v) FROM t) + 5 AND (SELECT MAX(v) FROM t) - 5"));
    assert_eq!(r, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple GROUP BY columns with ORDER BY on second group col.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_multi_order_second() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2)")
        .unwrap();
    let r = q(
        &db,
        "SELECT a, b, COUNT(*) FROM t GROUP BY a, b ORDER BY b ASC, a ASC",
    );
    // Groups: (1,1),(1,2),(2,1),(2,2). ORDER BY b,a: (1,1),(2,1),(1,2),(2,2).
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Self-join producing pairs.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_self_join_pairs() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    // Pairs where a.v < b.v.
    let r = q(&db, "SELECT a.id, b.id FROM t a JOIN t b ON a.v < b.v");
    // Pairs: (1,2),(1,3),(2,3).
    assert_eq!(r.len(), 3);
    // Verify all pairs have a.id < b.id (a.v < b.v).
    for row in &r {
        let a = match &row[0] {
            Value::Integer(i) => *i,
            _ => -999,
        };
        let b = match &row[1] {
            Value::Integer(i) => *i,
            _ => -999,
        };
        assert!(a < b, "pair must have a.id < b.id; got ({},{})", a, b);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT with WHERE and GROUP BY and HAVING and ORDER BY and LIMIT (full).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_full_count_combo() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, sub TEXT)")
        .unwrap();
    // cat 'a' has 4 subs, 'b' has 3, 'c' has 2.
    db.execute("INSERT INTO t VALUES (1,'a','x'),(2,'a','y'),(3,'a','z'),(4,'a','w'),(5,'b','x'),(6,'b','y'),(7,'b','z'),(8,'c','x'),(9,'c','y')").unwrap();
    // WHERE sub != 'w', GROUP BY cat, HAVING COUNT >= 2, ORDER BY COUNT DESC, LIMIT 2.
    // After WHERE (excl id4 sub=w): a=3,b=3,c=2. HAVING>=2: all. ORDER BY COUNT DESC: a(3),b(3),c(2).
    // LIMIT 2 → a,b (tie between a,b; order may vary but both count 3).
    let r = q(&db, "SELECT cat, COUNT(*) FROM t WHERE sub != 'w' GROUP BY cat HAVING COUNT(*) >= 2 ORDER BY COUNT(*) DESC LIMIT 2");
    assert_eq!(r.len(), 2);
    // Both should have count 3.
    for row in &r {
        assert_eq!(row[1], Value::Integer(3));
    }
}

// ─────────────────────────────────────────────────────────────────────────
// IN with TEXT values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_text_values() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'foo'),(2,'bar'),(3,'baz'),(4,'qux')")
        .unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE s IN ('foo', 'baz', 'zzz')"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT with ORDER BY on the distinct column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_order_by_same() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,30),(4,10),(5,20)")
        .unwrap();
    let r = q(&db, "SELECT DISTINCT v FROM t ORDER BY v ASC");
    let vals: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![10, 20, 30]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over empty result of subquery (IN empty).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_over_empty_in() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // No rows match empty IN → COUNT=0, SUM=NULL.
    assert_eq!(
        q(
            &db,
            "SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM t WHERE v > 100)"
        ),
        vec![vec![Value::Integer(0)]]
    );
    assert_eq!(
        q(
            &db,
            "SELECT SUM(v) FROM t WHERE id IN (SELECT id FROM t WHERE v > 100)"
        ),
        vec![vec![Value::Null]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Update then index query consistency (multiple updates).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_indexed_multi_update() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.wait_for_indexes_ready();
    // Multiple updates changing indexed column.
    db.execute("UPDATE t SET cat = 'x' WHERE id = 1").unwrap();
    db.execute("UPDATE t SET cat = 'x' WHERE id = 2").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE cat = 'x'"));
    assert_eq!(r, vec![1, 2]);
    // 'a' and 'b' gone.
    let r2 = q(&db, "SELECT id FROM t WHERE cat = 'a'");
    assert!(r2.is_empty());
    let r3 = q(&db, "SELECT id FROM t WHERE cat = 'b'");
    assert!(r3.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────
// Negative literal in WHERE comparison with column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_negative_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-10),(2,0),(3,-5),(4,10)")
        .unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v < -3"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on FLOAT column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_float() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,3.14),(2,1.41),(3,2.71)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY v ASC");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![2, 3, 1]); // 1.41, 2.71, 3.14.
}
