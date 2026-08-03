//! Bug Hunt v84 — round 11: deep edge cases — empty/single-row table behavior,
//! NULL in aggregates across paths, CAST chains, repeated predicates,
//! subquery returning 0 rows vs NULL, GROUP BY ordinal, HAVING without aggregates.

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
// Single-row table aggregates.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_single_row_aggregates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,42)").unwrap();
    assert_eq!(
        q(
            &db,
            "SELECT COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM t"
        ),
        vec![vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Integer(42),
            Value::Integer(42),
            Value::Integer(42)
        ]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Empty table SELECT * returns no rows (not error).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_empty_table_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let r = q(&db, "SELECT * FROM t");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// Empty table WHERE returns no rows.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_empty_table_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let r = q(&db, "SELECT * FROM t WHERE v > 5");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY ordinal (1-based). NOTE: the parser only accepts column names in
// GROUP BY (parse_column_list), so `GROUP BY 1` raises a clear ParseError.
// ORDER BY supports ordinals, but GROUP BY does not yet — this documents the
// feature gap (not a silent wrong result). We verify GROUP BY by column name
// works (the supported form).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_column_name() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)")
        .unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![
            vec![Value::Text("a".into()), Value::Integer(2)],
            vec![Value::Text("b".into()), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING without aggregates (filter on group key).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_on_group_key() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'c',100)")
        .unwrap();
    let mut r = q(&db, "SELECT cat FROM t GROUP BY cat HAVING cat > 'a'");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![vec![Value::Text("b".into())], vec![Value::Text("c".into())]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Repeated predicate (v = 10 AND v = 10).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_repeated_predicate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v = 10 AND v = 10");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Contradictory predicate (v = 10 AND v = 20).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_contradictory_predicate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v = 10 AND v = 20");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// Tautology (v = 10 OR v != 10) — all non-NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_tautology_or() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,NULL)")
        .unwrap();
    // v=10 OR v!=10: id1 TRUE; id2 TRUE; id3 (NULL=10)=NULL OR (NULL!=10)=NULL → NULL.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = 10 OR v != 10"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery returning NULL compared with >.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_null_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE empty(id INT PRIMARY KEY, x INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // (SELECT MAX(x) FROM empty) is NULL. v > NULL → NULL → no rows.
    let r = q(&db, "SELECT id FROM t WHERE v > (SELECT MAX(x) FROM empty)");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT of all-NULL column = 0.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_all_null_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL),(3,NULL)")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT COUNT(v) FROM t"),
        vec![vec![Value::Integer(0)]]
    );
    assert_eq!(
        q(&db, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Integer(3)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// SUM of all-NULL = NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_all_null_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL)")
        .unwrap();
    assert_eq!(q(&db, "SELECT SUM(v) FROM t"), vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// MIN/MAX of all-NULL = NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_max_all_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL)")
        .unwrap();
    assert_eq!(q(&db, "SELECT MIN(v) FROM t"), vec![vec![Value::Null]]);
    assert_eq!(q(&db, "SELECT MAX(v) FROM t"), vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate ignoring NULL in middle of values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_null_in_middle() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT SUM(v) FROM t"),
        vec![vec![Value::Integer(40)]]
    );
    assert_eq!(
        q(&db, "SELECT COUNT(v) FROM t"),
        vec![vec![Value::Integer(2)]]
    );
    assert_eq!(
        q(&db, "SELECT MIN(v) FROM t"),
        vec![vec![Value::Integer(10)]]
    );
    assert_eq!(
        q(&db, "SELECT MAX(v) FROM t"),
        vec![vec![Value::Integer(30)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on column with all same values (stable).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_constant() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,5),(3,5)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY v");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 3);
    // All present (order among ties unspecified).
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT literal + column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_select_literal_plus_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT v + 5 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(15)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with expression on both sides.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_expr_both_sides() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,5),(2,20,10)")
        .unwrap();
    // a - b = 5 → both match (10-5=5, 20-10=10→no). Actually id2: 20-10=10≠5.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a - b = 5"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested COALESCE / CASE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_coalesce_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,5),(3,0)")
        .unwrap();
    let r = q(
        &db,
        "SELECT COALESCE(CASE WHEN v IS NULL THEN -1 ELSE v END, -99) FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(-1)], // v=NULL → CASE → -1
            vec![Value::Integer(5)],  // v=5 → 5
            vec![Value::Integer(0)],  // v=0 → 0
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple ORDER BY keys with NULLs.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_multi_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,NULL),(2,1,10),(3,2,NULL),(4,2,5)")
        .unwrap();
    // ORDER BY a ASC, b ASC. NULLs sorted first (ASC) in many engines.
    let r = q(&db, "SELECT id FROM t ORDER BY a ASC, b ASC");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    // a=1 group then a=2 group. Within a=1: NULL before 10 (if NULLs-first).
    // Just verify a-grouping is correct (a=1 rows before a=2 rows).
    let pos1 = ids.iter().position(|&i| i == 1).unwrap();
    let pos2 = ids.iter().position(|&i| i == 2).unwrap();
    let pos3 = ids.iter().position(|&i| i == 3).unwrap();
    let pos4 = ids.iter().position(|&i| i == 4).unwrap();
    assert!(
        pos1 < pos3 && pos1 < pos4 && pos2 < pos3 && pos2 < pos4,
        "a=1 rows (id1,id2) must come before a=2 rows (id3,id4); got {:?}",
        ids
    );
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT with ORDER BY on different column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_order_by_other() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3,'a'),(1,'b'),(2,'a')")
        .unwrap();
    // DISTINCT cat, ORDER BY id.
    let r = q(&db, "SELECT DISTINCT cat FROM t ORDER BY id");
    // DISTINCT may collapse; order by id. Just verify both cats present.
    let cats: Vec<String> = r
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Text(s) => Some(s.as_str().to_string()),
            _ => None,
        })
        .collect();
    let mut sorted_cats = cats.clone();
    sorted_cats.sort();
    assert_eq!(sorted_cats, vec!["a".to_string(), "b".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate in HAVING referencing different column than SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_different_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT, w INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10,1),(2,'a',20,2),(3,'b',5,10)")
        .unwrap();
    // SELECT cat, SUM(v) HAVING SUM(w) > 5.
    // 'a': SUM(w)=3 (not >5). 'b': SUM(w)=10 (>5).
    let r = q(
        &db,
        "SELECT cat, SUM(v) FROM t GROUP BY cat HAVING SUM(w) > 5",
    );
    assert_eq!(r, vec![vec![Value::Text("b".into()), Value::Integer(5)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Text comparison with numbers stored as text.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_text_numeric_lexicographic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'10'),(2,'9'),(3,'100')")
        .unwrap();
    // Text ordering: '10' < '100' < '9' (lexicographic).
    let r = q(&db, "SELECT s FROM t ORDER BY s ASC");
    let vals: Vec<String> = r
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Text(s) => Some(s.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        vals,
        vec!["10".to_string(), "100".to_string(), "9".to_string()],
        "text '10' < '100' < '9' lexicographically"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE all rows to same value.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_all_same() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("UPDATE t SET v = 0").unwrap();
    let mut r = q(&db, "SELECT v FROM t");
    r.sort_by_key(|row| match row[0] {
        Value::Integer(i) => i,
        _ => 999,
    });
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(0)],
            vec![Value::Integer(0)],
            vec![Value::Integer(0)]
        ]
    );
}
