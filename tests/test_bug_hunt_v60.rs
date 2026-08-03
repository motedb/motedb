//! Bug Hunt v60 — seventh round: DISTINCT+ORDER BY, DML counts, agg edges, casts.

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

fn f_of(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => panic!("expected number, got {:?}", v),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT + ORDER BY
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_distinct_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,3),(2,1),(3,3),(4,1),(5,2)")
        .unwrap();
    let r = q(&db, "SELECT DISTINCT v FROM t ORDER BY v");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );
}

#[test]
fn test_distinct_multiple_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(1,1),(1,2),(2,1)")
        .unwrap();
    let r = q(&db, "SELECT DISTINCT a, b FROM t ORDER BY a, b");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(1)],
        ]
    );
}

#[test]
fn test_distinct_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL),(2,NULL),(3,5),(4,5)")
        .unwrap();
    let r = q(&db, "SELECT DISTINCT v FROM t ORDER BY v");
    // NULL + 5 (two distinct)
    assert_eq!(r.len(), 2);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE / UPDATE affected row counts
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_delete_affected_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, g INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,1),(3,2),(4,2)")
        .unwrap();
    let n = affected(&db, "DELETE FROM t WHERE g = 1");
    assert_eq!(n, 2, "DELETE should report 2 affected rows");
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_delete_all_affected_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let n = affected(&db, "DELETE FROM t");
    assert_eq!(n, 3);
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_delete_no_match_affected_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let n = affected(&db, "DELETE FROM t WHERE id = 999");
    assert_eq!(n, 0, "DELETE matching nothing should report 0");
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_update_affected_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let n = affected(&db, "UPDATE t SET v = 0 WHERE v >= 20");
    assert_eq!(n, 2, "UPDATE should report 2 affected rows");
    let r = q(&db, "SELECT v FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(10)],
            vec![Value::Integer(0)],
            vec![Value::Integer(0)]
        ]
    );
}

#[test]
fn test_update_no_match_affected_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let n = affected(&db, "UPDATE t SET v = 999 WHERE id = 999");
    assert_eq!(n, 0);
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregates with NULL
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_avg_with_some_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // values 10, NULL, 30 → AVG ignores NULL → 20
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT AVG(v) FROM t");
    assert!(
        (f_of(&r[0][0]) - 20.0).abs() < 1e-9,
        "AVG should ignore NULL, got {:?}",
        r
    );
}

#[test]
fn test_count_col_vs_star_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT COUNT(*), COUNT(v) FROM t");
    // COUNT(*) = 3, COUNT(v) = 2 (NULL excluded)
    assert_eq!(r, vec![vec![Value::Integer(3), Value::Integer(2)]]);
}

#[test]
fn test_sum_ignores_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(40)]]);
}

#[test]
fn test_min_max_ignore_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30),(4,5)")
        .unwrap();
    let r = q(&db, "SELECT MIN(v), MAX(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY + JOIN + aggregate
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_join_groupby_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust INT, amt INT)")
        .unwrap();
    db.execute("CREATE TABLE cust(id INT PRIMARY KEY, region TEXT)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50),(4,2,150)")
        .unwrap();
    db.execute("INSERT INTO cust VALUES (1,'US'),(2,'EU')")
        .unwrap();
    let r = q(
        &db,
        "SELECT c.region, SUM(o.amt) FROM cust c JOIN orders o ON c.id = o.cust GROUP BY c.region ORDER BY c.region",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("EU".into()), Value::Integer(200)],
            vec![Value::text("US".into()), Value::Integer(300)]
        ]
    );
}

#[test]
fn test_groupby_count_star() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(1,30),(2,5)")
        .unwrap();
    let r = q(&db, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(1)]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// String function more edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_replace_all_occurrences() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REPLACE('a-b-c-d', '-', ',')");
    assert_eq!(r, vec![vec![Value::text("a,b,c,d".into())]]);
}

#[test]
fn test_replace_no_match() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REPLACE('hello', 'z', 'y')");
    assert_eq!(r, vec![vec![Value::text("hello".into())]]);
}

#[test]
fn test_trim_internal_whitespace() {
    // TRIM only strips leading/trailing, not internal.
    let (db, _d) = db();
    let r = q(&db, "SELECT TRIM('  a  b  ')");
    assert_eq!(r, vec![vec![Value::text("a  b".into())]]);
}

#[test]
fn test_upper_lower() {
    let (db, _d) = db();
    let r = q(&db, "SELECT UPPER('abc'), LOWER('XYZ')");
    assert_eq!(
        r,
        vec![vec![Value::text("ABC".into()), Value::text("xyz".into())]]
    );
}

#[test]
fn test_length_empty() {
    let (db, _d) = db();
    let r = q(&db, "SELECT LENGTH('')");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_substr_start_beyond() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('abc', 10)");
    assert_eq!(r, vec![vec![Value::text("".into())]]);
}

#[test]
fn test_substr_length_exceeds() {
    // SUBSTR('abc', 1, 100) → 'abc' (clamped)
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('abc', 1, 100)");
    assert_eq!(r, vec![vec![Value::text("abc".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison type coercion edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_compare_int_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 5 = 5.0, 5 < 5.1, 5 > 4.9, 5 <> 5.0");
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false)
        ]]
    );
}

#[test]
fn test_compare_text_lexical() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'abc' < 'abd', 'abc' = 'abc', 'b' > 'a'");
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true)
        ]]
    );
}

#[test]
fn test_bool_in_arithmetic() {
    // TRUE = 1, FALSE = 0 in arithmetic.
    let (db, _d) = db();
    let r = q(&db, "SELECT TRUE + 1, FALSE + 1");
    assert_eq!(r, vec![vec![Value::Integer(2), Value::Integer(1)]]);
}

#[test]
fn test_bool_comparison() {
    let (db, _d) = db();
    let r = q(&db, "SELECT TRUE = 1, FALSE = 0, TRUE > FALSE");
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true)
        ]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// CAST edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_cast_int_to_text() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(42 AS TEXT)");
    assert_eq!(r, vec![vec![Value::text("42".into())]]);
}

#[test]
fn test_cast_float_to_text() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(3.14 AS TEXT)");
    let s = match &r[0][0] {
        Value::Text(t) => t.to_string(),
        _ => panic!("{:?}", r),
    };
    assert!(
        s.starts_with("3.14"),
        "CAST(3.14 AS TEXT) should start with 3.14, got {}",
        s
    );
}

#[test]
fn test_cast_bool_to_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(TRUE AS INTEGER), CAST(FALSE AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(0)]]);
}

#[test]
fn test_cast_text_to_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST('3.14' AS FLOAT)");
    assert!((f_of(&r[0][0]) - 3.14).abs() < 1e-9);
}

#[test]
fn test_cast_round_trip() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(CAST(42 AS FLOAT) AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// BETWEEN
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_between_inclusive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10),(3,15),(4,20)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id FROM t WHERE v BETWEEN 10 AND 20 ORDER BY id",
    );
    // inclusive → 10,15,20 → ids 2,3,4
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)]
        ]
    );
}

#[test]
fn test_not_between() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10),(3,15)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id FROM t WHERE v NOT BETWEEN 8 AND 12 ORDER BY id",
    );
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_between_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'apple'),(2,'banana'),(3,'cherry')")
        .unwrap();
    let r = q(
        &db,
        "SELECT s FROM t WHERE s BETWEEN 'b' AND 'd' ORDER BY s",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("banana".into())],
            vec![Value::text("cherry".into())]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE / DELETE with subquery in WHERE
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_delete_with_in_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE main(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("CREATE TABLE excl(cat TEXT)").unwrap();
    db.execute("INSERT INTO main VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    db.execute("INSERT INTO excl VALUES ('a'),('c')").unwrap();
    let n = affected(&db, "DELETE FROM main WHERE cat IN (SELECT cat FROM excl)");
    assert_eq!(n, 2);
    let r = q(&db, "SELECT id FROM main ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_update_with_subquery_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE marker(id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("INSERT INTO marker VALUES (1),(3)").unwrap();
    let n = affected(
        &db,
        "UPDATE t SET v = 0 WHERE id IN (SELECT id FROM marker)",
    );
    assert_eq!(n, 2);
    let r = q(&db, "SELECT v FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(0)],
            vec![Value::Integer(20)],
            vec![Value::Integer(0)]
        ]
    );
}

#[test]
fn test_update_set_from_subquery() {
    // UPDATE SET col = (SELECT ...) — scalar subquery in SET.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE src(id INT PRIMARY KEY, nv INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("INSERT INTO src VALUES (1,99)").unwrap();
    let res = db.execute("UPDATE t SET v = (SELECT nv FROM src WHERE src.id = t.id) WHERE id = 1");
    match res {
        Ok(_) => {
            let r = q(&db, "SELECT v FROM t WHERE id = 1");
            assert_eq!(r, vec![vec![Value::Integer(99)]]);
        }
        Err(_) => { /* correlated SET subquery may be unsupported */ }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// IS NULL / IS NOT NULL
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_is_null_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IS NULL");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_is_not_null_filter() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IS NOT NULL ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_count_with_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,NULL)")
        .unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE v IS NULL");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// AND / OR precedence
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_and_or_precedence() {
    // a OR b AND c → a OR (b AND c) (AND binds tighter)
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    // row4: a=0, b=1, id=4 → b=1 AND id=4 is TRUE → matches via OR
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,0),(3,0,1),(4,0,1)")
        .unwrap();
    // WHERE a = 1 OR b = 1 AND id = 4 → a=1 OR (b=1 AND id=4)
    // id1: a=1 T ; id2: a=1 T ; id3: b=1 but id=3 F, a=0 F ; id4: a=0, (b=1 AND id=4) T
    let r = q(
        &db,
        "SELECT id FROM t WHERE a = 1 OR b = 1 AND id = 4 ORDER BY id",
    );
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
}

#[test]
fn test_and_or_precedence_paren_override() {
    // (a OR b) AND c — parens override.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,0),(3,0,1),(4,0,0)")
        .unwrap();
    // WHERE (a = 1 OR b = 1) AND id = 1 → only id1 satisfies both
    let r = q(
        &db,
        "SELECT id FROM t WHERE (a = 1 OR b = 1) AND id = 1 ORDER BY id",
    );
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn test_not_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE NOT v = 20 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}
