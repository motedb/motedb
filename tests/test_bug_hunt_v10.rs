//! Bug-hunt v10: WHERE + aggregate interaction, HAVING edge cases,
//! multi-column index queries, and SELECT count distinct correctness.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize().unwrap_or_else(|e| panic!("mat failed: {}\n  err: {}", sql, e));
    match rs { QueryResult::Select { rows, .. } => rows, _ => panic!("not Select") }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("int? {:?}: {}", o, sql) }
}

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1);
    match r[0].first() {
        Some(Value::Float(n)) => *n,
        Some(Value::Integer(n)) => *n as f64,
        o => panic!("float? {:?}: {}", o, sql),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. WHERE + COUNT(*) correctness (the v9 bug class — thorough re-check)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_where_text_column_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=20 {
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i));
    }
    // COUNT(*) WHERE cat = 'even' → 10 rows (ids 2,4,...,20).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'even'"), 10);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'odd'"), 10);
}

#[test]
fn sum_where_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=10 {
        let cat = if i <= 5 { "a" } else { "b" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i * 10));
    }
    // SUM(v) WHERE cat = 'a' → 10+20+30+40+50 = 150.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'a'"), 150);
    // cat 'b': i=6..10 → v=60,70,80,90,100 → SUM = 400.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'b'"), 400);
}

#[test]
fn min_max_where_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=20 {
        let cat = if i <= 10 { "low" } else { "high" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i));
    }
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t WHERE cat = 'low'"), 1);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t WHERE cat = 'low'"), 10);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t WHERE cat = 'high'"), 11);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t WHERE cat = 'high'"), 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. GROUP BY + WHERE (the v9 fix — thorough verification)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_where_correct_counts() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, region TEXT, status TEXT)");
    // region=US: 3 active, 2 inactive. region=EU: 2 active, 1 inactive.
    exec(&db, "INSERT INTO t VALUES (1, 'US', 'active')");
    exec(&db, "INSERT INTO t VALUES (2, 'US', 'active')");
    exec(&db, "INSERT INTO t VALUES (3, 'US', 'inactive')");
    exec(&db, "INSERT INTO t VALUES (4, 'US', 'active')");
    exec(&db, "INSERT INTO t VALUES (5, 'US', 'inactive')");
    exec(&db, "INSERT INTO t VALUES (6, 'EU', 'active')");
    exec(&db, "INSERT INTO t VALUES (7, 'EU', 'active')");
    exec(&db, "INSERT INTO t VALUES (8, 'EU', 'inactive')");
    // GROUP BY region WHERE status = 'active': US=3, EU=2.
    let r = rows(&db, "SELECT region, COUNT(*) FROM t WHERE status = 'active' GROUP BY region ORDER BY region");
    assert_eq!(r.len(), 2);
    match (&r[0][0], &r[0][1]) {
        (Value::Text(reg), Value::Integer(cnt)) => {
            assert_eq!(&*reg.0, "EU");
            assert_eq!(*cnt, 2);
        }
        o => panic!("{:?}", o),
    }
    match (&r[1][0], &r[1][1]) {
        (Value::Text(reg), Value::Integer(cnt)) => {
            assert_eq!(&*reg.0, "US");
            assert_eq!(*cnt, 3);
        }
        o => panic!("{:?}", o),
    }
}

#[test]
fn group_by_where_sum_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'a', 5)");
    exec(&db, "INSERT INTO t VALUES (4, 'b', 100)");
    exec(&db, "INSERT INTO t VALUES (5, 'b', 200)");
    // SUM per cat WHERE v >= 10: a → 10+20=30 (5 excluded), b → 100+200=300.
    let r = rows(&db, "SELECT cat, SUM(v) FROM t WHERE v >= 10 GROUP BY cat ORDER BY cat");
    assert_eq!(r.len(), 2);
    match (&r[0][0], &r[0][1]) {
        (Value::Text(c), Value::Integer(s)) => { assert_eq!(&*c.0, "a"); assert_eq!(*s, 30); }
        o => panic!("{:?}", o),
    }
    match (&r[1][0], &r[1][1]) {
        (Value::Text(c), Value::Integer(s)) => { assert_eq!(&*c.0, "b"); assert_eq!(*s, 300); }
        o => panic!("{:?}", o),
    }
}

#[test]
fn group_by_where_empty_result() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'b', 20)");
    // WHERE filters all → no groups.
    let r = rows(&db, "SELECT cat, COUNT(*) FROM t WHERE v > 1000 GROUP BY cat");
    assert_eq!(r.len(), 0, "WHERE filtering all → 0 groups");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. HAVING with WHERE (both filters apply)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn having_with_where_both_apply() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    // cat 'a': v=10,20,30,40 (4 rows). cat 'b': v=5,15 (2 rows).
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'a', 30)");
    exec(&db, "INSERT INTO t VALUES (4, 'a', 40)");
    exec(&db, "INSERT INTO t VALUES (5, 'b', 5)");
    exec(&db, "INSERT INTO t VALUES (6, 'b', 15)");
    // WHERE v >= 15: a→20,30,40 (sum=90, count=3), b→15 (sum=15, count=1).
    // HAVING COUNT(*) >= 2: only 'a' (count=3).
    let r = rows(&db, "SELECT cat, COUNT(*), SUM(v) FROM t WHERE v >= 15 GROUP BY cat HAVING COUNT(*) >= 2 ORDER BY cat");
    assert_eq!(r.len(), 1, "only 'a' passes both WHERE and HAVING");
    match (&r[0][0], &r[0][1], &r[0][2]) {
        (Value::Text(c), Value::Integer(cnt), Value::Integer(s)) => {
            assert_eq!(&*c.0, "a");
            assert_eq!(*cnt, 3);
            assert_eq!(*s, 90);
        }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. COUNT(DISTINCT) with WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_distinct_with_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 10)"); // dup v
    exec(&db, "INSERT INTO t VALUES (3, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (4, 'b', 10)");
    exec(&db, "INSERT INTO t VALUES (5, 'b', 30)");
    // COUNT(DISTINCT v) WHERE cat = 'a': distinct {10, 20} = 2.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(DISTINCT v) FROM t WHERE cat = 'a'"), 2);
    // WHERE cat = 'b': distinct {10, 30} = 2.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(DISTINCT v) FROM t WHERE cat = 'b'"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. AVG with WHERE (float division correctness)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn avg_where_float_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'a', 25)");
    // AVG(v) WHERE cat = 'a' = (10+20+25)/3 = 18.333.
    let a = scalar_f64(&db, "SELECT AVG(v) FROM t WHERE cat = 'a'");
    assert!((a - 18.333).abs() < 0.01, "AVG = 18.33, got {}", a);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Multiple WHERE conditions with AND on different columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_and_two_cols_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, {})", i, i, i * 2));
    }
    // WHERE a > 5 AND b < 30: a in 6..14 (b=12..28), so a=6..14 (9 rows).
    // a>5: ids 6..20 (15). b<30: b=2*id<30 → id<15 → ids 1..14 (14).
    // Intersection: ids 6..14 = 9 rows.
    let r = rows(&db, "SELECT id FROM t WHERE a > 5 AND b < 30 ORDER BY id");
    assert_eq!(r.len(), 9);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. ORDER BY + WHERE + LIMIT (combined)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_where_limit_combined() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, 100 - i));
    }
    // WHERE v > 80 → v=81..99 → ids where 100-id>80 → id<20 → ids 1..19 (19 rows).
    // ORDER BY v DESC LIMIT 3 → top 3: v=99,98,97 → ids 1,2,3.
    let r = rows(&db, "SELECT id FROM t WHERE v > 80 ORDER BY v DESC LIMIT 3");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. SUM over filtered + NULL values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_where_with_nulls_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 'a', 30)");
    exec(&db, "INSERT INTO t VALUES (4, 'b', 100)");
    // SUM(v) WHERE cat = 'a' = 10 + 30 = 40 (NULL skipped).
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'a'"), 40);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. DELETE with WHERE then verify aggregate
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_where_then_aggregate() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=20 {
        let cat = if i % 3 == 0 { "x" } else { "y" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i));
    }
    // Initial: 20 rows. cat 'x': ids 3,6,9,12,15,18 = 6 rows.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'x'"), 6);
    // Delete cat 'x'.
    exec(&db, "DELETE FROM t WHERE cat = 'x'");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 14);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'x'"), 0);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE cat = 'y'"), 14);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. UPDATE WHERE then verify aggregate reflects change
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_where_then_sum() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=10 {
        let cat = if i <= 5 { "a" } else { "b" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i * 10));
    }
    // SUM(v) WHERE cat = 'a' = 10+20+30+40+50 = 150.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'a'"), 150);
    // Double all 'a' values.
    exec(&db, "UPDATE t SET v = v * 2 WHERE cat = 'a'");
    // Now SUM = 20+40+60+80+100 = 300.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'a'"), 300);
    // cat 'b' unchanged: 60+70+80+90+100 = 400.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t WHERE cat = 'b'"), 400);
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Negative values in WHERE range
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_negative_range() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for v in -10..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", v + 11, v));
    }
    // WHERE v < 0 → 10 rows (v=-10..-1).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v < 0"), 10);
    // WHERE v >= 0 → 11 rows (v=0..10).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v >= 0"), 11);
    // WHERE v BETWEEN -5 AND 5 → 11 rows (-5..5).
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v BETWEEN -5 AND 5"), 11);
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. DISTINCT with WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_with_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 1)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 1)"); // dup
    exec(&db, "INSERT INTO t VALUES (3, 'a', 2)");
    exec(&db, "INSERT INTO t VALUES (4, 'b', 1)");
    exec(&db, "INSERT INTO t VALUES (5, 'b', 3)");
    // DISTINCT v WHERE cat = 'a' → {1, 2} = 2.
    let r = rows(&db, "SELECT DISTINCT v FROM t WHERE cat = 'a' ORDER BY v");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Aggregate on column with all same values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_all_same_values() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 42)", i));
    }
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 420);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), 42);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), 42);
    let a = scalar_f64(&db, "SELECT AVG(v) FROM t");
    assert!((a - 42.0).abs() < 0.001);
    // STDDEV of all-same = 0.
    let sd = scalar_f64(&db, "SELECT STDDEV(v) FROM t");
    assert!(sd.abs() < 0.001, "STDDEV of identical values = 0, got {}", sd);
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. GROUP BY single group (implicit aggregation)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_single_group_with_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'a', 30)");
    // GROUP BY cat WHERE v >= 20: only 'a', count=2, sum=50.
    let r = rows(&db, "SELECT cat, COUNT(*), SUM(v) FROM t WHERE v >= 20 GROUP BY cat");
    assert_eq!(r.len(), 1);
    match (&r[0][0], &r[0][1], &r[0][2]) {
        (Value::Text(c), Value::Integer(cnt), Value::Integer(s)) => {
            assert_eq!(&*c.0, "a");
            assert_eq!(*cnt, 2);
            assert_eq!(*s, 50);
        }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. WHERE with OR across different columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_or_diff_cols_then_count() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 100)");
    exec(&db, "INSERT INTO t VALUES (2, 20, 200)");
    exec(&db, "INSERT INTO t VALUES (3, 30, 100)");
    exec(&db, "INSERT INTO t VALUES (4, 10, 300)");
    // a = 10 OR b = 200 → ids 1 (a=10), 2 (b=200), 4 (a=10) = 3.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE a = 10 OR b = 200"), 3);
}
