//! Bug Hunt v91 — round 18: nested subquery materialization edges, CASE at
//! aggregate boundary, HAVING with complex predicate, COUNT over expressions,
//! and write-isolation under rapid sequences.

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
// CASE expression used inside aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_case_conditional() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'a',30)")
        .unwrap();
    // SUM(CASE WHEN cat='a' THEN v ELSE 0 END) = 10+0+30 = 40.
    let r = q(
        &db,
        "SELECT SUM(CASE WHEN cat = 'a' THEN v ELSE 0 END) FROM t",
    );
    assert_eq!(r, vec![vec![Value::Integer(40)]]);
}

#[test]
fn test_count_case_condition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)")
        .unwrap();
    // COUNT rows where v > 10, via CASE producing non-NULL.
    let r = q(&db, "SELECT COUNT(CASE WHEN v > 10 THEN 1 END) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]], "v>10 → id2,id3");
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING with AND of two aggregate conditions.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_having_two_aggregates_and() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',100)")
        .unwrap();
    // HAVING SUM(v) > 20 AND COUNT(*) >= 2.
    // a: sum=30, count=2 → both true. b: sum=105, count=2 → both true.
    let mut r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > 20 AND COUNT(*) >= 2",
    );
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]]
    );
}

#[test]
fn test_having_two_aggregates_or() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)")
        .unwrap();
    // HAVING SUM(v) > 100 OR COUNT(*) > 1.
    // a: sum=30 (not >100), count=2 (>1) → true via count. b: sum=5, count=1 → false.
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > 100 OR COUNT(*) > 1",
    );
    assert_eq!(r, vec![vec![Value::Text("a".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested subquery 3 levels deep.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_three_levels() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    // id IN (SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t)))
    // Innermost MIN=10. Middle: v>10 → {20,30,40}, MIN=20. Outer: v>20 → {30,40}.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE v > (SELECT MIN(v) FROM t WHERE v > (SELECT MIN(v) FROM t)))"));
    assert_eq!(r, vec![3, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery returning multiple columns used in scalar context (should error).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_distinct_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'a',30)")
        .unwrap();
    // For each cat, count of distinct v.
    let mut r = q(&db, "SELECT cat, COUNT(DISTINCT v) FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    // a: v={10,20,30} → 3. b: v={5,15} → 2.
    assert_eq!(
        r,
        vec![
            vec![Value::Text("a".into()), Value::Integer(3)],
            vec![Value::Text("b".into()), Value::Integer(2)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over column with mixed NULL and non-NULL in GROUP BY context.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_agg_with_null_values() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',NULL),(3,'a',30),(4,'b',NULL)")
        .unwrap();
    let mut r = q(
        &db,
        "SELECT cat, SUM(v), COUNT(v), COUNT(*) FROM t GROUP BY cat",
    );
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    // a: SUM=40, COUNT(v)=2, COUNT(*)=3. b: SUM=NULL, COUNT(v)=0, COUNT(*)=1.
    assert_eq!(
        r,
        vec![
            vec![
                Value::Text("a".into()),
                Value::Integer(40),
                Value::Integer(2),
                Value::Integer(3)
            ],
            vec![
                Value::Text("b".into()),
                Value::Null,
                Value::Integer(0),
                Value::Integer(1)
            ],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE then SELECT in tight loop (write isolation).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_rapid_update_select_loop() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,0)").unwrap();
    for i in 1..=20 {
        db.execute(&format!("UPDATE t SET v = {}", i)).unwrap();
        let r = q(&db, "SELECT v FROM t");
        assert_eq!(
            r,
            vec![vec![Value::Integer(i)]],
            "iter {}: v should be {}",
            i,
            i
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE-INSERT cycle (row recycling).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_insert_cycle() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    for cycle in 0..5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", cycle, cycle * 10))
            .unwrap();
    }
    // Delete all, re-insert different values.
    db.execute("DELETE FROM t").unwrap();
    for cycle in 0..3 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, {})",
            cycle + 100,
            cycle
        ))
        .unwrap();
    }
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
    let sumr = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sumr, vec![vec![Value::Integer(3)]]); // 0+1+2
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple indexes on same table.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_indexes() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,'x'),(2,20,'y'),(3,10,'z')")
        .unwrap();
    db.execute("CREATE INDEX idx_a ON t(a)").unwrap();
    db.execute("CREATE INDEX idx_b ON t(b)").unwrap();
    db.wait_for_indexes_ready();
    // Query via first index.
    let r1 = sorted_int(&q(&db, "SELECT id FROM t WHERE a = 10"));
    assert_eq!(r1, vec![1, 3]);
    // Query via second index.
    let r2 = q(&db, "SELECT id FROM t WHERE b = 'y'");
    assert_eq!(r2, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// AND of conditions on different indexed columns.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_two_indexed_cols() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,100),(2,10,200),(3,20,100)")
        .unwrap();
    db.execute("CREATE INDEX idx_a ON t(a)").unwrap();
    db.wait_for_indexes_ready();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a = 10 AND b = 200"));
    assert_eq!(r, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Implicit aggregation (no GROUP BY) with non-aggregate column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_implicit_agg_count_only() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    // Just COUNT(*), no other column — single row result.
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with WHERE on aggregate-input column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_where_on_agg_col() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)")
        .unwrap();
    // WHERE v > 5 excludes id3 (v=5). Then GROUP BY cat.
    // a: {10,20} sum=30. b: {15} sum=15.
    let mut r = q(&db, "SELECT cat, SUM(v) FROM t WHERE v > 5 GROUP BY cat");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![
            vec![Value::Text("a".into()), Value::Integer(30)],
            vec![Value::Text("b".into()), Value::Integer(15)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// String functions chained.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_chained_string_funcs() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // UPPER(TRIM('  hello  ')) = 'HELLO'.
    let r = q(&db, "SELECT UPPER(TRIM('  hello  ')) FROM t");
    assert_eq!(r, vec![vec![Value::Text("HELLO".into())]]);
}

#[test]
fn test_length_of_upper() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // LENGTH(UPPER('abc')) = 3.
    let r = q(&db, "SELECT LENGTH(UPPER('abc')) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in SELECT that returns different value per outer row (correlated,
// different tables).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_correlated_select_list_diff_tables() {
    let (db, _d) = db();
    db.execute("CREATE TABLE dept(id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, dept_id INT, salary INT)")
        .unwrap();
    db.execute("INSERT INTO dept VALUES (1,'eng'),(2,'sales')")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (10,1,100),(11,1,200),(12,2,150)")
        .unwrap();
    // For each dept, count of employees.
    let mut r = q(
        &db,
        "SELECT name, (SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id) FROM dept",
    );
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    // eng: 2 emps. sales: 1 emp.
    assert_eq!(
        r,
        vec![
            vec![Value::Text("eng".into()), Value::Integer(2)],
            vec![Value::Text("sales".into()), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on subquery result column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_subquery_result() {
    let (db, _d) = db();
    db.execute("CREATE TABLE dept(id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, dept_id INT)")
        .unwrap();
    db.execute("INSERT INTO dept VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (10,1),(11,1),(12,1),(13,2)")
        .unwrap();
    // Order depts by emp count DESC.
    let r = q(&db, "SELECT name, (SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id) AS cnt FROM dept ORDER BY cnt DESC");
    // a:3, b:1, c:0. DESC → a, b, c.
    let names: Vec<String> = r
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Text(s) => Some(s.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        names,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE with subquery returning aggregate, compared with >=.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_subquery_ge_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    // v >= AVG(v). avg=25. → id3 (30), id4 (40).
    let r = sorted_int(&q(
        &db,
        "SELECT id FROM t WHERE v >= (SELECT AVG(v) FROM t)",
    ));
    assert_eq!(r, vec![3, 4]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) vs COUNT(1) consistency (both count all rows).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_star_vs_count_one_grouped() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',NULL),(3,'b',5)")
        .unwrap();
    let mut r1 = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    r1.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    let mut r2 = q(&db, "SELECT cat, COUNT(1) FROM t GROUP BY cat");
    r2.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(r1, r2, "COUNT(*) and COUNT(1) must agree");
    assert_eq!(
        r1,
        vec![
            vec![Value::Text("a".into()), Value::Integer(2)],
            vec![Value::Text("b".into()), Value::Integer(1)],
        ]
    );
}
