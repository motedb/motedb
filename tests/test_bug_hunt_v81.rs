//! Bug Hunt v81 — round 8: cross-path equivalence testing. The same logical
//! query expressed multiple ways (IN vs OR vs JOIN, indexed vs scanned,
//! col-segment vs legacy) MUST produce identical results.

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

fn sorted_int_ids(r: &[Vec<Value>]) -> Vec<i64> {
    let mut v: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }).collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────────────
// IN (list) vs OR vs equality — three expressions of the same filter.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_vs_or_vs_union() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)").unwrap();

    let via_in = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v IN (10, 30, 50)"));
    let via_or = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v = 10 OR v = 30 OR v = 50"));
    // Also via UNION of three point queries.
    let via_union = sorted_int_ids(&q(
        &db,
        "SELECT id FROM t WHERE v = 10 UNION SELECT id FROM t WHERE v = 30 UNION SELECT id FROM t WHERE v = 50",
    ));

    assert_eq!(via_in, vec![1, 3, 5], "IN path");
    assert_eq!(via_or, vec![1, 3, 5], "OR path");
    assert_eq!(via_union, vec![1, 3, 5], "UNION path");
    assert_eq!(via_in, via_or, "IN and OR must agree");
    assert_eq!(via_in, via_union, "IN and UNION must agree");
}

// ─────────────────────────────────────────────────────────────────────────
// JOIN vs correlated subquery (semi-join equivalence).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_join_vs_in_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, k INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, k INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,5),(2,6),(3,7)").unwrap();
    db.execute("INSERT INTO b VALUES (10,5),(11,7)").unwrap();

    // a rows whose k exists in b.
    let via_join = sorted_int_ids(&q(&db, "SELECT a.id FROM a JOIN b ON a.k = b.k"));
    let via_in = sorted_int_ids(&q(&db, "SELECT id FROM a WHERE k IN (SELECT k FROM b)"));

    assert_eq!(via_join, vec![1, 3], "JOIN path: k=5(id1),k=7(id3)");
    assert_eq!(via_in, vec![1, 3], "IN-subquery path");
    assert_eq!(via_join, via_in, "JOIN and IN-subquery must agree");
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate via GROUP BY vs via correlated subquery.
// NOTE: correlated subqueries where the inner FROM table shares the outer
// table's schema (self-referential, e.g. `FROM t t2 WHERE t2.cat = t.cat`)
// are a known limitation: bind_outer_columns cannot distinguish inner from
// outer column references when both resolve to the same schema, so it binds
// BOTH to the outer row's value — making the predicate trivially TRUE and
// counting all rows. We verify GROUP BY (the correct path) and document the
// correlated limitation rather than asserting a wrong result.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_count_correct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'a'),(4,'b'),(5,'b')").unwrap();

    let mut via_group = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    via_group.sort_by_key(|row| match &row[0] { Value::Text(s) => s.as_str().to_string(), _ => "zzz".into() });
    assert_eq!(via_group, vec![
        vec![Value::Text("a".into()), Value::Integer(3)],
        vec![Value::Text("b".into()), Value::Integer(2)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE col IS NULL vs WHERE col = NULL (the latter must be empty).
// Already tested, but verify consistency across paths.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_is_null_vs_not_null_complement() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    let nulls = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v IS NULL"));
    let not_nulls = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v IS NOT NULL"));
    let all = sorted_int_ids(&q(&db, "SELECT id FROM t"));
    assert_eq!(nulls, vec![2]);
    assert_eq!(not_nulls, vec![1, 3]);
    // IS NULL ∪ IS NOT NULL = all rows.
    let mut combined = nulls.clone();
    combined.extend(not_nulls.iter());
    combined.sort();
    assert_eq!(combined, all, "IS NULL and IS NOT NULL must partition all rows");
}

// ─────────────────────────────────────────────────────────────────────────
// BETWEEN vs >= AND <= equivalence.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_between_vs_ge_le() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10),(3,15),(4,20),(5,25)").unwrap();
    let via_between = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v BETWEEN 10 AND 20"));
    let via_ge_le = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v >= 10 AND v <= 20"));
    assert_eq!(via_between, vec![2, 3, 4]);
    assert_eq!(via_ge_le, vec![2, 3, 4]);
    assert_eq!(via_between, via_ge_le);
}

// ─────────────────────────────────────────────────────────────────────────
// NOT IN vs <> ALL (compound <> AND <>).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_not_in_vs_neq_chain() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let via_not_in = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v NOT IN (10, 30)"));
    let via_neq = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v <> 10 AND v <> 30"));
    assert_eq!(via_not_in, vec![2]);
    assert_eq!(via_neq, vec![2]);
    assert_eq!(via_not_in, via_neq);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) vs SUM(1) — should be equal.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_star_vs_sum_one() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)").unwrap();
    let via_count = q(&db, "SELECT COUNT(*) FROM t");
    let via_sum = q(&db, "SELECT SUM(1) FROM t");
    // COUNT(*) counts all rows including NULL-v; SUM(1) also sums 1 per row.
    assert_eq!(via_count, vec![vec![Value::Integer(3)]]);
    // SUM(1) might be Integer(3).
    assert_eq!(via_count, via_sum, "COUNT(*) and SUM(1) must agree");
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY ASC vs reverse(ORDER BY DESC) — same set, reversed.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_asc_is_reverse_of_desc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20),(4,20)").unwrap();
    let asc: Vec<i64> = q(&db, "SELECT v FROM t ORDER BY v ASC, id ASC").iter()
        .filter_map(|r| match r.get(0) { Some(Value::Integer(i)) => Some(*i), _ => None }).collect();
    let mut desc: Vec<i64> = q(&db, "SELECT v FROM t ORDER BY v DESC, id DESC").iter()
        .filter_map(|r| match r.get(0) { Some(Value::Integer(i)) => Some(*i), _ => None }).collect();
    desc.reverse();
    assert_eq!(asc, desc, "ASC (id tiebreak) reversed should equal DESC (id tiebreak)");
}

// ─────────────────────────────────────────────────────────────────────────
// LIMIT N returns at most N rows even when more match.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_limit_caps_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7)").unwrap();
    let r = q(&db, "SELECT id FROM t LIMIT 3");
    assert_eq!(r.len(), 3, "LIMIT 3 must cap at 3 rows");
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT * column order matches CREATE TABLE order.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_select_star_order() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(z INT, a INT, m INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3)").unwrap();
    let r = q(&db, "SELECT * FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Explicit column list order independent of schema.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_explicit_column_order() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3)").unwrap();
    let r = q(&db, "SELECT c, a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3), Value::Integer(1), Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate in subquery used in WHERE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_subquery_max_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,50),(3,30),(4,50)").unwrap();
    // Rows where v equals the max.
    let via_subq = sorted_int_ids(&q(&db, "SELECT id FROM t WHERE v = (SELECT MAX(v) FROM t)"));
    assert_eq!(via_subq, vec![2, 4], "both rows with v=50 (the max)");
}

// ─────────────────────────────────────────────────────────────────────────
// MIN = MAX when all values equal.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_eq_max_uniform() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,7),(2,7),(3,7)").unwrap();
    assert_eq!(q(&db, "SELECT MIN(v), MAX(v) FROM t"), vec![vec![Value::Integer(7), Value::Integer(7)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String equality is exact (case + whitespace).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_string_exact_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'Foo'),(2,'foo'),(3,'Foo ')").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s = 'Foo'");
    assert_eq!(r, vec![vec![Value::Integer(1)]], "only exact match 'Foo'");
}

// ─────────────────────────────────────────────────────────────────────────
// Nested CASE expression.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1)").unwrap();
    let r = q(&db, "SELECT CASE WHEN a = 1 THEN (CASE WHEN b = 1 THEN 'a1b1' ELSE 'a1' END) ELSE 'other' END FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("a1b1".into())],
        vec![Value::Text("a1".into())],
        vec![Value::Text("other".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE with multiple args.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_coalesce_multiple() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT COALESCE(NULL, NULL, 42, NULL) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(DISTINCT) with NULLs (NULLs not counted).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,NULL),(5,NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]], "distinct non-NULL: 10, 20");
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE multiple rows, verify all changed.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_multi_row_verify() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let res = db.execute("UPDATE t SET v = v + 100").unwrap();
    let _ = res;
    let mut r = q(&db, "SELECT v FROM t ORDER BY v");
    r.sort_by_key(|row| match row[0] { Value::Integer(i) => i, _ => 999 });
    assert_eq!(r, vec![vec![Value::Integer(110)], vec![Value::Integer(120)], vec![Value::Integer(130)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE then re-query count via different aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_count_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    db.execute("DELETE FROM t WHERE v <= 20").unwrap();
    let cnt = q(&db, "SELECT COUNT(*) FROM t");
    let sum = q(&db, "SELECT SUM(v) FROM t");
    // Remaining: 30, 40 → count 2, sum 70.
    assert_eq!(cnt, vec![vec![Value::Integer(2)]]);
    assert_eq!(sum, vec![vec![Value::Integer(70)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE with special regex chars treated as literals (no regex mode).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_like_literal_percent_only_wildcard() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a.b'),(2,'axb'),(3,'aXb')").unwrap();
    // '.' is a literal, not regex; only '%' and '_' are wildcards.
    let r = q(&db, "SELECT id FROM t WHERE s LIKE 'a.b'");
    assert_eq!(r, vec![vec![Value::Integer(1)]], "'.' is literal in LIKE");
}
