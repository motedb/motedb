//! Engine-level query-equivalence tests: two SQL spellings of the same
//! predicate must return the same result set. Complements the unit-level
//! compiled-vs-native differential test by covering the whole executor
//! (parser → planner → scan paths → aggregation).

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(r: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match r.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    rows(
        db.execute(sql)
            .unwrap_or_else(|e| panic!("query failed: {sql}: {e}")),
    )
}

/// Sort result rows for order-insensitive comparison (format each Value to
/// a canonical string; avoids Value Ord quirks across types).
fn canon(v: Vec<Vec<Value>>) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = v
        .iter()
        .map(|r| {
            let mut cs: Vec<String> = r.iter().map(|x| format!("{x:?}")).collect();
            cs.sort();
            cs
        })
        .collect();
    out.sort();
    out
}

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT, f REAL, flag BOOLEAN)")
        .unwrap();
    let rows = [
        "INSERT INTO t VALUES (1, 10, 'apple', 1.5, TRUE)",
        "INSERT INTO t VALUES (2, 20, 'Apple', 2.5, FALSE)",
        "INSERT INTO t VALUES (3, NULL, 'banana', NULL, TRUE)",
        "INSERT INTO t VALUES (4, 10, NULL, -0.5, NULL)",
        "INSERT INTO t VALUES (5, 30, 'cherry', 0.0, FALSE)",
        "INSERT INTO t VALUES (6, 20, 'date', 3.5, TRUE)",
    ];
    for r in rows {
        db.execute(r).unwrap();
    }
    (db, dir)
}

fn assert_equiv(db: &Database, a: &str, b: &str) {
    let ra = canon(q(db, a));
    let rb = canon(q(db, b));
    assert_eq!(
        ra, rb,
        "queries disagree:\n  A: {a}\n  B: {b}\n  A→{ra:?}\n  B→{rb:?}"
    );
}

#[test]
fn in_list_equals_or_chain() {
    let (db, _d) = setup();
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE v IN (10, 30)",
        "SELECT id FROM t WHERE v = 10 OR v = 30",
    );
}

#[test]
fn not_in_equals_and_chain() {
    let (db, _d) = setup();
    // v NOT IN (10,20) excludes NULLs on both spellings
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE v NOT IN (10, 20)",
        "SELECT id FROM t WHERE v <> 10 AND v <> 20",
    );
}

#[test]
fn between_equals_range() {
    let (db, _d) = setup();
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE v BETWEEN 10 AND 20",
        "SELECT id FROM t WHERE v >= 10 AND v <= 20",
    );
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE f BETWEEN 0.0 AND 2.0",
        "SELECT id FROM t WHERE f >= 0.0 AND f <= 2.0",
    );
}

#[test]
fn not_between_equals_exclusion() {
    let (db, _d) = setup();
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE v NOT BETWEEN 10 AND 20",
        "SELECT id FROM t WHERE v < 10 OR v > 20",
    );
}

#[test]
fn not_gt_equals_le_for_non_null() {
    let (db, _d) = setup();
    // NOT (v > 10) keeps NULL rows as UNKNOWN→excluded on BOTH spellings
    // (`v <= 10` also excludes NULL). Equivalent.
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE NOT v > 10",
        "SELECT id FROM t WHERE v <= 10",
    );
}

#[test]
fn like_contains_equals_contains2() {
    let (db, _d) = setup();
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE s LIKE '%an%'",
        "SELECT id FROM t WHERE s LIKE '%an%' AND s IS NOT NULL",
    );
}

#[test]
fn in_with_null_matches_or_with_null() {
    let (db, _d) = setup();
    // IN (10, NULL): members match; non-members UNKNOWN — same for the OR
    // spelling (v=10 OR v=NULL keeps only v=10).
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE v IN (10, NULL)",
        "SELECT id FROM t WHERE v = 10 OR v = NULL",
    );
}

#[test]
fn is_null_equals_eq_null_for_this_engine() {
    let (db, _d) = setup();
    // The engine treats `= NULL` as UNKNOWN (no match); IS NULL is the
    // correct form. They are NOT equivalent — this pins that IS NULL works
    // while = NULL matches nothing.
    let is_null = q(&db, "SELECT id FROM t WHERE v IS NULL");
    let eq_null = q(&db, "SELECT id FROM t WHERE v = NULL");
    assert_eq!(is_null, vec![vec![Value::Integer(3)]]);
    assert!(eq_null.is_empty(), "v = NULL must match nothing");
}

#[test]
fn aggregate_matches_manual() {
    let (db, _d) = setup();
    // SUM via GROUP BY must equal the per-group manual sums
    let grouped = q(
        &db,
        "SELECT v, COUNT(id), SUM(id) FROM t WHERE v IS NOT NULL GROUP BY v ORDER BY v",
    );
    // v=10: ids {1,4}; v=20: ids {2,6}; v=30: ids {5}
    assert_eq!(grouped.len(), 3);
    assert_eq!(grouped[0][0], Value::Integer(10));
    assert_eq!(grouped[0][1], Value::Integer(2));
    assert_eq!(grouped[0][2], Value::Integer(5)); // 1+4
    assert_eq!(grouped[1][0], Value::Integer(20));
    assert_eq!(grouped[1][2], Value::Integer(8)); // 2+6
    assert_eq!(grouped[2][0], Value::Integer(30));
    assert_eq!(grouped[2][2], Value::Integer(5)); // 5
}

#[test]
fn count_star_equals_count_col_with_nulls() {
    let (db, _d) = setup();
    // COUNT(*) counts all rows; COUNT(v) skips NULL v
    let star = q(&db, "SELECT COUNT(*) FROM t");
    let col = q(&db, "SELECT COUNT(v) FROM t");
    assert_eq!(star[0][0], Value::Integer(6));
    assert_eq!(col[0][0], Value::Integer(5));
}

#[test]
fn boolean_flag_comparisons() {
    let (db, _d) = setup();
    // flag = TRUE ≡ flag = 1 (Bool↔Int coercion on BOTH compiled & native)
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE flag = TRUE",
        "SELECT id FROM t WHERE flag = 1",
    );
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE flag = FALSE",
        "SELECT id FROM t WHERE flag = 0",
    );
    assert_equiv(
        &db,
        "SELECT id FROM t WHERE flag",
        "SELECT id FROM t WHERE flag = TRUE",
    );
}

#[test]
fn not_like_complement() {
    let (db, _d) = setup();
    // NOT LIKE is the complement over non-NULL rows; NULL excluded both ways
    let all_non_null = q(&db, "SELECT id FROM t WHERE s IS NOT NULL");
    let like = q(&db, "SELECT id FROM t WHERE s LIKE '%a%'");
    let not_like = q(&db, "SELECT id FROM t WHERE s NOT LIKE '%a%'");
    let mut union = like.clone();
    union.extend(not_like.clone());
    let mut canon_union: Vec<i64> = union
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect();
    canon_union.sort();
    let expect: Vec<i64> = all_non_null
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect::<Vec<_>>()
        .tap_sorted();
    assert_eq!(canon_union, expect, "LIKE ∪ NOT LIKE = all non-NULL rows");
}

trait TapSorted {
    fn tap_sorted(self) -> Self;
}
impl TapSorted for Vec<i64> {
    fn tap_sorted(mut self) -> Self {
        self.sort();
        self
    }
}

#[test]
fn update_where_equivalence_with_select() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 7))
            .unwrap();
    }
    // rows an UPDATE WHERE v IN (1,2) would touch == rows the SELECT finds
    let touched_select = q(&db, "SELECT id FROM t WHERE v IN (1, 2) ORDER BY id");
    db.execute("UPDATE t SET v = 99 WHERE v IN (1, 2)").unwrap();
    let touched_update = q(&db, "SELECT id FROM t WHERE v = 99 ORDER BY id");
    assert_eq!(touched_select, touched_update);
}
