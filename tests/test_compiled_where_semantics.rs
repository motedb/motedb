//! CompiledWhere predicate semantics regression tests (BUG #36/#37).
//!
//! CompiledWhere was near dead-code before round 17 wired it into the hot
//! scan paths — two latent bugs surfaced the moment real queries ran through
//! it:
//! - BUG #36: like_match backtracked without an exhaustion check —
//!   `LIKE '%x%'` against a text not containing x looped forever.
//! - BUG #37: compile_where DROPPED the NOT IN `negated` flag — NOT IN
//!   silently evaluated as IN (returned exactly the excluded rows).

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

fn ints(v: &[Vec<Value>]) -> Vec<i64> {
    v.iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("expected int, got {other:?}"),
        })
        .collect()
}

fn setup() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s TEXT)")
        .unwrap();
    for i in 0..10i64 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i}, 'item{}')", i % 3))
            .unwrap();
    }
    (db, dir)
}

#[test]
fn not_in_returns_excluded() {
    let (db, _d) = setup();
    let got = ints(&rows(
        db.execute("SELECT v FROM t WHERE v NOT IN (0, 1, 2) ORDER BY v")
            .unwrap(),
    ));
    assert_eq!(
        got,
        vec![3, 4, 5, 6, 7, 8, 9],
        "NOT IN must return the complement"
    );
}

#[test]
fn not_in_with_null_matches_nothing() {
    // SQL 三值逻辑: x NOT IN (0, NULL) → UNKNOWN for every x → 0 rows
    let (db, _d) = setup();
    let got = rows(
        db.execute("SELECT v FROM t WHERE v NOT IN (0, NULL)")
            .unwrap(),
    );
    assert!(
        got.is_empty(),
        "NOT IN with NULL in list must match nothing, got {got:?}"
    );
}

#[test]
fn in_with_null_still_matches_members() {
    // x IN (0, NULL): x=0 → true; x≠0 → UNKNOWN → false
    let (db, _d) = setup();
    let got = ints(&rows(
        db.execute("SELECT v FROM t WHERE v IN (0, NULL)").unwrap(),
    ));
    assert_eq!(got, vec![0]);
}

#[test]
fn like_non_matching_wildcard_terminates() {
    // BUG #36: '%x%' against text without x used to infinite-loop
    let (db, _d) = setup();
    let got = rows(db.execute("SELECT id FROM t WHERE s LIKE '%x%'").unwrap());
    assert!(got.is_empty(), "no item text contains x");
}

#[test]
fn like_patterns_correct() {
    let (db, _d) = setup();
    // item0/item1/item2 cycle: 'item0' matches %0 → ids {0,3,6,9}
    let got = ints(&rows(
        db.execute("SELECT id FROM t WHERE s LIKE '%0' ORDER BY id")
            .unwrap(),
    ));
    assert_eq!(got, vec![0, 3, 6, 9]);

    let got2 = ints(&rows(
        db.execute("SELECT id FROM t WHERE s LIKE 'item_' ORDER BY id")
            .unwrap(),
    ));
    assert_eq!(got2.len(), 10, "'item_' matches every row");

    let got3 = ints(&rows(
        db.execute("SELECT id FROM t WHERE s NOT LIKE '%0' ORDER BY id")
            .unwrap(),
    ));
    assert_eq!(got3, vec![1, 2, 4, 5, 7, 8], "NOT LIKE complement");
}

#[test]
fn compiled_and_or_combo() {
    let (db, _d) = setup();
    let got = ints(&rows(
        db.execute("SELECT v FROM t WHERE v NOT IN (0, 1) AND v < 4 ORDER BY v")
            .unwrap(),
    ));
    assert_eq!(got, vec![2, 3], "NOT IN ∧ range");
}

#[test]
fn not_in_zero_rows_excluded_all() {
    let (db, _d) = setup();
    let got = rows(
        db.execute("SELECT v FROM t WHERE v NOT IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
            .unwrap(),
    );
    assert!(got.is_empty(), "NOT IN full set matches nothing");
}

#[test]
fn null_like_and_not_like_both_false() {
    // NULL LIKE / NULL NOT LIKE → UNKNOWN → the row must never match
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'abc')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    let like = ints(&rows(
        db.execute("SELECT id FROM t WHERE s LIKE '%'").unwrap(),
    ));
    assert_eq!(like, vec![1], "NULL LIKE '%' excludes the NULL row");
    let not_like = ints(&rows(
        db.execute("SELECT id FROM t WHERE s NOT LIKE '%'").unwrap(),
    ));
    assert_eq!(
        not_like,
        Vec::<i64>::new(),
        "NULL NOT LIKE '%' must NOT include the NULL row"
    );
}

#[test]
fn eq_null_literal_matches_nothing() {
    // SQL: NULL = NULL → UNKNOWN → false（prepared `v = ?` 传 NULL 的场景）
    let (db, _d) = setup();
    let got = rows(db.execute("SELECT v FROM t WHERE v = NULL").unwrap());
    assert!(got.is_empty(), "v = NULL must match nothing, got {got:?}");
}
