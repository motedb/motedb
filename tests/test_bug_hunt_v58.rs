//! Bug Hunt v58 — fifth round: coercion, ORDER BY edges, string funcs, self-join.

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

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY edge cases
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_by_ordinal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY 2");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(2), Value::Integer(10)],
            vec![Value::Integer(3), Value::Integer(20)],
            vec![Value::Integer(1), Value::Integer(30)]
        ]
    );
}

#[test]
fn test_order_by_expression_projected() {
    // ORDER BY a computed column that IS in the SELECT list works.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,1),(2,1,10),(3,5,5)")
        .unwrap();
    // sums: id1=11, id2=11, id3=10. ORDER BY s ASC, id ASC → 3 (10), 1 (11), 2 (11).
    let r = q(&db, "SELECT id, a + b AS s FROM t ORDER BY s, id");
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![3, 1, 2]
    );
}

#[test]
fn test_order_by_alias() {
    // ORDER BY a column alias defined in the SELECT list.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    let r = q(&db, "SELECT id, v * 2 AS dbl FROM t ORDER BY dbl");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(40)],
            vec![Value::Integer(1), Value::Integer(60)],
        ]
    );
}

#[test]
fn test_order_by_desc_nulls() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,20)")
        .unwrap();
    // Document NULL placement. Most paths: ASC → NULLs first, DESC → NULLs last.
    let asc = q(&db, "SELECT id FROM t ORDER BY v ASC");
    let asc_ids: Vec<i64> = asc
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => -1,
        })
        .collect();
    let desc = q(&db, "SELECT id FROM t ORDER BY v DESC");
    let desc_ids: Vec<i64> = desc
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            _ => -1,
        })
        .collect();
    // Non-null must be sorted correctly in both.
    assert_eq!(
        asc_ids
            .iter()
            .filter(|&&x| x == 1 || x == 3)
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(
        desc_ids
            .iter()
            .filter(|&&x| x == 1 || x == 3)
            .copied()
            .collect::<Vec<_>>(),
        vec![3, 1]
    );
}

#[test]
fn test_order_by_multiple_mixed_direction() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,10),(2,1,20),(3,2,5),(4,2,15)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY g ASC, v DESC");
    // g=1: v 20,10 → ids 2,1 ; g=2: v 15,5 → ids 4,3
    assert_eq!(
        r.iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => -1,
            })
            .collect::<Vec<_>>(),
        vec![2, 1, 4, 3]
    );
}

#[test]
fn test_order_by_text_lexical() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'banana'),(2,'apple'),(3,'cherry')")
        .unwrap();
    let r = q(&db, "SELECT s FROM t ORDER BY s");
    assert_eq!(
        r,
        vec![
            vec![Value::text("apple".into())],
            vec![Value::text("banana".into())],
            vec![Value::text("cherry".into())],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// String functions edge cases & NULL propagation
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_length_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT LENGTH(NULL)");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_upper_lower_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT UPPER(NULL), LOWER(NULL)");
    assert_eq!(r, vec![vec![Value::Null, Value::Null]]);
}

#[test]
fn test_substr_beyond_length() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('hello', 10, 3)");
    assert_eq!(r, vec![vec![Value::text("".into())]]);
}

#[test]
fn test_substr_negative_start() {
    let (db, _d) = db();
    // Behavior varies; SQLite: negative start counts from end. Document it.
    let res = db.execute("SELECT SUBSTR('hello', -2)");
    match res {
        Ok(r) => {
            let got = rows(r.materialize().unwrap());
            assert_eq!(got.len(), 1);
        }
        Err(_) => { /* unsupported negative start: acceptable */ }
    }
}

#[test]
fn test_replace_basic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REPLACE('hello world', 'world', 'sql')");
    assert_eq!(r, vec![vec![Value::text("hello sql".into())]]);
}

#[test]
fn test_replace_null_propagates() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REPLACE(NULL, 'a', 'b')");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_trim_ltrim_rtrim() {
    let (db, _d) = db();
    let r = q(&db, "SELECT TRIM('  x  '), LTRIM('  x  '), RTRIM('  x  ')");
    assert_eq!(
        r,
        vec![vec![
            Value::text("x".into()),
            Value::text("x  ".into()),
            Value::text("  x".into()),
        ]]
    );
}

#[test]
fn test_concat_concat_op_bool_consistency() {
    // Probe: does `b || 'x'` and CONCAT(b, 'x') stringify BOOLEAN the same way?
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, b BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, TRUE)").unwrap();
    let pipe = q(&db, "SELECT b || 'x' FROM t WHERE id = 1");
    let concat = q(&db, "SELECT CONCAT(b, 'x') FROM t WHERE id = 1");
    assert_eq!(
        pipe, concat,
        "|| and CONCAT must stringify BOOLEAN the same way; ||={:?} CONCAT={:?}",
        pipe, concat
    );
}

#[test]
fn test_concat_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CONCAT('n=', 42)");
    assert_eq!(r, vec![vec![Value::text("n=42".into())]]);
}

#[test]
fn test_reverse() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REVERSE('abc')");
    assert_eq!(r, vec![vec![Value::text("cba".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Type coercion in INSERT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_float_into_int_coerces() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    // 3.0 → 3 (whole-number float into INT).
    let res = db.execute("INSERT INTO t VALUES (1, 3.0)");
    match res {
        Ok(_) => {
            let r = q(&db, "SELECT v FROM t WHERE id = 1");
            assert_eq!(r, vec![vec![Value::Integer(3)]]);
        }
        Err(_) => { /* strict: acceptable */ }
    }
}

#[test]
fn test_insert_int_into_float_coerces() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Float(42.0)]]);
}

#[test]
fn test_insert_bool_into_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let res = db.execute("INSERT INTO t VALUES (1, TRUE)");
    match res {
        Ok(_) => {
            let r = q(&db, "SELECT v FROM t WHERE id = 1");
            assert_eq!(r, vec![vec![Value::Integer(1)]]);
        }
        Err(_) => { /* strict: acceptable */ }
    }
}

#[test]
fn test_insert_into_subset_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t (id, a) VALUES (1, 10)").unwrap();
    let r = q(&db, "SELECT id, a, b FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1), Value::Integer(10), Value::Null]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Self-join
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_self_join_with_aliases() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, parent INT, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,NULL,'root'),(2,1,'child1'),(3,1,'child2')")
        .unwrap();
    let r = q(
        &db,
        "SELECT c.name, p.name FROM t c JOIN t p ON c.parent = p.id ORDER BY c.name",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("child1".into()), Value::text("root".into())],
            vec![Value::text("child2".into()), Value::text("root".into())],
        ]
    );
}

#[test]
fn test_three_table_join() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, bid INT)")
        .unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, cid INT)")
        .unwrap();
    db.execute("CREATE TABLE c(id INT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO b VALUES (10,100),(20,200)")
        .unwrap();
    db.execute("INSERT INTO c VALUES (100,'x'),(200,'y')")
        .unwrap();
    let r = q(
        &db,
        "SELECT a.id, c.val FROM a JOIN b ON a.bid = b.id JOIN c ON b.cid = c.id ORDER BY a.id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::text("x".into())],
            vec![Value::Integer(2), Value::text("y".into())]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// LEFT JOIN NULL fill
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_left_join_null_fill() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE s(tid INT, extra TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO s VALUES (1,'a')").unwrap();
    let r = q(
        &db,
        "SELECT t.id, s.extra FROM t LEFT JOIN s ON t.id = s.tid ORDER BY t.id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::text("a".into())],
            vec![Value::Integer(2), Value::Null],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY + HAVING combos
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_having_orderby() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,5),(3,100)")
        .unwrap();
    let r = q(
        &db,
        "SELECT g, SUM(v) AS s FROM t GROUP BY g HAVING SUM(v) > 10 ORDER BY s DESC",
    );
    // g=3 sum=100, g=1 sum=30; g=2 sum=10 filtered out.
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(3), Value::Integer(100)],
            vec![Value::Integer(1), Value::Integer(30)]
        ]
    );
}

#[test]
fn test_groupby_multiple_aggs() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(1,30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT g, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY g",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(1),
            Value::Integer(3),
            Value::Integer(60),
            Value::Float(20.0),
            Value::Integer(10),
            Value::Integer(30),
        ]]
    );
}

#[test]
fn test_having_without_groupby() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // HAVING COUNT(*) > 1 → one row; HAVING COUNT(*) > 5 → no rows.
    let r = q(&db, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 1");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
    let r2 = q(&db, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 5");
    assert!(r2.is_empty());
}
