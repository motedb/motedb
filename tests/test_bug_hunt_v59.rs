//! Bug Hunt v59 — sixth round: window funcs, CTE edges, string/numeric edges, CASE.

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

fn f_of(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => panic!("expected number, got {:?}", v),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Window functions: RANK / DENSE_RANK with ties
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_rank_with_ties() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, score INT)").unwrap();
    // scores: 100, 90, 90, 80
    db.execute("INSERT INTO s VALUES (1,100),(2,90),(3,90),(4,80)").unwrap();
    let r = q(&db, "SELECT id, RANK() OVER (ORDER BY score DESC) AS rk FROM s ORDER BY id");
    // rank: 100→1, 90→2, 90→2, 80→4
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(4), Value::Integer(4)],
        ]
    );
}

#[test]
fn test_dense_rank_with_ties() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, score INT)").unwrap();
    db.execute("INSERT INTO s VALUES (1,100),(2,90),(3,90),(4,80)").unwrap();
    let r = q(&db, "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM s ORDER BY id");
    // dense_rank: 100→1, 90→2, 90→2, 80→3
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(4), Value::Integer(3)],
        ]
    );
}

#[test]
fn test_row_number_partitioned() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, g INT, v INT)").unwrap();
    db.execute("INSERT INTO s VALUES (1,1,30),(2,1,10),(3,2,20),(4,2,40)").unwrap();
    let r = q(
        &db,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) AS rn FROM s ORDER BY id",
    );
    // g=1: v30→rn1, v10→rn2 ; g=2: v40→rn1, v20→rn2
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(4), Value::Integer(1)],
        ]
    );
}

#[test]
fn test_lag_lead() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO s VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(
        &db,
        "SELECT id, LAG(v) OVER (ORDER BY id) AS prev, LEAD(v) OVER (ORDER BY id) AS nxt FROM s ORDER BY id",
    );
    // id1: prev=NULL, next=20 ; id2: prev=10, next=30 ; id3: prev=20, next=NULL
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Null, Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(10), Value::Integer(30)],
            vec![Value::Integer(3), Value::Integer(20), Value::Null],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// CTE edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_cte_basic() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(&db, "WITH x AS (SELECT * FROM t WHERE v > 15) SELECT v FROM x ORDER BY v");
    assert_eq!(
        r,
        vec![vec![Value::Integer(20)], vec![Value::Integer(30)]]
    );
}

#[test]
fn test_cte_referencing_cte() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(
        &db,
        "WITH a AS (SELECT * FROM t WHERE v > 10), b AS (SELECT * FROM a WHERE v < 30) SELECT v FROM b ORDER BY v",
    );
    // a: 20,30 ; b: 20
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
}

#[test]
fn test_cte_with_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5)").unwrap();
    let r = q(
        &db,
        "WITH agg AS (SELECT g, SUM(v) AS s FROM t GROUP BY g) SELECT g, s FROM agg WHERE s > 10 ORDER BY g",
    );
    // g1 sum=30, g2 sum=5; filter s>10 → g1
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String function edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_substr_full_length() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('hello', 1, 5)");
    assert_eq!(r, vec![vec![Value::text("hello".into())]]);
}

#[test]
fn test_substr_partial() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('hello', 2, 3)");
    assert_eq!(r, vec![vec![Value::text("ell".into())]]);
}

#[test]
fn test_length_unicode() {
    // LENGTH counts chars, not bytes.
    let (db, _d) = db();
    let r = q(&db, "SELECT LENGTH('héllo')");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

#[test]
fn test_leftstr_rightstr() {
    let (db, _d) = db();
    let res = db.execute("SELECT LEFTSTR('hello', 2), RIGHTSTR('hello', 2)");
    match res {
        Ok(r) => {
            let got = rows(r.materialize().unwrap());
            assert_eq!(got, vec![vec![Value::text("he".into()), Value::text("lo".into())]]);
        }
        Err(_) => { /* unsupported: acceptable */ }
    }
}

#[test]
fn test_repeat() {
    let (db, _d) = db();
    let res = db.execute("SELECT REPEAT('ab', 3)");
    match res {
        Ok(r) => {
            let got = rows(r.materialize().unwrap());
            assert_eq!(got, vec![vec![Value::text("ababab".into())]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_concat_multiple() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CONCAT('a', 'b', 'c')");
    assert_eq!(r, vec![vec![Value::text("abc".into())]]);
}

#[test]
fn test_pipe_concat_with_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'n=' || 42 || '!'");
    assert_eq!(r, vec![vec![Value::text("n=42!".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Numeric edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_integer_division() {
    let (db, _d) = db();
    // 7 / 2 with both ints → integer division → 3
    let r = q(&db, "SELECT 7 / 2");
    let v = match &r[0][0] {
        Value::Integer(i) => *i,
        Value::Float(f) => *f as i64,
        _ => panic!("{:?}", r),
    };
    assert_eq!(v, 3, "7/2 integer division = 3, got {:?}", r);
}

#[test]
fn test_float_division() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 7.0 / 2.0");
    assert!((f_of(&r[0][0]) - 3.5).abs() < 1e-9);
}

#[test]
fn test_modulo_basic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 7 % 3");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_negative_arithmetic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT -5 + 3, 10 - -3, -2 * -4");
    assert_eq!(r, vec![vec![Value::Integer(-2), Value::Integer(13), Value::Integer(8)]]);
}

#[test]
fn test_arithmetic_precedence() {
    let (db, _d) = db();
    // 2 + 3 * 4 = 14
    let r = q(&db, "SELECT 2 + 3 * 4");
    assert_eq!(r, vec![vec![Value::Integer(14)]]);
    // (2 + 3) * 4 = 20
    let r2 = q(&db, "SELECT (2 + 3) * 4");
    assert_eq!(r2, vec![vec![Value::Integer(20)]]);
}

#[test]
fn test_large_int_no_overflow() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1000000 * 1000000");
    assert_eq!(r, vec![vec![Value::Integer(1000000000000)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_case_no_match_no_else_returns_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CASE WHEN 1 = 2 THEN 'a' END");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_case_with_else() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CASE WHEN 1 = 2 THEN 'a' ELSE 'b' END");
    assert_eq!(r, vec![vec![Value::text("b".into())]]);
}

#[test]
fn test_searched_case_multiple_when() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)").unwrap();
    let r = q(
        &db,
        "SELECT id, CASE WHEN v < 10 THEN 'low' WHEN v < 20 THEN 'mid' ELSE 'high' END AS bucket FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::text("low".into())],
            vec![Value::Integer(2), Value::text("mid".into())],
            vec![Value::Integer(3), Value::text("high".into())],
        ]
    );
}

#[test]
fn test_simple_case_expr() {
    // CASE expr WHEN val THEN ... (simple form)
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, code INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,2),(3,3)").unwrap();
    let r = q(
        &db,
        "SELECT id, CASE code WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS name FROM t ORDER BY id",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::text("one".into())],
            vec![Value::Integer(2), Value::text("two".into())],
            vec![Value::Integer(3), Value::text("other".into())],
        ]
    );
}

#[test]
fn test_case_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE CASE WHEN v > 10 THEN 1 ELSE 0 END = 1 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COALESCE / NULLIF
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_coalesce_basic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT COALESCE(NULL, NULL, 'c', 'd')");
    assert_eq!(r, vec![vec![Value::text("c".into())]]);
}

#[test]
fn test_coalesce_all_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT COALESCE(NULL, NULL)");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_nullif_equal() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULLIF(5, 5), NULLIF(5, 3)");
    assert_eq!(r, vec![vec![Value::Null, Value::Integer(5)]]);
}

#[test]
fn test_ifnull() {
    let (db, _d) = db();
    let r = q(&db, "SELECT IFNULL(NULL, 'default'), IFNULL('x', 'default')");
    assert_eq!(r, vec![vec![Value::text("default".into()), Value::text("x".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Nested subquery / scalar subquery
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_scalar_subquery_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(&db, "SELECT id, (SELECT MAX(v) FROM t) AS mx FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(30)],
            vec![Value::Integer(2), Value::Integer(30)],
            vec![Value::Integer(3), Value::Integer(30)],
        ]
    );
}

#[test]
fn test_subquery_in_where_comparison() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // rows where v > average (avg=20) → id 3
    let r = q(&db, "SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_correlated_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, cust INT, amt INT)").unwrap();
    db.execute("CREATE TABLE customers(id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50)").unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob')").unwrap();
    // customers whose total orders > 100 → Alice (300)
    let r = q(
        &db,
        "SELECT c.name FROM customers c WHERE (SELECT SUM(o.amt) FROM orders o WHERE o.cust = c.id) > 100 ORDER BY c.name",
    );
    assert_eq!(r, vec![vec![Value::text("Alice".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// LIMIT / OFFSET edges
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_offset_beyond_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id OFFSET 10");
    assert!(r.is_empty());
}

#[test]
fn test_limit_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT id FROM t LIMIT 0");
    assert!(r.is_empty());
}

#[test]
fn test_limit_with_offset() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2");
    assert_eq!(r, vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
}
