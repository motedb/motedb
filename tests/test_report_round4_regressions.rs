//! Regression tests for the round-4 probes (post v0.17.0 fix rounds).
//!
//! Every shape here used to return 0 rows / affect 0 rows because one more
//! evaluator compared a bracketed-vector TEXT literal against a VECTOR value
//! with bare `==` / `partial_cmp`: the DELETE scan evaluator
//! (`eval_binary_op`), the materialized point-query filter and
//! `compile_simple_comparison` behind the generic JOIN, the comma-join slow
//! path (`eval_with_materialized`), the JOIN pushdown producers and the
//! positional `<>`.
use motedb::types::Value;
use motedb::{Database, QueryResult};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}
fn n(db: &Database, sql: &str) -> i64 {
    match &rows(db, sql)[0][0] {
        Value::Integer(i) => *i,
        other => panic!("non-int {other:?} for {sql}"),
    }
}
fn ids(db: &Database, sql: &str) -> Vec<i64> {
    let mut v: Vec<i64> = rows(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("non-int {other:?} for {sql}"),
        })
        .collect();
    v.sort_unstable();
    v
}

/// UPDATE / DELETE with a bracketed-vector predicate, in the write buffer and
/// after checkpoint. DELETE went through `ExprEvaluator::eval_binary_op`,
/// whose bare `partial_cmp` never matched (and made `<>` match every row);
/// UPDATE, on the positional evaluator, already worked.
#[test]
fn dml_with_vector_predicates() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, emb VECTOR(2))")
        .unwrap();
    for (i, c, e) in [
        (1, "a", "'[1.0, 2.0]'"),
        (2, "a", "'[3.0, 4.0]'"),
        (3, "b", "'[1.0, 2.0]'"),
        (4, "b", "'[5.0, 6.0]'"),
    ] {
        db.execute(&format!("INSERT INTO t VALUES ({i}, '{c}', {e})"))
            .unwrap();
    }

    // Write buffer.
    assert_eq!(
        db.execute("UPDATE t SET cat = 'z' WHERE emb = '[1.0, 2.0]'")
            .unwrap()
            .affected_rows(),
        2,
        "UPDATE WHERE emb = (buffer)"
    );
    assert_eq!(
        db.execute("DELETE FROM t WHERE emb = '[5.0, 6.0]'")
            .unwrap()
            .affected_rows(),
        1,
        "DELETE WHERE emb = (buffer)"
    );
    assert_eq!(ids(&db, "SELECT id FROM t"), vec![1, 2, 3]);

    // Segments.
    db.checkpoint().unwrap();
    assert_eq!(
        db.execute("DELETE FROM t WHERE emb = '[3.0, 4.0]'")
            .unwrap()
            .affected_rows(),
        1,
        "DELETE WHERE emb = (after checkpoint)"
    );
    assert_eq!(ids(&db, "SELECT id FROM t"), vec![1, 3]);
    db.execute("INSERT INTO t VALUES (9, 'q', '[3.0, 4.0]')")
        .unwrap();
    assert_eq!(
        db.execute("DELETE FROM t WHERE emb <> '[1.0, 2.0]'")
            .unwrap()
            .affected_rows(),
        1,
        "DELETE WHERE emb <> must delete only the non-matching row"
    );
    assert_eq!(ids(&db, "SELECT id FROM t"), vec![1, 3]);
    db.execute("INSERT INTO t VALUES (10, 'q', '[3.0, 4.0]')")
        .unwrap();
    assert_eq!(
        db.execute("DELETE FROM t WHERE emb NOT IN ('[1.0, 2.0]')")
            .unwrap()
            .affected_rows(),
        1,
        "DELETE WHERE emb NOT IN"
    );
    assert_eq!(
        db.execute("UPDATE t SET cat = 'y' WHERE emb = '[1.0, 2.0]' AND id = 3")
            .unwrap()
            .affected_rows(),
        1,
        "UPDATE WHERE emb = AND id ="
    );
    // A malformed literal is plain text: matches nothing, never errors.
    assert_eq!(
        db.execute("UPDATE t SET cat = 'w' WHERE emb = '[1.0, x]'")
            .unwrap()
            .affected_rows(),
        0
    );
    assert_eq!(
        db.execute("DELETE FROM t WHERE emb = '[1.0, x]'")
            .unwrap()
            .affected_rows(),
        0
    );
    assert_eq!(n(&db, "SELECT COUNT(*) FROM t"), 2);
}

/// JOIN shapes with a vector predicate on one side. Each shape takes a
/// different executor path (positional hash join, PK nested-loop join,
/// generic join + point filter, comma join + slow evaluator) and each one
/// used to drop every row (`=`) or keep every row (`<>`).
#[test]
fn join_with_vector_predicates_all_shapes() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, emb VECTOR(2), tag TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, aid INT, v INT)")
        .unwrap();
    for i in 1..=4i64 {
        db.execute(&format!(
            "INSERT INTO a VALUES ({i}, '[{i}.0, {}.0]', 't{i}')",
            i * 2
        ))
        .unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({i}, {i}, {})", i * 10))
            .unwrap();
    }
    let check = |phase: &str| {
        let eq_shapes = [
            // vector table on the left: positional hash join + pushdown
            "SELECT b.id FROM a JOIN b ON a.id = b.aid WHERE a.emb = '[2.0, 4.0]'",
            // vector table on the right, join column is its PK: generic join + point filter
            "SELECT b.id FROM b JOIN a ON a.id = b.aid WHERE a.emb = '[2.0, 4.0]'",
            "SELECT b.id FROM b JOIN a ON a.id = b.aid WHERE a.emb = '[2.0, 4.0]' AND b.v > 0",
            // comma joins: slow evaluator
            "SELECT b.id FROM b, a WHERE a.id = b.aid AND a.emb = '[2.0, 4.0]'",
            "SELECT b.id FROM b, a WHERE a.emb = '[2.0, 4.0]' AND a.id = b.aid",
            "SELECT b.id FROM a, b WHERE a.id = b.aid AND a.emb = '[2.0, 4.0]'",
        ];
        for sql in eq_shapes {
            assert_eq!(ids(&db, sql), vec![2], "{phase}: {sql}");
        }
        let ne_shapes = [
            "SELECT b.id FROM a JOIN b ON a.id = b.aid WHERE a.emb <> '[2.0, 4.0]'",
            "SELECT b.id FROM b JOIN a ON a.id = b.aid WHERE a.emb <> '[2.0, 4.0]'",
            "SELECT b.id FROM b, a WHERE a.id = b.aid AND a.emb <> '[2.0, 4.0]'",
        ];
        for sql in ne_shapes {
            assert_eq!(ids(&db, sql), vec![1, 3, 4], "{phase}: {sql}");
        }
        assert_eq!(
            rows(
                &db,
                "SELECT SUM(b.v) FROM b JOIN a ON a.id = b.aid WHERE a.emb = '[2.0, 4.0]'"
            )[0][0],
            Value::Integer(20),
            "{phase}: aggregate over join with vector predicate"
        );
        // The vector column itself survives the join intact.
        assert_eq!(
            rows(
                &db,
                "SELECT a.emb FROM b JOIN a ON a.id = b.aid WHERE b.id = 2"
            )[0][0],
            Value::Vector(motedb::types::ArcVec::new(vec![2.0, 4.0])),
            "{phase}: vector value through join"
        );
    };
    check("buffer");
    db.checkpoint().unwrap();
    check("after checkpoint");
}

/// Prepared-statement spelling: `WHERE emb = ?` / `IN (?, ?)` / `<> ?` bound
/// to bracketed strings behave like the inline literals.
#[test]
fn prepared_vector_params() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '[1.0, 2.0]')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, '[3.0, 4.0]')")
        .unwrap();
    db.checkpoint().unwrap();
    let q = |sql: &str, params: Vec<Value>| -> Vec<i64> {
        match db
            .execute_prepared(sql, params)
            .unwrap()
            .materialize()
            .unwrap()
        {
            QueryResult::Select { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i,
                    other => panic!("{other:?}"),
                })
                .collect(),
            other => panic!("unexpected: {other:?}"),
        }
    };
    assert_eq!(
        q(
            "SELECT id FROM t WHERE emb = ?",
            vec![Value::Text("[1.0, 2.0]".into())]
        ),
        vec![1]
    );
    assert_eq!(
        q(
            "SELECT id FROM t WHERE emb IN (?, ?)",
            vec![
                Value::Text("[9.0, 9.0]".into()),
                Value::Text("[1.0, 2.0]".into())
            ]
        ),
        vec![1]
    );
    assert_eq!(
        q(
            "SELECT id FROM t WHERE emb <> ?",
            vec![Value::Text("[1.0, 2.0]".into())]
        ),
        vec![2]
    );
    assert_eq!(
        db.execute_prepared(
            "DELETE FROM t WHERE emb = ?",
            vec![Value::Text("[3.0, 4.0]".into())]
        )
        .unwrap()
        .affected_rows(),
        1
    );
    assert_eq!(n(&db, "SELECT COUNT(*) FROM t"), 1);
}

/// The distance operators with a quoted vector literal outside ORDER BY:
/// projected `emb <-> '[…]'` evaluated to NULL and `WHERE emb <-> '[…]' <
/// 0.5` matched nothing on the positional evaluators (ORDER BY worked
/// because the KNN plan parses the literal itself).
#[test]
fn distance_operator_with_quoted_vector_literal() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '[1.0, 2.0]')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, '[3.0, 4.0]')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, '[1.0, 2.0]')")
        .unwrap();
    for phase in ["buffer", "after checkpoint"] {
        if phase == "after checkpoint" {
            db.checkpoint().unwrap();
        }
        let proj = rows(
            &db,
            "SELECT id, emb <-> '[1.0, 2.0]' AS d FROM t ORDER BY d, id",
        );
        assert_eq!(proj.len(), 3, "{phase}");
        assert_eq!(
            proj[0],
            vec![Value::Integer(1), Value::Float(0.0)],
            "{phase}: projected distance"
        );
        assert_eq!(
            proj[1],
            vec![Value::Integer(3), Value::Float(0.0)],
            "{phase}: projected distance"
        );
        match &proj[2][1] {
            Value::Float(d) => assert!((d - 8f64.sqrt()).abs() < 1e-5, "{phase}: {d}"),
            other => panic!("{phase}: {other:?}"),
        }
        assert_eq!(
            ids(&db, "SELECT id FROM t WHERE emb <-> '[1.0, 2.0]' < 0.5"),
            vec![1, 3],
            "{phase}: distance threshold in WHERE"
        );
        assert_eq!(
            ids(&db, "SELECT id FROM t WHERE emb <=> '[1.0, 2.0]' < 0.001"),
            vec![1, 3],
            "{phase}: cosine threshold in WHERE"
        );
        // Mirror spelling and the literal on the left.
        assert_eq!(
            ids(&db, "SELECT id FROM t WHERE '[1.0, 2.0]' <-> emb < 0.5"),
            vec![1, 3],
            "{phase}: literal on the left"
        );
    }
}
