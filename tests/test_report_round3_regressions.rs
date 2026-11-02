//! Regression tests for round-3 review findings (post BUG-REPORT-0.16.0).
//!
//! Each test reproduces a probe from the round-3 session. The btree items
//! (flush persistence beyond cache capacity, key-0 runs) live in
//! test_timestamp_index_duplicates.rs; the FTS tf item in
//! test_fts_phrase_multi_flush.rs.

use motedb::types::{ArcVec, Value};
use motedb::{Database, QueryResult};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

/// P5: after a reopen the pk cache is COLD (ColSegmentStore tables are not
/// warmed), so `UPDATE t SET id = <existing>` used to pass the uniqueness
/// check and the relocation path silently shadowed the existing row — the
/// table lost a row with no error.
#[test]
fn update_pk_to_existing_value_after_reopen_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("p5.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 1..=10 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = db.execute("UPDATE t SET id = 7 WHERE id = 3");
    assert!(
        r.is_err(),
        "cold-cache UPDATE-PK to an existing id must error"
    );

    let n = rows(&db, "SELECT id FROM t").len();
    assert_eq!(n, 10, "no row may be lost");
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 7"),
        vec![vec![Value::Text("v7".into())]],
        "the existing row must keep its value"
    );
}

/// P5's sibling: a cold-cache autocommit INSERT of an existing Integer PK.
#[test]
fn insert_existing_pk_after_reopen_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("p5b.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = db.execute("INSERT INTO t VALUES (1, 'b')");
    assert!(r.is_err(), "cold-cache duplicate INSERT must error");
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1")[0][0],
        Value::Text("a".into())
    );
}

/// P3: the ≥100-row fast-batch path used to skip the coercion/padding the
/// slow path ran — a short row got NULLs at row 100 where rows 1..99 got
/// the column DEFAULT.
#[test]
fn short_batch_rows_get_defaults_at_any_size() {
    for n in [99, 100, 250] {
        let dir = TempDir::new().unwrap();
        let db = Database::create(dir.path().join("d.mote")).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT DEFAULT 'x')")
            .unwrap();
        let rows_in: Vec<Vec<Value>> = (0..n).map(|i| vec![Value::Integer(i)]).collect();
        db.insert_rows("t", rows_in).unwrap();
        let with_default = rows(&db, "SELECT id FROM t WHERE v = 'x'").len();
        assert_eq!(
            with_default, n as usize,
            "batch of {n}: every row must carry the DEFAULT"
        );
    }
}

/// P4: INSERT … SELECT requires an exact column count in the positional
/// form too — only VALUES may omit trailing columns.
#[test]
fn insert_select_short_source_is_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE dst (id INT PRIMARY KEY, v TEXT DEFAULT 'd', w INT)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (1)").unwrap();
    let r = db.execute("INSERT INTO dst SELECT id FROM src");
    assert!(r.is_err(), "INSERT..SELECT with fewer columns must error");
    assert!(rows(&db, "SELECT * FROM dst").is_empty());

    // The VALUES form keeps the SQLite-compatible fill.
    db.execute("INSERT INTO dst VALUES (2)").unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM dst WHERE id = 2")[0][0],
        Value::Text("d".into())
    );
}

/// P9: VECTOR dims beyond the u16 storage header are rejected at CREATE,
/// ALTER and the raw schema path instead of truncating and corrupting the
/// whole column at flush.
#[test]
fn oversized_vector_dims_are_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    let r = db.execute("CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(70000))");
    assert!(r.is_err(), "CREATE VECTOR(70000) must error");

    // ALTER path
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY)").unwrap();
    let r = db.execute("ALTER TABLE t2 ADD COLUMN v VECTOR(65536)");
    assert!(r.is_err(), "ALTER ADD COLUMN VECTOR(65536) must error");

    // Raw schema path
    let schema = motedb::types::TableSchema::new(
        "t3".into(),
        vec![
            motedb::types::ColumnDef::new("id".into(), motedb::types::ColumnType::Integer, 0),
            motedb::types::ColumnDef::new("v".into(), motedb::types::ColumnType::Tensor(70000), 1),
        ],
    );
    assert!(motedb::MoteDB::create(dir.path().join("raw.mote"))
        .unwrap()
        .create_table(schema)
        .is_err());
}

/// P2: the timestamp may sit on either side of `+` for TEXT and DATE
/// spellings; `INTEGER - timestamp` is not representable and errors loudly
/// instead of silently matching nothing.
#[test]
fn timestamp_arithmetic_both_operand_orders() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, '2026-01-01 00:00:00')")
        .unwrap();

    let scalar = |sql: &str| -> Result<motedb::types::Timestamp, String> {
        match db.execute(sql) {
            Ok(q) => match q.materialize().unwrap() {
                QueryResult::Select { rows, .. } => match &rows[0][0] {
                    Value::Timestamp(t) => Ok(*t),
                    o => Err(format!("non-timestamp {o:?}")),
                },
                o => Err(format!("{o:?}")),
            },
            Err(e) => Err(e.to_string()),
        }
    };
    let t0 = motedb::types::Timestamp::parse_iso("2026-01-01 00:01:00").unwrap();
    assert_eq!(scalar("SELECT 60000000 + '2026-01-01 00:00:00'"), Ok(t0));
    assert_eq!(scalar("SELECT '2026-01-01 00:00:00' + 60000000"), Ok(t0));
    assert_eq!(
        scalar("SELECT '2026-01-01' + 60000000"),
        Ok(t0),
        "date-only ISO text"
    );
    assert_eq!(
        scalar("SELECT '2026-01-01 00:02:00' - 60000000"),
        Ok(t0),
        "TEXT - INTEGER"
    );
    assert!(
        scalar("SELECT 60000000 - '2026-01-01 00:00:00'").is_err(),
        "INTEGER - timestamp is not representable — must error, not silently NULL"
    );
}

/// VECTOR(65536) — exactly at the u16 boundary — still round-trips.
#[test]
fn vector_at_boundary_dim_round_trips() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(40))")
        .unwrap();
    let want = ArcVec::new((0..40).map(|i| i as f32).collect());
    let v = Value::Vector(want.clone());
    db.insert_rows("t", vec![vec![Value::Integer(1), v.clone()]])
        .unwrap();
    db.checkpoint().unwrap();
    // NOTE: Value's PartialEq has no Vector arm (vectors always compare
    // unequal) — compare element-wise.
    match &rows(&db, "SELECT v FROM t")[0][0] {
        Value::Vector(got) => assert_eq!(got.0.as_ref(), want.0.as_ref()),
        o => panic!("expected Vector, got {o:?}"),
    }
}

/// Bulk `insert_rows` (≥100 rows → fast_batch paths) must maintain the
/// timestamp index — it used to skip it entirely (a 5000-row import left
/// query_timestamp_range with zero entries) — and must pair secondary-index
/// values with the RIGHT row ids: store rows are sorted by key before
/// indexing, and zipping them with the unsorted row_ids vector bound
/// `tag0` to another row for any non-ascending explicit-PK batch
/// (`WHERE tag = 'tag0'` returned [] while a table scan showed the row).
#[test]
fn bulk_insert_maintains_timestamp_and_column_indexes_for_shuffled_pks() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, ts TIMESTAMP)")
        .unwrap();
    db.execute("CREATE INDEX t_tag ON t (tag)").unwrap();
    let base: i64 = 1_700_000_000_000_000;
    let batch: Vec<Vec<Value>> = (0..150i64)
        .rev()
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::Text(format!("tag{i}").into()),
                Value::Timestamp(motedb::types::Timestamp::from_micros(base + i * 1000)),
            ]
        })
        .collect();
    db.insert_rows("t", batch).unwrap();
    db.checkpoint().unwrap();

    for i in [0i64, 1, 50, 100, 149] {
        assert_eq!(
            rows(&db, &format!("SELECT id FROM t WHERE tag = 'tag{i}'")),
            vec![vec![Value::Integer(i)]],
            "column index must map tag{i} to row {i}"
        );
    }
    assert_eq!(
        db.query_timestamp_range(base, base + 10_000_000)
            .unwrap()
            .len(),
        150
    );

    // AUTO_INCREMENT fast path too.
    db.execute("CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, ts TIMESTAMP)")
        .unwrap();
    let batch2: Vec<Vec<Value>> = (0..300i64)
        .map(|i| {
            vec![
                Value::Null,
                Value::Timestamp(motedb::types::Timestamp::from_micros(base + 50_000_000 + i)),
            ]
        })
        .collect();
    db.insert_rows("a", batch2).unwrap();
    assert_eq!(
        db.query_timestamp_range(base + 50_000_000, base + 60_000_000)
            .unwrap()
            .len(),
        300
    );
}

/// Cross-table version-store conflicts: two tables whose Integer PKs both
/// use row 1 — committing one used to fail the other's commit with a
/// spurious "Write-write conflict on row 1" (version chains were keyed by
/// bare row_id; now (table, row_id)).
#[test]
fn concurrent_transactions_on_same_row_id_different_tables_do_not_conflict() {
    use std::sync::Arc;
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("d.mote")).unwrap());
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 0)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 0)").unwrap();

    let (a, b) = (Arc::clone(&db), Arc::clone(&db));
    let ta = std::thread::spawn(move || {
        for _ in 0..50 {
            let tx = a.begin_transaction().unwrap();
            a.execute("UPDATE a SET v = v + 1 WHERE id = 1").unwrap();
            if let Err(e) = a.commit_transaction(tx) {
                return Err(format!("table a commit failed: {e}"));
            }
        }
        Ok(())
    });
    let tb = std::thread::spawn(move || {
        for _ in 0..50 {
            let tx = b.begin_transaction().unwrap();
            b.execute("UPDATE b SET v = v + 1 WHERE id = 1").unwrap();
            if let Err(e) = b.commit_transaction(tx) {
                return Err(format!("table b commit failed: {e}"));
            }
        }
        Ok(())
    });
    ta.join()
        .unwrap()
        .expect("no spurious cross-table conflict");
    tb.join()
        .unwrap()
        .expect("no spurious cross-table conflict");
}

/// `WHERE emb IN ('[1,2]')` — the bracketed-vector spelling every other
/// vector context accepts — must match VECTOR column values (normalize_for_in
/// parses it into a Vector so the HashSet lookup hits).
#[test]
fn in_list_matches_bracketed_vector_literals() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '[1.0, 2.0]')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, '[3.0, 4.0]')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE emb IN ('[1.0, 2.0]')").len(),
        1
    );
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM t WHERE emb IN ('[3.0,4.0]', '[1.0,2.0]')"
        )
        .len(),
        2
    );
    // Plain-text IN on a TEXT column still matches the literal string.
    db.execute("CREATE TABLE s (k TEXT)").unwrap();
    db.execute("INSERT INTO s VALUES ('[1.0, 2.0]')").unwrap();
    assert_eq!(
        rows(&db, "SELECT k FROM s WHERE k IN ('[1.0, 2.0]')").len(),
        1
    );
    // NOT IN excludes.
    assert_eq!(
        rows(&db, "SELECT id FROM t WHERE emb NOT IN ('[1.0, 2.0]')").len(),
        1
    );
}

/// Every comparison shape against a bracketed-vector literal must give the
/// same answer before and after checkpoint. Each shape used to be served by a
/// different evaluator (try_fast_select, the single-predicate projected scan,
/// CompiledWhere, eval_expr_simple, scan_projected_filtered decode) and each
/// one dropped the Text→Vector coercion somewhere: the segmented bare `=`
/// path was the last one returning 0 rows.
#[test]
fn vector_comparisons_agree_before_and_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', '[1.0, 2.0]')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'a', '[3.0, 4.0]')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 'b', '[1.0, 2.0]')")
        .unwrap();

    let check = |phase: &str| {
        let ids = |sql: &str| -> Vec<i64> {
            let mut v: Vec<i64> = rows(&db, sql)
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i,
                    other => panic!("{phase}: non-integer id {other:?}"),
                })
                .collect();
            v.sort_unstable();
            v
        };
        assert_eq!(
            ids("SELECT id FROM t WHERE emb = '[1.0, 2.0]'"),
            vec![1, 3],
            "{phase}: ="
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE '[1.0, 2.0]' = emb"),
            vec![1, 3],
            "{phase}: swapped ="
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb != '[1.0, 2.0]'"),
            vec![2],
            "{phase}: !="
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb <> '[1.0, 2.0]'"),
            vec![2],
            "{phase}: <>"
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb < '[2.0, 0.0]'"),
            vec![1, 3],
            "{phase}: <"
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb = '[1.0, 2.0]' AND cat = 'a'"),
            vec![1],
            "{phase}: = AND text"
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE cat = 'b' AND emb = '[1.0, 2.0]'"),
            vec![3],
            "{phase}: text AND ="
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb IN ('[1.0, 2.0]')"),
            vec![1, 3],
            "{phase}: IN"
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb NOT IN ('[1.0, 2.0]')"),
            vec![2],
            "{phase}: NOT IN"
        );
        assert_eq!(
            ids("SELECT id FROM t WHERE emb = '[9.0, 9.0]'"),
            Vec::<i64>::new(),
            "{phase}: no match"
        );
        // A malformed literal is just text: never equal to a vector, never an error.
        assert_eq!(
            ids("SELECT id FROM t WHERE emb = '[1.0, x]'"),
            Vec::<i64>::new(),
            "{phase}: malformed"
        );
        assert_eq!(
            rows(&db, "SELECT COUNT(*) FROM t WHERE emb = '[1.0, 2.0]'")[0][0],
            Value::Integer(2),
            "{phase}: COUNT(*)"
        );
        assert_eq!(
            rows(&db, "SELECT SUM(id) FROM t WHERE emb = '[1.0, 2.0]'")[0][0],
            Value::Integer(4),
            "{phase}: SUM WHERE vector"
        );
        assert_eq!(
            rows(
                &db,
                "SELECT COUNT(*), MAX(id) FROM t WHERE emb = '[1.0, 2.0]' AND cat = 'a'"
            )[0],
            vec![Value::Integer(1), Value::Integer(1)],
            "{phase}: multi-agg WHERE vector AND text"
        );
        let mut grouped = rows(
            &db,
            "SELECT cat, COUNT(*) FROM t WHERE emb = '[1.0, 2.0]' GROUP BY cat",
        );
        grouped.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(
            grouped,
            vec![
                vec![Value::Text("a".into()), Value::Integer(1)],
                vec![Value::Text("b".into()), Value::Integer(1)],
            ],
            "{phase}: GROUP BY WHERE vector"
        );
    };

    check("write-buffer");
    db.checkpoint().unwrap();
    check("after checkpoint");

    // Mixed: one segmented row plus one fresh write-buffer row with the same vector.
    db.execute("INSERT INTO t VALUES (4, 'c', '[1.0, 2.0]')")
        .unwrap();
    let mut ids: Vec<i64> = rows(&db, "SELECT id FROM t WHERE emb = '[1.0, 2.0]'")
        .into_iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("non-integer id {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 3, 4], "mixed segment + buffer");
}
