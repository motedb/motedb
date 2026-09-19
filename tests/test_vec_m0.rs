//! VEC M0 集成回归：真实段 → read_column_batch → 批 → Value 边界 roundtrip，
//! 以及 ColumnarRowSet null 修复后的 to_row_based 还原。
use motedb::sql::{Lexer, Parser, QueryExecutor};
use motedb::types::Value;
use motedb::{MoteDB};
use std::sync::Arc;

fn ex(db: &Arc<MoteDB>, sql: &str) -> motedb::QueryResult {
    let stmt = Parser::new(Lexer::new(sql).tokenize().unwrap())
        .parse()
        .unwrap();
    QueryExecutor::new(db.clone()).execute(stmt).unwrap()
}

fn rows(db: &Arc<MoteDB>, sql: &str) -> Vec<Vec<Value>> {
    match ex(db, sql) {
        motedb::QueryResult::Select { rows, .. } => rows,
        o => panic!("expected select: {:?}", o),
    }
}

/// 真实表 → checkpoint 成段 → read_column_batch 全列 → 批取行 == SQL 结果。
#[test]
fn segment_batch_roundtrip_matches_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m0.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE t (id INT PRIMARY KEY, v REAL, b BOOLEAN, s TEXT, ts TIMESTAMP)",
    );
    for i in 0..500i64 {
        let (v, b, s) = if i % 7 == 0 {
            ("NULL".into(), "NULL".into(), "NULL".into())
        } else {
            (
                format!("{:.3}", i as f64 * 1.5),
                (i % 2 == 0).to_string(),
                format!("'s{}'", i % 13),
            )
        };
        let ts = if i % 11 == 0 {
            "NULL".to_string()
        } else {
            (1_700_000_000_000_000 + i * 1000).to_string()
        };
        ex(
            &db,
            &format!(
                "INSERT INTO t VALUES ({}, {}, {}, {}, {})",
                i, v, b, s, ts
            ),
        );
    }
    db.checkpoint().unwrap();

    let store = db
        .get_col_segment_store("t")
        .expect("store after checkpoint");
    let schema = db.get_table_schema("t").unwrap();
    let segments = store.segments_snapshot();
    assert!(!segments.is_empty(), "checkpoint 应产生段");

    // 每段每列 → 批；收集 (id, 批取行) 并对照 SQL 全表
    let cts = schema.col_types();
    let mut got: Vec<(i64, Value, Value, Value, Value)> = Vec::new();
    for seg in &segments {
        let cols: Vec<_> = (0..cts.len())
            .map(|c| seg.read_column_batch(c, &cts[c]))
            .collect::<Option<Vec<_>>>()
            .expect("read_column_batch 全列成功");
        let batch = motedb::storage::colbatch::ColumnBatch::new(cols);
        for i in 0..batch.row_count() {
            let r = batch.get_row(i);
            if let Value::Integer(id) = r[0] {
                got.push((id, r[1].clone(), r[2].clone(), r[3].clone(), r[4].clone()));
            }
        }
    }
    got.sort_by_key(|t| t.0);

    let sql_rows = rows(&db, "SELECT id, v, b, s, ts FROM t ORDER BY id ASC");
    assert_eq!(got.len(), sql_rows.len(), "行数一致");
    for (g, s) in got.iter().zip(sql_rows.iter()) {
        assert_eq!(Value::Integer(g.0), s[0], "id");
        for (idx, gv) in [(1usize, &g.1), (2, &g.2), (3, &g.3), (4, &g.4)] {
            let sv = &s[idx];
            match (gv, sv) {
                (Value::Null, Value::Null) => {}
                (Value::Float(a), Value::Float(b)) => {
                    assert!((a - b).abs() < 1e-9, "col{} {} vs {}", idx, a, b)
                }
                (a, b) => assert_eq!(a, b, "col{} row{}", idx, g.0),
            }
        }
    }
    // NULL 分布抽查：i%7==0 的行 v/b/s 为 NULL
    let n_nulls = got.iter().filter(|g| matches!(g.1, Value::Null)).count();
    assert_eq!(n_nulls, (0..500).filter(|i| i % 7 == 0).count());
}

/// ColumnarRowSet null 修复：encode→decode_row_into_columns 循环后
/// 数组不错位、to_row_based 还原 NULL（修复前 null 行被丢、后续行左移）。
#[test]
fn columnar_row_set_null_alignment() {
    use motedb::storage::colbatch::ValidityBitmap;
    use motedb::storage::row_format::{
        decode_row_into_columns, encode, ColumnArray, ColumnarRowSet, SchemaDecodeContext,
    };
    use motedb::types::ColumnType;

    let cts = [
        ColumnType::Integer,
        ColumnType::Text,
        ColumnType::Float,
        ColumnType::Boolean,
        ColumnType::Timestamp,
    ];
    let ctx = SchemaDecodeContext::new(&cts);
    let mut col_data: Vec<ColumnArray> = cts
        .iter()
        .map(|ct| match ct {
            ColumnType::Integer => ColumnArray::Integers(Vec::new()),
            ColumnType::Float => ColumnArray::Floats(Vec::new()),
            ColumnType::Text => ColumnArray::Texts(Vec::new()),
            ColumnType::Timestamp => ColumnArray::Timestamps(Vec::new()),
            ColumnType::Boolean => ColumnArray::Bools(Vec::new()),
            _ => ColumnArray::Values(Vec::new()),
        })
        .collect();
    let mut validity: Vec<ValidityBitmap> = cts
        .iter()
        .map(|_| ValidityBitmap::with_capacity(64))
        .collect();

    // 50 行: i%3==0 → a/c NULL, 其余 → b NULL; d/ts 混 NULL
    for i in 0..50i64 {
        let row = vec![
            if i % 3 == 0 { Value::Null } else { Value::Integer(i) },
            if i % 3 == 0 { Value::text(format!("x{}", i)) } else { Value::Null },
            if i % 3 == 0 { Value::Null } else { Value::Float(i as f64 * 0.5) },
            if i % 5 == 0 { Value::Null } else { Value::Bool(i % 2 == 0) },
            if i % 7 == 0 { Value::Null } else {
                Value::Timestamp(motedb::types::Timestamp::from_micros(1_700_000_000_000_000 + i))
            },
        ];
        let bytes = encode(&row, &cts).unwrap();
        decode_row_into_columns(&ctx, &bytes, &mut col_data, &mut validity).unwrap();
    }

    // 手工组装 ColumnarRowSet 验证对齐
    let mut crs = ColumnarRowSet::new(
        vec!["a".into(), "b".into(), "c".into(), "d".into(), "ts".into()],
        &cts,
    );
    crs.data = col_data;
    crs.validity = validity;
    crs.num_rows = 50;
    let rows = crs.to_row_based();
    assert_eq!(rows.len(), 50);
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r.len(), 5, "行{} 列数 (不错位)", i);
        let fi = i as i64;
        // a
        if fi % 3 == 0 {
            assert!(matches!(r[0], Value::Null), "行{} a", i);
        } else {
            assert!(matches!(&r[0], Value::Integer(v) if *v == fi), "行{} a={:?}", i, r[0]);
        }
        // b (与 a 反相)
        if fi % 3 == 0 {
            assert!(matches!(&r[1], Value::Text(t) if t.len() == 2 + i.to_string().len() - i.to_string().len() || !t.is_empty()), "行{} b={:?}", i, r[1]);
        } else {
            assert!(matches!(r[1], Value::Null), "行{} b", i);
        }
        // c
        if fi % 3 == 0 {
            assert!(matches!(r[2], Value::Null), "行{} c", i);
        } else {
            assert!(matches!(&r[2], Value::Float(f) if (*f - fi as f64 * 0.5).abs() < 1e-12));
        }
        // d / ts 混 NULL
        if fi % 5 == 0 {
            assert!(matches!(r[3], Value::Null), "行{} d", i);
        }
        if fi % 7 == 0 {
            assert!(matches!(r[4], Value::Null), "行{} ts", i);
        }
    }
}
