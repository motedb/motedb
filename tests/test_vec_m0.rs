//! VEC M0 集成回归：真实段 → read_column_batch → 批 → Value 边界 roundtrip，
//! 以及 ColumnarRowSet null 修复后的 to_row_based 还原。
use motedb::sql::{Lexer, Parser, QueryExecutor};
use motedb::types::Value;
use motedb::MoteDB;
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
            &format!("INSERT INTO t VALUES ({}, {}, {}, {}, {})", i, v, b, s, ts),
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
        let batch = motedb::storage::colbatch::ColumnBatch::new_shared(cols);
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
            if i % 3 == 0 {
                Value::Null
            } else {
                Value::Integer(i)
            },
            if i % 3 == 0 {
                Value::text(format!("x{}", i))
            } else {
                Value::Null
            },
            if i % 3 == 0 {
                Value::Null
            } else {
                Value::Float(i as f64 * 0.5)
            },
            if i % 5 == 0 {
                Value::Null
            } else {
                Value::Bool(i % 2 == 0)
            },
            if i % 7 == 0 {
                Value::Null
            } else {
                Value::Timestamp(motedb::types::Timestamp::from_micros(
                    1_700_000_000_000_000 + i,
                ))
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
            assert!(
                matches!(&r[0], Value::Integer(v) if *v == fi),
                "行{} a={:?}",
                i,
                r[0]
            );
        }
        // b (与 a 反相)
        if fi % 3 == 0 {
            assert!(
                matches!(&r[1], Value::Text(t) if t.len() == 2 + i.to_string().len() - i.to_string().len() || !t.is_empty()),
                "行{} b={:?}",
                i,
                r[1]
            );
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

/// VEC M1: 批聚合差分回归（含 NULL/三值逻辑/BETWEEN/OR/NOT）。
#[test]
fn vec_no_group_aggregate_matches_expectations() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m1.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP, dev TEXT, val REAL, qty INT)",
    );
    for i in 1..=400i64 {
        let (v, q) = if i % 7 == 0 {
            ("NULL".into(), "NULL".into())
        } else {
            (
                format!("{:.3}", i as f64 * 1.5 - 300.0),
                (i % 11 - 5).to_string(),
            )
        };
        ex(
            &db,
            &format!(
                "INSERT INTO t VALUES ({}, {}, 'dev-{:03}', {}, {})",
                i,
                1700000000000000 + i * 1000,
                i % 8,
                v,
                q
            ),
        );
    }
    db.checkpoint().unwrap();

    // 手工期望
    let mut cnt = 0i64;
    let mut nn = 0i64;
    let mut fsum = 0.0f64;
    let mut mn = f64::INFINITY;
    let mut mx = f64::NEG_INFINITY;
    let mut qsum = 0i64;
    for i in 1..=400i64 {
        cnt += 1;
        if i % 7 != 0 {
            let v = i as f64 * 1.5 - 300.0;
            nn += 1;
            fsum += v;
            mn = mn.min(v);
            mx = mx.max(v);
            qsum += i % 11 - 5;
        }
    }
    let r = rows(
        &db,
        "SELECT COUNT(*), COUNT(val), SUM(val), MIN(val), MAX(val), SUM(qty) FROM t",
    );
    assert_eq!(
        r[0],
        vec![
            Value::Integer(cnt),
            Value::Integer(nn),
            Value::Float((fsum * 1000.0).round() / 1000.0),
            Value::Float(mn),
            Value::Float(mx),
            Value::Integer(qsum),
        ]
    );

    // WHERE + BETWEEN + OR/NOT 三值
    let r = rows(
        &db,
        "SELECT COUNT(*), AVG(val) FROM t WHERE id BETWEEN 50 AND 350 AND qty > 0",
    );
    let mut w = 0i64;
    let mut wsum = 0.0;
    let mut wnn = 0i64;
    for i in 50..=400 {
        if i <= 350 && i % 7 != 0 && (i % 11 - 5) > 0 {
            w += 1;
            wnn += 1;
            wsum += i as f64 * 1.5 - 300.0;
        }
    }
    assert_eq!(r[0][0], Value::Integer(w));
    match &r[0][1] {
        Value::Float(f) => assert!((f - wsum / wnn as f64).abs() < 1e-9),
        o => panic!("{:?}", o),
    }

    let r = rows(
        &db,
        "SELECT COUNT(*) FROM t WHERE dev = 'dev-003' OR qty IS NULL",
    );
    let mut orc = 0i64;
    for i in 1..=400i64 {
        if i % 8 == 3 || i % 7 == 0 {
            orc += 1;
        }
    }
    assert_eq!(r[0][0], Value::Integer(orc));

    // NOT(val > 0): NULL 行是 UNKNOWN 被排除 — 只有非 NULL 且 <= 0 计入
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE NOT (val > 0)");
    let mut nc = 0i64;
    for i in 1..=400i64 {
        if i % 7 != 0 && (i as f64 * 1.5 - 300.0) <= 0.0 {
            nc += 1;
        }
    }
    assert_eq!(r[0][0], Value::Integer(nc), "NOT+NULL 三值语义");
}

/// fuzz seed 14 挖出的 OFFSET 点查 bug 回归。
#[test]
fn pk_point_query_respects_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("off.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT, val REAL, qty INT)",
    );
    for i in 1..=20i64 {
        ex(
            &db,
            &format!(
                "INSERT INTO items VALUES ({}, 'c{}', {}, {})",
                i,
                i % 3,
                i,
                i
            ),
        );
    }
    let r = rows(
        &db,
        "SELECT qty FROM items WHERE id = 1 ORDER BY val DESC LIMIT 13 OFFSET 6",
    );
    assert!(r.is_empty(), "OFFSET 跳过唯一匹配行 → 0 行, got {:?}", r);
    let r = rows(&db, "SELECT qty FROM items WHERE id = 1 LIMIT 5 OFFSET 3");
    assert!(r.is_empty());
    let r = rows(&db, "SELECT qty FROM items WHERE id = 1 LIMIT 5");
    assert_eq!(r.len(), 1);
}

/// VEC M2: 批 GROUP BY 差分回归（表达式键/别名/双键/WHERE/序号 ORDER BY/LIMIT）。
#[test]
fn vec_group_by_matches_expectations() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m2.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE t (id INT PRIMARY KEY, val REAL, qty INT, note TEXT)",
    );
    for i in 1..=400i64 {
        let (v, q) = if i % 7 == 0 {
            ("NULL".into(), "NULL".into())
        } else {
            (
                format!("{:.3}", i as f64 * 1.5 - 300.0),
                (i % 11 - 5).to_string(),
            )
        };
        ex(
            &db,
            &format!("INSERT INTO t VALUES ({}, {}, {}, 'n{}')", i, v, q, i % 13),
        );
    }
    db.checkpoint().unwrap();

    // 手工期望: id % 5 分组 COUNT(*) / SUM(qty) (qty 跳 NULL)
    let r = rows(
        &db,
        "SELECT id % 5, COUNT(*), SUM(qty) FROM t GROUP BY id % 5 ORDER BY 1",
    );
    let mut want: Vec<(i64, i64, i64)> = Vec::new();
    for m in 0..5i64 {
        let mut c = 0;
        let mut s = 0;
        for i in 1..=400i64 {
            if i % 5 == m {
                c += 1;
                if i % 7 != 0 {
                    s += i % 11 - 5;
                }
            }
        }
        want.push((m, c, s));
    }
    for (row, w) in r.iter().zip(want.iter()) {
        assert_eq!(row[0], Value::Integer(w.0));
        assert_eq!(row[1], Value::Integer(w.1));
        assert_eq!(row[2], Value::Integer(w.2));
    }
    assert_eq!(r.len(), 5);

    // 别名键 + WHERE + AVG
    let r = rows(
        &db,
        "SELECT id % 4 AS k, COUNT(*), AVG(val) FROM t WHERE qty > 0 GROUP BY k ORDER BY k",
    );
    let mut want2: Vec<(i64, i64, f64)> = Vec::new();
    for m in 0..4i64 {
        let (mut c, mut sum, mut nn) = (0i64, 0.0f64, 0i64);
        for i in 1..=400i64 {
            if i % 4 == m && i % 7 != 0 && (i % 11 - 5) > 0 {
                c += 1;
                nn += 1;
                sum += i as f64 * 1.5 - 300.0;
            }
        }
        want2.push((m, c, if nn > 0 { sum / nn as f64 } else { f64::NAN }));
    }
    assert_eq!(r.len(), 4);
    for (row, w) in r.iter().zip(want2.iter()) {
        assert_eq!(row[0], Value::Integer(w.0));
        assert_eq!(row[1], Value::Integer(w.1));
        if let Value::Float(f) = &row[2] {
            assert!((f - w.2).abs() < 1e-9);
        } else {
            panic!("{:?}", row);
        }
    }

    // LIMIT + ORDER BY 聚合值
    let r = rows(
        &db,
        "SELECT id % 9, COUNT(*) FROM t GROUP BY id % 9 ORDER BY 2 DESC, 1 ASC LIMIT 3",
    );
    assert_eq!(r.len(), 3);
}

/// VEC M3: 批 hash equi-JOIN + GROUP BY 差分回归。
#[test]
fn vec_equi_join_group_by_matches_expectations() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("m3.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE ev (id INT PRIMARY KEY, dev TEXT, val REAL, qty INT)",
    );
    ex(&db, "CREATE TABLE sen (dev TEXT PRIMARY KEY, zone INT)");
    for i in 1..=300i64 {
        let (v, q) = if i % 7 == 0 {
            ("NULL".into(), "NULL".into())
        } else {
            (format!("{:.3}", i as f64 * 0.5), (i % 11 - 5).to_string())
        };
        ex(
            &db,
            &format!(
                "INSERT INTO ev VALUES ({}, 'dev-{:02}', {}, {})",
                i,
                i % 8,
                v,
                q
            ),
        );
    }
    for d in 0..8i64 {
        ex(
            &db,
            &format!("INSERT INTO sen VALUES ('dev-{:02}', {})", d, d % 3),
        );
    }
    db.checkpoint().unwrap();

    // 手工期望: zone=1 的 sen 设备 (dev-01, dev-04, dev-07) 与 ev 连接计数
    let r = rows(
        &db,
        "SELECT e.dev, COUNT(*) FROM ev e JOIN sen s ON e.dev = s.dev \
         WHERE s.zone = 1 GROUP BY e.dev ORDER BY e.dev",
    );
    let mut want: Vec<(String, i64)> = Vec::new();
    for d in [1i64, 4, 7] {
        let mut c = 0;
        for i in 1..=300i64 {
            if i % 8 == d {
                c += 1;
            }
        }
        want.push((format!("dev-{:02}", d), c));
    }
    assert_eq!(r.len(), want.len());
    for (row, w) in r.iter().zip(want.iter()) {
        assert_eq!(row[0], Value::text(w.0.clone()));
        assert_eq!(row[1], Value::Integer(w.1));
    }

    // 组键=join 键 + AVG (NULL 语义)
    let r = rows(
        &db,
        "SELECT e.dev, COUNT(*), AVG(e.val) FROM ev e JOIN sen s ON e.dev = s.dev \
         GROUP BY e.dev ORDER BY e.dev",
    );
    assert_eq!(r.len(), 8);
    for (d, row) in r.iter().enumerate() {
        let dd = d as i64;
        let mut nn = 0;
        let mut sum = 0.0;
        for i in 1..=300i64 {
            if i % 8 == dd && i % 7 != 0 {
                nn += 1;
                sum += i as f64 * 0.5;
            }
        }
        assert_eq!(row[0], Value::text(format!("dev-{:02}", dd)));
        match &row[2] {
            Value::Float(f) => assert!((f - sum / nn as f64).abs() < 1e-9),
            o => panic!("{:?}", o),
        }
    }

    // 整型 join 键 + 表达式组键 + 双侧 WHERE
    ex(
        &db,
        "CREATE TABLE ev2 (id INT PRIMARY KEY, dev_id INT, v REAL)",
    );
    ex(&db, "CREATE TABLE sen2 (dev_id INT PRIMARY KEY, zone INT)");
    for i in 1..=200i64 {
        ex(
            &db,
            &format!("INSERT INTO ev2 VALUES ({}, {}, {})", i, i % 8, i as f64),
        );
    }
    for d in 0..8i64 {
        ex(&db, &format!("INSERT INTO sen2 VALUES ({}, {})", d, d % 3));
    }
    db.checkpoint().unwrap();
    let r = rows(
        &db,
        "SELECT e.id % 4, COUNT(*) FROM ev2 e JOIN sen2 s ON e.dev_id = s.dev_id \
         WHERE s.zone < 2 AND e.id > 50 GROUP BY e.id % 4 ORDER BY 1",
    );
    let mut want2: Vec<(i64, i64)> = Vec::new();
    for m in 0..4i64 {
        let mut c = 0;
        for i in 51..=200i64 {
            if i % 4 == m && (i % 8) % 3 < 2 {
                c += 1;
            }
        }
        want2.push((m, c));
    }
    assert_eq!(r.len(), 4);
    for (row, w) in r.iter().zip(want2.iter()) {
        assert_eq!(row[0], Value::Integer(w.0));
        assert_eq!(row[1], Value::Integer(w.1));
    }
}
