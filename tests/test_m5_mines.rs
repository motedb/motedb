//! M5 雷#1 回归：共享 executor 的 bind→execute→clear params 竞态。
//! 多线程在同一 Database 上并发执行参数化查询，各线程必须看到自己的行。
//! (修复前: params 是 executor 上的共享 RwLock — 线程 A bind 后线程 B
//! bind 覆盖, A 执行时用的是 B 的参数 → 静默错行。)
use motedb::types::Value;
use motedb::Database as MoteDB;
use std::sync::Arc;

fn ex(db: &Arc<MoteDB>, sql: &str, params: Vec<motedb::types::Value>) -> motedb::QueryResult {
    db.execute_prepared(sql, params)
        .unwrap()
        .materialize()
        .unwrap()
}

#[test]
fn params_race_concurrent_parameterized_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("race.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, v INT)",
        vec![],
    );
    for i in 0..400i64 {
        ex(
            &db,
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![
                Value::Integer(i),
                Value::Text(format!("tag{}", i % 7).into()),
                Value::Integer(i * 3),
            ],
        );
    }
    db.checkpoint().unwrap();

    // 8 线程 × 各自 500 次参数化点查; 每次必须恰好返回自己的行。
    let mut handles = Vec::new();
    for t in 0..8i64 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut wrong = 0usize;
            for k in 0..500i64 {
                let id = (t * 500 + k) % 400;
                let r = ex(
                    &db,
                    "SELECT id, v FROM t WHERE id = ?",
                    vec![Value::Integer(id)],
                );
                let rows = match r {
                    motedb::QueryResult::Select { rows, .. } => rows,
                    o => panic!("expected select: {:?}", o),
                };
                let expected_v = id * 3;
                let good = rows.len() == 1
                    && rows[0][0] == Value::Integer(id)
                    && rows[0][1] == Value::Integer(expected_v);
                if !good {
                    wrong += 1;
                }
            }
            wrong
        }));
    }
    let total_wrong: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(
        total_wrong, 0,
        "{} parameterized reads returned WRONG rows",
        total_wrong
    );
}

/// 同一 Database、无参数并发读 — 不得错果（雷#2/#3 的并行安全基线）。
#[test]
fn concurrent_reads_no_params_correct() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(MoteDB::create(tmp.path().join("cread.mote")).unwrap());
    ex(
        &db,
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v REAL)",
        vec![],
    );
    for i in 0..2000i64 {
        ex(
            &db,
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![
                Value::Integer(i),
                Value::Integer(i % 13),
                Value::Float(i as f64 * 0.5),
            ],
        );
    }
    db.checkpoint().unwrap();

    let mut handles = Vec::new();
    for _ in 0..8 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut bad = 0usize;
            for _ in 0..200 {
                let r = ex(
                    &db,
                    "SELECT g, COUNT(*), SUM(v) FROM t GROUP BY g ORDER BY g",
                    vec![],
                );
                let rows = match r {
                    motedb::QueryResult::Select { rows, .. } => rows,
                    o => panic!("expected select: {:?}", o),
                };
                if rows.len() != 13 {
                    bad += 1;
                }
                // 每组计数: g 值 x∈[0,13), 行 id ≡ x (mod 13)
                for (i, row) in rows.iter().enumerate() {
                    let x = i as i64;
                    let expect_cnt = (0..2000i64).filter(|&j| j % 13 == x).count() as i64;
                    if row[1] != Value::Integer(expect_cnt) {
                        bad += 1;
                    }
                }
            }
            bad
        }));
    }
    let total_bad: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(
        total_bad, 0,
        "concurrent GROUP BY reads returned wrong results"
    );
}
