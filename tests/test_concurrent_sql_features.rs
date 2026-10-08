//! 并发覆盖: v2 新 SQL 特性(递归 CTE / 窗口 / CTE)在多线程混合负载下的
//! 稳定性 — 写 churn + 各类读 + 在线 backup 并发, 断言无错误无 panic、
//! 结束后数据一致且 doctor 无结构性问题。
//! 复审系列的非确定性 bug 属单线程; 本文件补并发维度。

use motedb::{types::Value, Database};
use tempfile::TempDir;

const ROWS: i64 = 400;

fn seed(db: &Database) {
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, g TEXT, v INT)")
        .unwrap();
    db.execute("CREATE TABLE u(id INT PRIMARY KEY, t_id INT, score FLOAT)")
        .unwrap();
    for i in 0..ROWS {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'g{}', {})",
            i,
            i % 4,
            i % 17
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO u VALUES ({}, {}, {:.2})",
            i,
            i % ROWS,
            (i % 13) as f64 / 3.0
        ))
        .unwrap();
    }
    db.execute("CREATE TEXT INDEX ti ON t(g)").unwrap();
    db.checkpoint().unwrap();
}

#[test]
fn concurrent_cte_window_write_backup() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("src.mote")).unwrap();
    seed(&db);

    let readers = 4;
    let writers = 2;
    let rounds = 60;

    std::thread::scope(|s| {
        // ── 读线程: CTE / 窗口 / 参数化点查轮转 ──
        for r in 0..readers {
            let db = &db;
            s.spawn(move || {
                for i in 0..rounds {
                    let which = (i + r) % 4;
                    let res = match which {
                        0 => db.execute(
                            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 8) SELECT SUM(n) FROM c",
                        ),
                        1 => db.execute(
                            "SELECT g, SUM(v) OVER (PARTITION BY g) FROM (SELECT g, v FROM t WHERE id < 50) AS sub ORDER BY g",
                        ),
                        2 => db.execute(
                            "WITH x AS (SELECT u.t_id, COUNT(*) AS c FROM u JOIN t ON u.t_id = t.id GROUP BY u.t_id) SELECT COUNT(*) FROM x WHERE c >= 1",
                        ),
                        _ => db.execute_prepared(
                            "SELECT v FROM t WHERE id = ?",
                            vec![Value::Integer((i * 7 + r) % ROWS)],
                        ),
                    };
                    // 读不得报错(数据在变, 但每条查询自身必须成功)
                    let result = res.and_then(|x| x.materialize());
                    result.unwrap_or_else(|e| {
                        panic!("reader#{r} round{i} query#{which} failed: {e}")
                    });
                }
            });
        }
        // ── 写线程: INSERT/UPDATE/DELETE churn ──
        for w in 0..writers {
            let db = &db;
            s.spawn(move || {
                let mut next = ROWS + w * 10_000;
                for i in 0..rounds {
                    match i % 3 {
                        0 => {
                            next += 1;
                            db.execute(&format!(
                                "INSERT INTO t VALUES ({}, 'g{}', {})",
                                next,
                                next % 4,
                                next % 17
                            ))
                            .unwrap();
                        }
                        1 => {
                            db.execute("UPDATE t SET v = v + 1 WHERE id < 10").unwrap();
                        }
                        _ => {
                            db.execute("DELETE FROM t WHERE id >= 10000").unwrap();
                        }
                    }
                }
            });
        }
        // ── backup 线程: 在线快照, 每次快照可独立打开且行数 ≥ 播种量 ──
        let db = &db;
        let root = dir.path();
        s.spawn(move || {
            for b in 0..6 {
                let dest = root.join(format!("snap{b}.mote"));
                if dest.exists() {
                    std::fs::remove_dir_all(&dest).ok();
                }
                db.backup_to(&dest).unwrap();
                let snap = match Database::open(&dest) {
                    Ok(s) => s,
                    Err(e) => {
                        let mut names = Vec::new();
                        if let Ok(rd) = std::fs::read_dir(&dest) {
                            for f in rd.flatten() {
                                names.push(f.file_name().to_string_lossy().to_string());
                            }
                        }
                        panic!("open snap{b} failed: {e}; dir has {names:?}");
                    }
                };
                let r = snap
                    .execute("SELECT COUNT(*) FROM t")
                    .unwrap()
                    .materialize()
                    .unwrap();
                if let motedb::sql::QueryResult::Select { rows, .. } = r {
                    let n = match rows[0][0] {
                        Value::Integer(n) => n,
                        _ => 0,
                    };
                    assert!(n >= ROWS, "快照行数 {n} < 播种量 {ROWS}");
                }
                drop(snap);
            }
        });
    });

    // ── 收尾: checkpoint + 重开一致性 + doctor ──
    db.checkpoint().unwrap();
    drop(db);

    let db = Database::open(dir.path().join("src.mote")).unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Select { rows, .. } = r {
        let n = match rows[0][0] {
            Value::Integer(n) => n,
            _ => 0,
        };
        // 写线程净效应不确定(INSERT/DELETE 竞争), 但行数必须有界且 ≥ 播种量
        assert!(n >= ROWS, "重开后行数 {n} < {ROWS}");
    }
    // 递归 CTE 在重开库上仍正确
    let r = db
        .execute("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 8) SELECT SUM(n) FROM c")
        .unwrap()
        .materialize()
        .unwrap();
    if let motedb::sql::QueryResult::Select { rows, .. } = r {
        assert_eq!(rows[0][0], Value::Integer(36), "1..8 求和 = 36");
    }
    let report = db.doctor();
    assert_ne!(
        report.worst(),
        motedb::database::doctor::DoctorStatus::Fail,
        "doctor 不应 FAIL: {:?}",
        report
            .checks
            .iter()
            .filter(|c| c.status == motedb::database::doctor::DoctorStatus::Fail)
            .map(|c| c.detail.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn backup_loop_alone() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("src.mote")).unwrap();
    seed(&db);
    let root = dir.path();
    for b in 0..6 {
        let dest = root.join(format!("snap{b}.mote"));
        if dest.exists() {
            std::fs::remove_dir_all(&dest).ok();
        }
        match db.backup_to(&dest) {
            Ok(_) => println!("backup {b} ok"),
            Err(e) => {
                println!("backup {b} ERR: {e}");
                break;
            }
        }
        match Database::open(&dest) {
            Ok(s) => {
                let _ = s.execute("SELECT COUNT(*) FROM t");
                drop(s);
                println!("  open ok");
            }
            Err(e) => {
                println!("  open ERR: {e}");
                break;
            }
        }
    }
}
