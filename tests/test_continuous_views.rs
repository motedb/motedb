//! 🔖 B5（0.18）验收: 时序连续聚合。
//!
//! - CREATE CONTINUOUS VIEW cv AS SELECT TIME_BUCKET(...) ... GROUP BY ...
//! - 惰性刷新 v1 = 查询时全量重算（语义如实: 查询时点一致性）;
//! - 外层投影/聚合/WHERE/ORDER BY/LIMIT 在 cv 上正常求值;
//! - FIRST(v, ts)/LAST(v, ts) 2 参 TS 聚合（即时 GROUP BY + 视图内）;
//! - 差分: 随机乱序写入下 cv ≡ 等价即时 GROUP BY;
//! - CHECKPOINT + reopen 后视图定义与查询存活，watermark 持久。

use motedb::sql::QueryResult;
use motedb::Database;
use rand::SeedableRng;

/// 物化 SELECT 结果为 (columns, rows)。
fn rows(db: &Database, sql: &str) -> (Vec<String>, Vec<Vec<motedb::types::Value>>) {
    let stream = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("execute {sql:?}: {e}"));
    let mat = stream
        .materialize()
        .unwrap_or_else(|e| panic!("materialize {sql:?}: {e}"));
    match mat {
        QueryResult::Select { columns, rows } => (columns, rows),
        o => panic!("expected Select for {sql:?}, got {o:?}"),
    }
}

fn seed_sensor(db: &Database) {
    db.execute("CREATE TABLE sensor (ts TIMESTAMP, device TEXT, v FLOAT)")
        .unwrap();
    for (ts, dev, v) in [
        ("2026-11-05T10:00:00", "a", 1.0),
        ("2026-11-05T10:00:30", "a", 3.0),
        ("2026-11-05T10:01:20", "b", 5.0),
    ] {
        db.execute(&format!("INSERT INTO sensor VALUES ('{ts}', '{dev}', {v})"))
            .unwrap();
    }
}

#[test]
fn create_query_and_outer_semantics() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed_sensor(&db);

    db.execute(
        "CREATE CONTINUOUS VIEW cv AS \
         SELECT TIME_BUCKET('1m', ts) AS b, device, AVG(v) AS v_avg \
         FROM sensor GROUP BY b, device",
    )
    .unwrap();

    // 基础查询: 桶 + 分组 + AVG 精确值，外层 ORDER BY 生效。
    let (cols, got) = rows(&db, "SELECT * FROM cv ORDER BY b, device");
    assert_eq!(cols, vec!["b", "device", "v_avg"]);
    let ts = |micros: i64| {
        motedb::types::Value::Timestamp(motedb::types::Timestamp::from_micros(micros))
    };
    assert_eq!(
        got,
        vec![
            vec![
                ts(1793872800_000000),
                motedb::types::Value::text_from("a"),
                motedb::types::Value::Float(2.0)
            ],
            vec![
                ts(1793872860_000000),
                motedb::types::Value::text_from("b"),
                motedb::types::Value::Float(5.0)
            ],
        ]
    );

    // 外层聚合: COUNT(*)。
    let (_, got) = rows(&db, "SELECT COUNT(*) FROM cv");
    assert_eq!(got, vec![vec![motedb::types::Value::Integer(2)]]);

    // 外层 WHERE + ORDER BY + LIMIT。
    let (_, got) = rows(
        &db,
        "SELECT device, v_avg FROM cv WHERE device = 'a' ORDER BY v_avg DESC LIMIT 1",
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], motedb::types::Value::text_from("a"));

    // 增量写入 → 惰性刷新看到新桶。
    db.execute("INSERT INTO sensor VALUES ('2026-11-05T10:02:00', 'a', 10.0)")
        .unwrap();
    let (_, got) = rows(&db, "SELECT COUNT(*) FROM cv");
    assert_eq!(got, vec![vec![motedb::types::Value::Integer(3)]]);

    // 同一语句内两次引用 cv → 语句内快照一致（注册一次，不重复刷新）。
    let (_, got) = rows(&db, "SELECT COUNT(*) FROM cv");
    assert_eq!(got, vec![vec![motedb::types::Value::Integer(3)]]);
}

#[test]
fn first_last_semantics() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed_sensor(&db);
    db.execute("INSERT INTO sensor VALUES ('2026-11-05T10:02:00', 'a', 10.0)")
        .unwrap();

    // 即时 GROUP BY（不经过视图）: a 桶跨度 10:00–10:02。
    let (_, got) = rows(
        &db,
        "SELECT device, FIRST(v, ts) AS fv, LAST(v, ts) AS lv \
         FROM sensor GROUP BY device ORDER BY device",
    );
    assert_eq!(got.len(), 2);
    assert_eq!(
        got[0],
        vec![
            motedb::types::Value::text_from("a"),
            motedb::types::Value::Float(1.0),
            motedb::types::Value::Float(10.0),
        ]
    );
    assert_eq!(
        got[1],
        vec![
            motedb::types::Value::text_from("b"),
            motedb::types::Value::Float(5.0),
            motedb::types::Value::Float(5.0),
        ]
    );

    // 视图内: 桶内 FIRST/LAST。
    db.execute(
        "CREATE CONTINUOUS VIEW cv2 AS \
         SELECT TIME_BUCKET('1m', ts) AS b, device, FIRST(v, ts) AS fv, LAST(v, ts) AS lv \
         FROM sensor GROUP BY b, device",
    )
    .unwrap();
    let (_, got) = rows(&db, "SELECT * FROM cv2 ORDER BY b, device");
    assert_eq!(got.len(), 3);
    assert_eq!(got[0][2], motedb::types::Value::Float(1.0)); // 10:00 a first
    assert_eq!(got[0][3], motedb::types::Value::Float(3.0)); // 10:00 a last
    assert_eq!(got[2][2], motedb::types::Value::Float(10.0)); // 10:02 a

    // 参数个数错误 → 明确报错（不静默 NULL）。
    assert!(db
        .execute("SELECT FIRST(v) FROM sensor GROUP BY device")
        .is_err());
}

/// 随机乱序写入下 cv ≡ 等价即时 GROUP BY（设计验收条款）。
#[test]
fn differential_vs_adhoc_group_by() {
    let seed: u64 = std::env::var("MOTE_CV_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20261105);
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(seed);
    use rand::Rng;

    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE sensor (ts TIMESTAMP, device TEXT, v FLOAT)")
        .unwrap();

    // 多设备乱序写入: 时间戳打乱插入，跨桶乱序。
    let n = 400;
    let mut stamps: Vec<i64> = (0..n).map(|i| 1793872800 + i * 17).collect(); // ~每行17s
    let devices = ["a", "b", "c"];
    let mut data = Vec::new();
    for ts in &stamps {
        let dev = devices[rng.gen_range(0..devices.len())];
        let v = rng.gen_range(0.0..100.0);
        data.push((*ts, dev.to_string(), v));
    }
    // 打乱插入顺序（乱序到达）。
    for i in (1..data.len()).rev() {
        let j = rng.gen_range(0..=i);
        data.swap(i, j);
    }
    for (ts, dev, v) in &data {
        let secs = ts % 60;
        let mins = ts / 60;
        db.execute(&format!(
            "INSERT INTO sensor VALUES ('2026-11-05T10:{:02}:{:02}', '{dev}', {v:.4})",
            (mins % 60) % 60,
            secs
        ))
        .unwrap();
    }
    // ↑ 时间戳在 10:00–10:1x 之间展开（构造不追求真实日历跨度，只求桶分布）。

    for (view_agg, adhoc_agg) in [
        ("AVG(v)", "AVG(v)"),
        ("MIN(v)", "MIN(v)"),
        ("MAX(v)", "MAX(v)"),
        ("SUM(v)", "SUM(v)"),
        ("COUNT(v)", "COUNT(v)"),
        ("FIRST(v, ts)", "FIRST(v, ts)"),
        ("LAST(v, ts)", "LAST(v, ts)"),
    ] {
        db.execute(&format!(
            "CREATE CONTINUOUS VIEW cvd AS SELECT TIME_BUCKET('1m', ts) AS b, device, \
             {view_agg} AS agg FROM sensor GROUP BY b, device"
        ))
        .unwrap();
        let (_, from_view) = rows(&db, "SELECT * FROM cvd ORDER BY b, device");
        let (_, adhoc) = rows(
            &db,
            &format!(
                "SELECT TIME_BUCKET('1m', ts) AS b, device, {adhoc_agg} AS agg \
                 FROM sensor GROUP BY b, device ORDER BY b, device"
            ),
        );
        assert_eq!(
            from_view.len(),
            adhoc.len(),
            "{view_agg}: row count mismatch"
        );
        for (i, (r1, r2)) in from_view.iter().zip(adhoc.iter()).enumerate() {
            assert_eq!(
                r1, r2,
                "{view_agg}: row {i} mismatch\nview={r1:?}\nadhoc={r2:?}"
            );
        }
        db.execute("DROP TABLE _mote_continuous_views").unwrap();
        // 重新 CREATE 系统表由下次 CREATE CONTINUOUS VIEW 幂等补建。
    }
}

#[test]
fn persistence_across_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        seed_sensor(&db);
        db.execute(
            "CREATE CONTINUOUS VIEW cvp AS \
             SELECT TIME_BUCKET('1m', ts) AS b, device, AVG(v) AS v_avg \
             FROM sensor GROUP BY b, device",
        )
        .unwrap();
        // 触发一次刷新（watermark 记录上次刷新时点）。
        let _ = rows(&db, "SELECT COUNT(*) FROM cvp");
        let (_, wm) = rows(
            &db,
            "SELECT watermark FROM _mote_continuous_views WHERE name = 'cvp'",
        );
        assert!(
            !matches!(wm[0][0], motedb::types::Value::Null),
            "watermark should be recorded after refresh"
        );
        db.execute("CHECKPOINT").unwrap();
    }
    // reopen: 定义存活、查询结果与刷新前一致。
    let db = Database::open(dir.path()).unwrap();
    let (cols, got) = rows(&db, "SELECT * FROM cvp ORDER BY b, device");
    assert_eq!(cols, vec!["b", "device", "v_avg"]);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0][2], motedb::types::Value::Float(2.0));
    // reopen 后 watermark 仍在（持久化正确）。
    let (_, wm) = rows(
        &db,
        "SELECT watermark FROM _mote_continuous_views WHERE name = 'cvp'",
    );
    assert!(!matches!(wm[0][0], motedb::types::Value::Null));
}

#[test]
fn create_guards() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed_sensor(&db);

    // 无 TIME_BUCKET → 拒绝。
    let err = db.execute(
        "CREATE CONTINUOUS VIEW cvx AS SELECT device, AVG(v) AS a FROM sensor GROUP BY device",
    );
    assert!(err.is_err());

    // 基表不存在 → 拒绝。
    assert!(db
        .execute("CREATE CONTINUOUS VIEW cvy AS SELECT TIME_BUCKET('1m', ts) AS b FROM nosuch GROUP BY b")
        .is_err());

    // 同名 → 拒绝。
    db.execute(
        "CREATE CONTINUOUS VIEW cvz AS SELECT TIME_BUCKET('1m', ts) AS b, AVG(v) AS a FROM sensor GROUP BY b",
    )
    .unwrap();
    assert!(db
        .execute("CREATE CONTINUOUS VIEW cvz AS SELECT TIME_BUCKET('1m', ts) AS b, AVG(v) AS a FROM sensor GROUP BY b")
        .is_err());

    // 嵌套（基表是另一个连续视图）→ 拒绝（v1 防递归）。
    assert!(db
        .execute("CREATE CONTINUOUS VIEW cvn AS SELECT TIME_BUCKET('1m', b) AS b2, AVG(a) AS a2 FROM cvz GROUP BY b2")
        .is_err());

    // 事务内 → 拒绝。
    db.execute("BEGIN").unwrap();
    let err = db.execute(
        "CREATE CONTINUOUSView cvt AS SELECT TIME_BUCKET('1m', ts) AS b FROM sensor GROUP BY b",
    );
    assert!(err.is_err());
    db.execute("ROLLBACK").unwrap();
}

/// 🔓 连续视图把物化行注册进 MATERIALIZED_CTES，随之暴露并修复了三个
/// 预存在的 CTE 查询形状硬报错（COUNT(*) O(1) 计数路径 / 裸 SELECT *
/// 快路径 / 单列 ORDER BY 主键优化都对 CTE 名直查 registry）。
#[test]
fn materialized_cte_shapes_no_longer_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed_sensor(&db);
    let (_, got) = rows(
        &db,
        "WITH x AS (SELECT device, v FROM sensor) SELECT COUNT(*) FROM x",
    );
    assert_eq!(got, vec![vec![motedb::types::Value::Integer(3)]]);
    let (_, got) = rows(
        &db,
        "WITH x AS (SELECT device, v FROM sensor) SELECT * FROM x ORDER BY device, v",
    );
    assert_eq!(got.len(), 3);
    let (_, got) = rows(
        &db,
        "WITH x AS (SELECT ts, device FROM sensor) SELECT * FROM x ORDER BY ts",
    );
    assert_eq!(got.len(), 3);
}

#[test]
fn explain_shows_live_refresh_step() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed_sensor(&db);
    db.execute(
        "CREATE CONTINUOUS VIEW cve AS \
         SELECT TIME_BUCKET('1m', ts) AS b, device, AVG(v) AS v_avg \
         FROM sensor GROUP BY b, device",
    )
    .unwrap();
    // B4 实录: EXPLAIN 重放应包含 continuous_view 决策点（source=live）。
    let (_, got) = rows(&db, "EXPLAIN SELECT * FROM cve");
    let joined = format!("{got:?}");
    assert!(
        joined.contains("continuous_view"),
        "EXPLAIN must record the continuous-view refresh step: {joined}"
    );
}
