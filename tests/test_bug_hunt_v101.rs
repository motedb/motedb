//! Bug Hunt v101 — 外部测评"SQL 兼容性缺失"项落地:
//! WITH RECURSIVE + CTE 体支持 UNION + 窗口聚合函数。
//!
//! 修复前: ① 连非递归 `WITH x AS (a UNION b)` 都解析失败(Expected RParen);
//! ② WITH RECURSIVE 被显式拒绝; ③ 窗口函数只有 ROW_NUMBER/RANK/DENSE_RANK/
//! LAG/LEAD, SUM(v) OVER (...) 直接 Parse error。
//!
//! 实现: CTE 体升级为 Statement(可含 UNION 链), 非递归 CTE 以派生表内联;
//! 递归 CTE 半朴素迭代(anchor 种子 → 每轮把工作集合成平衡 UNION 子查询
//! 内联进递归步), 结果 ≤10k 行内联、超出明确报错; 窗口聚合遵循 SQL 默认帧
//! (无 ORDER BY=全分区; 有 ORDER BY=RANGE 到当前同行组), NULL 语义与裸聚合
//! 一致(SUM/AVG/MIN/MAX 跳过 NULL, COUNT(*) 计行数)。

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {:?}", other),
    }
}

fn ints(r: &[Vec<Value>], col: usize) -> Vec<i64> {
    r.iter()
        .map(|row| match row.get(col) {
            Some(Value::Integer(i)) => *i,
            other => panic!("expected INTEGER, got {:?}", other),
        })
        .collect()
}

// ─── WITH RECURSIVE ────────────────────────────────────────────────────────

#[test]
fn recursive_series_sum_and_count() {
    let (db, _d) = db();
    let r = rows(
        &db,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) SELECT SUM(n) FROM c",
    );
    assert_eq!(ints(&r, 0), vec![15]);

    let r = rows(
        &db,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n+1 FROM c WHERE n < 5) SELECT COUNT(*) FROM c",
    );
    assert_eq!(ints(&r, 0), vec![5], "UNION 去重: 1..5 共 5 行");
}

#[test]
fn recursive_join_hierarchy() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, parent INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2), (4, 2)")
        .unwrap();
    let mut r = rows(
        &db,
        "WITH RECURSIVE tree(id, depth) AS ( \
             SELECT id, 0 FROM t WHERE parent IS NULL \
             UNION ALL \
             SELECT t.id, tree.depth + 1 FROM t JOIN tree ON t.parent = tree.id \
         ) SELECT id, depth FROM tree ORDER BY id",
    );
    // 修正: ORDER BY 在合成子查询外 — 主查询排序应生效
    let ids = ints(&r, 0);
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "ORDER BY id 应保持");
    r.sort_by_key(|row| match row[0] {
        Value::Integer(i) => i,
        _ => 0,
    });
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(0)],
            vec![Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(4), Value::Integer(2)],
        ]
    );
}

#[test]
fn recursive_fibonacci() {
    let (db, _d) = db();
    let r = rows(
        &db,
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 50) SELECT a FROM fib",
    );
    assert_eq!(ints(&r, 0), vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34]);
}

#[test]
fn recursive_requires_marker_and_union_shape() {
    let (db, _d) = db();
    let e = match db.execute("WITH c AS (SELECT 1 UNION ALL SELECT n+1 FROM c) SELECT * FROM c") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("缺 RECURSIVE 标记应报错"),
    };
    assert!(e.contains("RECURSIVE"), "{}", e);

    let e = match db.execute("WITH RECURSIVE c AS (SELECT * FROM c) SELECT * FROM c") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("非 UNION 体应报错"),
    };
    assert!(e.contains("UNION"), "{}", e);

    // 锚点自引用(左半部)也拒绝
    let e = match db
        .execute("WITH RECURSIVE c AS (SELECT * FROM c UNION ALL SELECT 1) SELECT * FROM c")
    {
        Err(e) => e.to_string(),
        Ok(_) => panic!("锚点自引用应报错"),
    };
    assert!(e.contains("anchor"), "{}", e);
}

#[test]
fn recursive_empty_result() {
    let (db, _d) = db();
    let r = rows(
        &db,
        "WITH RECURSIVE c(n) AS (SELECT 1 WHERE 1 = 0 UNION ALL SELECT n+1 FROM c WHERE n < 3) SELECT COUNT(*) FROM c",
    );
    assert_eq!(ints(&r, 0), vec![0], "空锚点 → 0 行");
}

#[test]
fn recursive_5000_rows_no_stack_overflow() {
    // 修复前: 左嵌套 UNION 链 5000 深 → execute_set_op 左递归栈溢出。
    // 平衡树合成后深度 log₂N。
    let (db, _d) = db();
    let r = rows(
        &db,
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5000) SELECT COUNT(*) FROM cnt",
    );
    assert_eq!(ints(&r, 0), vec![5000]);
}

#[test]
fn recursive_result_over_10k_errors_clearly() {
    let (db, _d) = db();
    let e = match db.execute("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 20000) SELECT COUNT(*) FROM c") {
        Err(e) => e.to_string(),
        Ok(_) => panic!(">10k 结果应明确报错"),
    };
    assert!(e.contains("10000") && e.contains("c"), "{}", e);
}

#[test]
fn recursive_referenced_twice_and_in_subquery() {
    let (db, _d) = db();
    let r = rows(
        &db,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 4) \
         SELECT (SELECT MAX(n) FROM c) AS mx, (SELECT COUNT(*) FROM c) AS cnt",
    );
    assert_eq!(ints(&r, 0), vec![4]);
    assert_eq!(ints(&r, 1), vec![4]);
}

// ─── CTE 体支持 UNION ─────────────────────────────────────────────────────

#[test]
fn cte_union_body_plain() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(20),(30)").unwrap();
    let mut r = ints(
        &rows(
            &db,
            "WITH a AS (SELECT v FROM t WHERE v <= 10 UNION ALL SELECT v FROM t WHERE v >= 30) SELECT v FROM a",
        ),
        0,
    );
    r.sort();
    assert_eq!(r, vec![10, 30]);

    // UNION 去重
    let r = ints(
        &rows(
            &db,
            "WITH a AS (SELECT v FROM t UNION SELECT v FROM t) SELECT COUNT(*) FROM a",
        ),
        0,
    );
    assert_eq!(r, vec![3]);
}

// ─── 窗口聚合 ──────────────────────────────────────────────────────────────

fn sales() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    // cat=a: 10,20,20  cat=b: 5,NULL
    db.execute("INSERT INTO s VALUES (1,'a',10),(2,'a',20),(3,'a',20),(4,'b',5),(5,'b',NULL)")
        .unwrap();
    (db, dir)
}

#[test]
fn window_sum_whole_partition_and_running() {
    let (db, _d) = sales();
    // 全分区: a=50, b=5(NULL 跳过)
    let r = rows(
        &db,
        "SELECT id, SUM(v) OVER (PARTITION BY cat) FROM s ORDER BY id",
    );
    assert_eq!(ints(&r, 1), vec![50, 50, 50, 5, 5]);

    // 运行聚合(ORDER BY v): a: 10,50,50 (并列行 20,20 同取 50 — RANGE 语义)
    // b 排序后 NULL 在前: NULL 行 sum=NULL, 然后 5
    let r = rows(
        &db,
        "SELECT id, SUM(v) OVER (PARTITION BY cat ORDER BY v) FROM s ORDER BY id",
    );
    match &r[0][1] {
        Value::Integer(10) => {}
        other => panic!("id1 running sum 应为 10, got {:?}", other),
    }
    assert_eq!(
        ints(&r[1..3], 1),
        vec![50, 50],
        "并列行(20,20)同帧取 50 — RANGE UNBOUNDED PRECEDING TO CURRENT ROW"
    );
    match &r[3][1] {
        Value::Integer(5) => {}
        other => panic!("id4 running sum 应为 5, got {:?}", other),
    }
    assert!(matches!(r[4][1], Value::Null), "全 NULL 前缀 SUM=NULL");
}

#[test]
fn window_count_star_vs_column() {
    let (db, _d) = sales();
    let r = rows(
        &db,
        "SELECT COUNT(*) OVER (PARTITION BY cat), COUNT(v) OVER (PARTITION BY cat) FROM s ORDER BY id",
    );
    assert_eq!(ints(&r, 0), vec![3, 3, 3, 2, 2], "COUNT(*) 计行");
    assert_eq!(ints(&r, 1), vec![3, 3, 3, 1, 1], "COUNT(v) 跳过 NULL");
}

#[test]
fn window_avg_min_max() {
    let (db, _d) = sales();
    let r = rows(
        &db,
        "SELECT AVG(v) OVER (PARTITION BY cat), MIN(v) OVER (PARTITION BY cat), MAX(v) OVER (PARTITION BY cat) FROM s ORDER BY id",
    );
    match &r[0][0] {
        Value::Float(f) => assert!((f - 50.0 / 3.0).abs() < 1e-9),
        other => panic!("AVG a = 50/3, got {:?}", other),
    }
    assert_eq!(ints(&r, 1)[..3], [10, 10, 10], "MIN a=10");
    assert_eq!(ints(&r, 2)[..3], [20, 20, 20], "MAX a=20");
    assert_eq!(ints(&r, 1)[3..], [5, 5], "MIN b=5");
    assert_eq!(ints(&r, 2)[3..], [5, 5], "MAX b=5");
    match &r[3][0] {
        Value::Float(f) => assert!((f - 5.0).abs() < 1e-9),
        other => panic!("AVG b = 5, got {:?}", other),
    }
}

#[test]
fn window_first_last_value_and_order_by_unprojected() {
    let (db, _d) = sales();
    // ORDER BY id 未投影 — 修复前排序退化为按输出第 0 列
    let r = rows(
        &db,
        "SELECT v, FIRST_VALUE(v) OVER (PARTITION BY cat ORDER BY id), LAST_VALUE(v) OVER (PARTITION BY cat ORDER BY id) FROM s ORDER BY id",
    );
    let vcol: Vec<Option<i64>> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            Value::Null => None,
            ref other => panic!("unexpected v: {:?}", other),
        })
        .collect();
    assert_eq!(
        vcol,
        vec![Some(10), Some(20), Some(20), Some(5), None],
        "行序 = id 序"
    );
    assert_eq!(ints(&r, 1), vec![10, 10, 10, 5, 5]);
    let lv_a: Vec<i64> = r[..3]
        .iter()
        .map(|row| match row[2] {
            Value::Integer(i) => i,
            ref other => panic!("unexpected lv: {:?}", other),
        })
        .collect();
    // 🔑 标准默认帧(RANGE UNBOUNDED PRECEDING..CURRENT ROW):
    // 每行的 LAST_VALUE = 当前行所在同行组的末尾值 — id1→10, id2/3→20
    // (v1 返回分区末行的值, 属非标准行为)
    assert_eq!(lv_a, vec![10, 20, 20], "LAST_VALUE 默认帧语义");
}

#[test]
fn window_running_count() {
    let (db, _d) = sales();
    let r = rows(
        &db,
        "SELECT id, COUNT(*) OVER (PARTITION BY cat ORDER BY id) FROM s ORDER BY id",
    );
    assert_eq!(ints(&r, 1), vec![1, 2, 3, 1, 2]);
}

// ─── 复审补测: 对抗形状 ────────────────────────────────────────────────────

#[test]
fn cte_union_body_with_column_aliases() {
    // 修复前: "explicit column aliases on a UNION body are not supported"
    let (db, _d) = db();
    db.execute("CREATE TABLE t(v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(20)").unwrap();
    let mut r = ints(
        &rows(
            &db,
            "WITH x(a) AS (SELECT v FROM t UNION ALL SELECT v + 100 FROM t) SELECT a FROM x",
        ),
        0,
    );
    r.sort();
    assert_eq!(r, vec![10, 20, 110, 120]);
}

#[test]
fn window_over_cte_and_subquery() {
    // 修复前: "Window query needs FROM table" —— 窗口函数无法作用于
    // CTE / 派生表(内联后 FROM 是 Subquery)。
    let (db, _d) = sales();
    let r = rows(
        &db,
        "WITH x AS (SELECT cat, v FROM s) SELECT cat, SUM(v) OVER (PARTITION BY cat) FROM x ORDER BY cat, v",
    );
    // a: 10,20,20 → 50 | b: 5,NULL → 5
    let sums: Vec<i64> = r
        .iter()
        .filter_map(|row| match &row[1] {
            Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert_eq!(sums, vec![50, 50, 50, 5, 5]);

    let r = rows(&db, "SELECT COUNT(*) OVER () FROM (SELECT v FROM s) AS sub");
    assert_eq!(ints(&r, 0), vec![5; 5]);

    // 递归 CTE 上的窗口
    let r = rows(
        &db,
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 4) SELECT SUM(n) OVER () FROM r",
    );
    assert_eq!(ints(&r, 0), vec![10; 4]);
}

#[test]
fn cte_name_shadows_real_table() {
    let (db, _d) = sales();
    // CTE 与真表同名 — SQL 作用域: CTE 遮蔽真表
    let r = rows(
        &db,
        "WITH s AS (SELECT 99 AS v) SELECT (SELECT SUM(v) FROM s) FROM s",
    );
    assert_eq!(ints(&r, 0), vec![99]);
}

#[test]
fn cte_inside_transaction_read_your_writes() {
    let (db, _d) = sales();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO s VALUES (9, 'z', 100)").unwrap();
    let r = rows(&db, "WITH x AS (SELECT v FROM s) SELECT COUNT(*) FROM x");
    assert_eq!(ints(&r, 0), vec![6], "事务内 CTE 应见未提交行");
    db.execute("ROLLback").unwrap();
    let r = rows(&db, "WITH x AS (SELECT v FROM s) SELECT COUNT(*) FROM x");
    assert_eq!(ints(&r, 0), vec![5], "回滚后只见已提交行");
}

// ─── 复审#2 补测: 组合形状 ─────────────────────────────────────────────────

#[test]
fn window_nested_in_expression() {
    // v1: 窗口嵌在表达式里静默求值为 NULL
    let (db, _d) = sales();
    let r = rows(&db, "SELECT SUM(v) OVER () + 1 FROM s LIMIT 1");
    // 10+20+20+5(NULL 跳过) = 55? 不: sales 表 v = 10,20,20,5,NULL → 55
    match &r[0][0] {
        Value::Integer(i) => assert_eq!(*i, 56, "55 + 1"),
        other => panic!("got {:?}", other),
    }
}

#[test]
fn window_with_group_by() {
    // v1: 直接报 "Non-aggregate expressions ... must be in GROUP BY"
    let (db, _d) = sales();
    let r = rows(
        &db,
        "SELECT cat, SUM(v) AS s, ROW_NUMBER() OVER (ORDER BY SUM(v) DESC) FROM s GROUP BY cat",
    );
    // 值绑定必须确定: a→(30, rn=1), b→(5, rn=2)(行序不保证, 按值断言)
    let mut by_g: std::collections::HashMap<String, (i64, i64)> = std::collections::HashMap::new();
    for row in &r {
        let g = match &row[0] {
            Value::Text(t) => t.to_string(),
            o => panic!("{:?}", o),
        };
        let s = match &row[1] {
            Value::Integer(i) => *i,
            o => panic!("聚合列应取 base 值, got {:?}", o),
        };
        let rn = match &row[2] {
            Value::Integer(i) => *i,
            o => panic!("{:?}", o),
        };
        by_g.insert(g, (s, rn));
    }
    assert_eq!(by_g.get("a"), Some(&(50, 1)), "a: SUM=50 rn=1(DESC 首位)");
    assert_eq!(by_g.get("b"), Some(&(5, 2)), "b: SUM=5 rn=2");
}

#[test]
fn window_where_subquery_not_silently_empty() {
    // v1: WHERE 含子查询时逐行求值为 NULL → 全部行被过滤 → 静默空结果
    let (db, _d) = sales();
    let r = rows(
        &db,
        "SELECT id, SUM(v) OVER (PARTITION BY cat) FROM s WHERE v IN (SELECT MAX(v) FROM s)",
    );
    assert_eq!(r.len(), 2, "v=20 的 id2 与 id3 命中");
    let mut ids: Vec<i64> = ints(&r, 0);
    ids.sort();
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn window_count_distinct_and_last_value_frame() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,5),(4,5)")
        .unwrap();
    // v1: DISTINCT 被丢弃 → 4; 标准默认帧: LAST_VALUE = 当前行值
    let r = rows(&db, "SELECT COUNT(DISTINCT v) OVER () FROM t LIMIT 1");
    assert_eq!(ints(&r, 0), vec![3]);
    let r = rows(
        &db,
        "SELECT id, LAST_VALUE(v) OVER (ORDER BY id) FROM t ORDER BY id",
    );
    assert_eq!(
        ints(&r, 1),
        vec![10, 20, 5, 5],
        "默认帧=同行组末尾(即当前行)"
    );
}

#[test]
fn recursive_cte_non_scalar_errors() {
    // 合成子查询只支持标量; 向量值必须明确报错而非字符串化
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, emb VECTOR(2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, [1.0, 2.0])").unwrap();
    let e = match db.execute(
        "WITH RECURSIVE r(e) AS (SELECT emb FROM t UNION ALL SELECT e FROM r WHERE 1 = 0) SELECT COUNT(*) FROM r",
    ) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("非标量递归结果应报错"),
    };
    assert!(
        e.contains("non-scalar") || e.contains("cannot be inlined"),
        "{}",
        e
    );
}

// ─── 复审#3 补测: 命名冲突与特殊表型 ────────────────────────────────────────

#[test]
fn window_marker_name_collision_with_user_column() {
    // 用户列恰好叫 __winval_0 — 修复前窗口标记静默覆盖该列的值
    let (db, _d) = db();
    db.execute("CREATE TABLE w(id INT PRIMARY KEY, __winval_0 INT, v INT)")
        .unwrap();
    db.execute("INSERT INTO w VALUES (1, 999, 10),(2, 888, 20)")
        .unwrap();
    let r = rows(
        &db,
        "SELECT id, __winval_0, ROW_NUMBER() OVER (ORDER BY v) FROM w ORDER BY id",
    );
    assert_eq!(ints(&r, 1), vec![999, 888], "用户列值不可被窗口标记覆盖");
    assert_eq!(ints(&r, 2), vec![1, 2], "窗口值本身正确");
}

#[test]
fn window_over_timeseries_table() {
    // 修复前: "TimeSeries table is served by the ColumnarStore, not a ColSeg"
    let (db, _d) = db();
    db.execute("CREATE TABLE ts(ts TIMESTAMP, dev TEXT, v FLOAT) TIMESERIES(ts)")
        .unwrap();
    db.execute("INSERT INTO ts VALUES (1000,'a',1.0),(2000,'a',2.0),(3000,'b',3.0)")
        .unwrap();
    let r = rows(&db, "SELECT ROW_NUMBER() OVER (ORDER BY ts) FROM ts");
    assert_eq!(ints(&r, 0), vec![1, 2, 3]);
    let r = rows(
        &db,
        "SELECT dev, SUM(v) OVER (PARTITION BY dev) FROM ts ORDER BY ts",
    );
    let sums: Vec<f64> = r
        .iter()
        .filter_map(|row| match &row[1] {
            Value::Float(f) => Some(*f),
            _ => None,
        })
        .collect();
    assert_eq!(sums, vec![3.0, 3.0, 3.0]);
}
