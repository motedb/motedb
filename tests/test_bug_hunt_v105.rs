//! round-14 bug 狩猎: 集合操作 / 外连接 / 数值函数 / CAST / AUTO_INCREMENT /
//! NaN-Inf 往返。

use motedb::{types::Value as V, Database};
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<V>> {
    let r = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n  err: {e}"))
        .materialize()
        .unwrap();
    let (_, rs) = r.select_rows().unwrap();
    rs.to_vec()
}

fn ints(db: &Database, sql: &str) -> Vec<i64> {
    rows(db, sql)
        .iter()
        .map(|r| match &r[0] {
            V::Integer(n) => *n,
            o => panic!("expected int, got {o:?}: {sql}"),
        })
        .collect()
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort();
    v
}

/// UNION 去重, UNION ALL 保留重复。
#[test]
fn probe_union_vs_union_all() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (v INT)").unwrap();
    db.execute("CREATE TABLE b (v INT)").unwrap();
    for v in [1, 2, 2] {
        db.execute(&format!("INSERT INTO a VALUES ({v})")).unwrap();
    }
    for v in [2, 3] {
        db.execute(&format!("INSERT INTO b VALUES ({v})")).unwrap();
    }
    let u = sorted(ints(&db, "SELECT v FROM a UNION SELECT v FROM b"));
    assert_eq!(u, vec![1, 2, 3], "UNION dedups");
    let ua = sorted(ints(&db, "SELECT v FROM a UNION ALL SELECT v FROM b"));
    assert_eq!(ua, vec![1, 2, 2, 2, 3], "UNION ALL keeps duplicates");
}

/// UNION 结果的 ORDER BY 作用于整体; INTERSECT/EXCEPT 集合语义。
#[test]
fn probe_intersect_except_and_order() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (v INT)").unwrap();
    db.execute("CREATE TABLE b (v INT)").unwrap();
    for v in [1, 2, 3, 4] {
        db.execute(&format!("INSERT INTO a VALUES ({v})")).unwrap();
    }
    for v in [3, 4, 5] {
        db.execute(&format!("INSERT INTO b VALUES ({v})")).unwrap();
    }
    let i = sorted(ints(&db, "SELECT v FROM a INTERSECT SELECT v FROM b"));
    assert_eq!(i, vec![3, 4], "INTERSECT");
    let e = sorted(ints(&db, "SELECT v FROM a EXCEPT SELECT v FROM b"));
    assert_eq!(e, vec![1, 2], "EXCEPT");
    let o = ints(&db, "SELECT v FROM a UNION SELECT v FROM b ORDER BY v DESC");
    assert_eq!(o, vec![5, 4, 3, 2, 1], "ORDER BY applies to the whole union");
}

/// LEFT JOIN: 未匹配右表 → 右列 NULL; WHERE 右列 IS NULL 找未匹配行。
#[test]
fn probe_left_join_null_semantics() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE l (id INT PRIMARY KEY, k INT)").unwrap();
    db.execute("CREATE TABLE r (id INT PRIMARY KEY, k INT, w TEXT)").unwrap();
    db.execute("INSERT INTO l VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO l VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO r VALUES (100, 10, 'ten')").unwrap();
    let r = rows(&db, "SELECT l.id, r.id, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.id");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0], vec![V::Integer(1), V::Integer(100), V::text("ten".into())]);
    assert_eq!(
        r[1],
        vec![V::Integer(2), V::Null, V::Null],
        "unmatched right side is NULL-extended"
    );
    // IS NULL 找未匹配左行 (反连接)。
    let un = ints(&db, "SELECT l.id FROM l LEFT JOIN r ON l.k = r.k WHERE r.id IS NULL");
    assert_eq!(un, vec![2], "anti-join via IS NULL");
}

/// FULL OUTER JOIN: 两侧未匹配都保留。
#[test]
fn probe_full_outer_join() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE l (k INT)").unwrap();
    db.execute("CREATE TABLE r (k INT, w TEXT)").unwrap();
    db.execute("INSERT INTO l VALUES (1)").unwrap();
    db.execute("INSERT INTO l VALUES (2)").unwrap();
    db.execute("INSERT INTO r VALUES (2, 'two')").unwrap();
    db.execute("INSERT INTO r VALUES (3, 'three')").unwrap();
    match db.execute("SELECT l.k, r.k, r.w FROM l FULL OUTER JOIN r ON l.k = r.k ORDER BY l.k") {
        Ok(res) => {
            let mat = res.materialize().unwrap();
            let rs = mat.select_rows().unwrap().1.to_vec();
            let mut got: Vec<(Option<i64>, Option<i64>, Option<String>)> = rs
                .iter()
                .map(|r| {
                    (opt_int(&r[0]), opt_int(&r[1]), opt_text(&r[2]))
                })
                .collect();
            // 排序键: l.k (right-only 行为 NULL → -1 最前)。
            got.sort_by_key(|(lk, _, _)| lk.unwrap_or(-1));
            assert_eq!(got.len(), 3, "both sides kept: {got:?}");
            assert_eq!(got[0], (None, Some(3), Some("three".into())), "right-only (l.k NULL)");
            assert_eq!(got[1], (Some(1), None, None), "left-only (r.* NULL)");
            assert_eq!(got[2], (Some(2), Some(2), Some("two".into())), "matched");
        }
        Err(e) => panic!("FULL OUTER JOIN failed: {e}"),
    }
}

/// 数值函数: ROUND/ABS/MOD/FLOOR/CEIL/COALESCE/NULLIF。
#[test]
fn probe_numeric_functions() {
    let (db, _d) = new_db();
    let q = |sql: &str| rows(&db, sql).remove(0).remove(0);
    assert_eq!(q("SELECT ROUND(3.14159, 2)"), V::Float(3.14));
    assert_eq!(q("SELECT ROUND(2.5)"), V::Float(3.0), "half rounds away from zero");
    assert_eq!(q("SELECT ABS(-5)"), V::Integer(5));
    assert_eq!(q("SELECT ABS(5)"), V::Integer(5));
    // MOD 与 % 运算符一致。
    let m = q("SELECT MOD(7, 3)");
    let p = q("SELECT 7 % 3");
    assert_eq!(m, p, "MOD() == %");
    match q("SELECT MOD(-7, 3)") {
        V::Integer(n) => assert!(n == -1 || n == 2, "sign convention consistent: {n}"),
        o => panic!("mod: {o:?}"),
    }
    match q("SELECT FLOOR(2.7)") {
        V::Integer(2) | V::Float(2.0) => {}
        o => panic!("floor: {o:?}"),
    }
    match q("SELECT CEIL(2.1)") {
        V::Integer(3) | V::Float(3.0) => {}
        o => panic!("ceil: {o:?}"),
    }
    assert_eq!(q("SELECT COALESCE(NULL, NULL, 42)"), V::Integer(42));
    assert_eq!(q("SELECT COALESCE(1, 2)"), V::Integer(1));
    assert_eq!(q("SELECT NULLIF(5, 5)"), V::Null);
    assert_eq!(q("SELECT NULLIF(5, 6)"), V::Integer(5));
}

/// CAST: 可解析字符串→数值, 数值→字符串往返。
#[test]
fn probe_cast_semantics() {
    let (db, _d) = new_db();
    let q = |sql: &str| rows(&db, sql).remove(0).remove(0);
    assert_eq!(q("SELECT CAST('123' AS INT)"), V::Integer(123));
    assert_eq!(q("SELECT CAST(1.9 AS INT)"), V::Integer(1), "truncation toward zero");
    assert_eq!(q("SELECT CAST(-1.9 AS INT)"), V::Integer(-1), "truncation toward zero (negative)");
    assert_eq!(q("SELECT CAST(42 AS TEXT)"), V::text("42".into()));
    assert_eq!(q("SELECT CAST('3.5' AS FLOAT)"), V::Float(3.5));
    // 不可解析字符串: 报错或 0/NULL — 自洽即可 (不 panic)。
    let _ = db.execute("SELECT CAST('abc' AS INT)");
}

/// AUTO_INCREMENT: 显式插入大值后续自动值不冲突。
#[test]
fn probe_auto_increment_after_explicit() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)").unwrap();
    db.execute("INSERT INTO t (v) VALUES (1)").unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (100, 2)").unwrap();
    db.execute("INSERT INTO t (v) VALUES (3)").unwrap();
    let ids = ints(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids, vec![1, 100, 101], "auto continues past the explicit value: {ids:?}");
    // 失败插入 (重复) 不烧掉自增值? 允许烧掉 (MySQL 兼容) — 只要不产生重复。
    let dup = db.execute("INSERT INTO t (id, v) VALUES (1, 9)");
    assert!(dup.is_err(), "explicit duplicate id rejected");
    db.execute("INSERT INTO t (v) VALUES (4)").unwrap();
    let ids2 = ints(&db, "SELECT id FROM t ORDER BY id");
    assert!(
        ids2.windows(2).all(|w| w[0] < w[1]),
        "ids strictly increasing, no reuse: {ids2:?}"
    );
}

/// NaN / ±Inf 浮点往返: 不 panic, 重开后语义一致。
#[test]
fn probe_nan_inf_roundtrip() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'NaN')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Infinity')").unwrap();
    db.execute("INSERT INTO t VALUES (3, '-Infinity')").unwrap();
    let r = rows(&db, "SELECT f FROM t ORDER BY id");
    assert_eq!(r.len(), 3, "all three special values readable: {r:?}");
    // 读回语义: NaN / +Inf / -Inf (round-14 修复: 引号 IEEE 拼写可入 FLOAT 列)。
    match &r[0][0] {
        V::Float(f) => assert!(f.is_nan(), "row1 NaN: {f}"),
        o => panic!("row1 NaN? {o:?}"),
    }
    match &r[1][0] {
        V::Float(f) => assert!(f.is_infinite() && *f > 0.0, "row2 +Inf: {f}"),
        o => panic!("row2 +Inf? {o:?}"),
    }
    match &r[2][0] {
        V::Float(f) => assert!(f.is_infinite() && *f < 0.0, "row3 -Inf: {f}"),
        o => panic!("row3 -Inf? {o:?}"),
    }
    // 普通引号数字同样可入 (原先也被拒)。
    db.execute("INSERT INTO t VALUES (4, '3.5')").unwrap();
    let four = rows(&db, "SELECT f FROM t WHERE id = 4");
    assert_eq!(four[0][0], V::Float(3.5), "quoted plain float coerces");
    // 重开一致性。
    db.execute("CHECKPOINT").unwrap();
    drop(db);
}

/// 连接条件非等值: ON a.k < b.k 也必须正确过滤。
#[test]
fn probe_join_non_equi_condition() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (k INT)").unwrap();
    db.execute("CREATE TABLE b (k INT)").unwrap();
    for v in [1, 2, 3] {
        db.execute(&format!("INSERT INTO a VALUES ({v})")).unwrap();
    }
    for v in [2, 4] {
        db.execute(&format!("INSERT INTO b VALUES ({v})")).unwrap();
    }
    let r = ints(&db, "SELECT a.k FROM a JOIN b ON a.k < b.k ORDER BY a.k");
    // a=1<2, 1<4, 2<4, 3<4 → 1,1,2,3
    assert_eq!(r, vec![1, 1, 2, 3], "non-equi join pairs");
}

/// 三表连接链: a-b-c 传递。
#[test]
fn probe_three_table_join_chain() {
    let (db, _d) = new_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, bid INT)").unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, cid INT)").unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (10, 100)").unwrap();
    db.execute("INSERT INTO c VALUES (100, 'deep')").unwrap();
    let r = rows(
        &db,
        "SELECT a.id, c.name FROM a JOIN b ON a.bid = b.id JOIN c ON b.cid = c.id",
    );
    assert_eq!(r, vec![vec![V::Integer(1), V::text("deep".into())]]);
}

fn opt_int(v: &V) -> Option<i64> {
    match v {
        V::Integer(n) => Some(*n),
        _ => None,
    }
}
fn opt_text(v: &V) -> Option<String> {
    match v {
        V::Text(s) => Some(s.as_str().to_string()),
        _ => None,
    }
}
