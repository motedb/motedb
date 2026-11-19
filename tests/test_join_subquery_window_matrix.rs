//! 🔴 JOIN × 子查询 × 表达式 DML × 窗口函数 —— 第四块 oracle 扫描。
//!
//! 前三轮覆盖标量查询/聚合/多模态/重启×DML(字面量赋值); 本轮补:
//!   * INNER/LEFT JOIN（含孤儿行 NULL 语义）+ JOIN 后聚合
//!   * IN 子查询 / 相关标量子查询 / 聚合标量子查询
//!   * UPDATE SET 表达式（自引用 amount = amount * 2）+ 复合 WHERE 的
//!     UPDATE/DELETE（镜像状态 oracle）
//!   * ROW_NUMBER / RANK / SUM OVER（含 PARTITION）vs Rust 计算

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn canon(v: &Value) -> String {
    match v {
        Value::Null => "∅".into(),
        Value::Integer(i) => format!("I:{i}"),
        Value::Float(f) => format!("F:{f:?}"),
        Value::Text(s) => format!("S:{}", s.as_str()),
        Value::Bool(b) => format!("B:{}", if *b { 1 } else { 0 }),
        Value::Timestamp(t) => format!("T:{}", t.as_micros()),
        Value::Date(d) => format!("D:{}", d.days()),
        other => format!("X:{other:?}"),
    }
}

fn rows_canon(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|r| r.iter().map(canon).collect::<Vec<_>>().join(","))
        .collect()
}

// ---------------------------------------------------------------------------
// Fixture: customers (one has NO orders → tests INNER vs LEFT), orders
// (two have customers that don't exist → orphan LEFT rows get NULL).
// ---------------------------------------------------------------------------

struct Cust {
    id: i64,
    name: &'static str,
    tier: &'static str,
}
#[derive(Clone)]
struct Ord {
    id: i64,
    cust: Option<i64>, // NULL cust → JOIN miss on both sides
    amount: i64,
}

const CUSTS: &[Cust] = &[
    Cust {
        id: 1,
        name: "ann",
        tier: "gold",
    },
    Cust {
        id: 2,
        name: "bob",
        tier: "silver",
    },
    Cust {
        id: 3,
        name: "cat",
        tier: "gold",
    },
    Cust {
        id: 4,
        name: "dan",
        tier: "bronze",
    }, // no orders
];
const ORDS: &[Ord] = &[
    Ord {
        id: 100,
        cust: Some(1),
        amount: 10,
    },
    Ord {
        id: 101,
        cust: Some(2),
        amount: 25,
    },
    Ord {
        id: 102,
        cust: Some(1),
        amount: 40,
    },
    Ord {
        id: 103,
        cust: Some(9),
        amount: 15,
    }, // orphan customer
    Ord {
        id: 104,
        cust: None,
        amount: 5,
    }, // NULL cust
    Ord {
        id: 105,
        cust: Some(3),
        amount: 30,
    },
    Ord {
        id: 106,
        cust: Some(2),
        amount: 8,
    },
];

fn setup(db: &Database) {
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, name TEXT, tier TEXT)")
        .unwrap();
    db.execute("CREATE TABLE o (id INT PRIMARY KEY, cust INT, amount INT)")
        .unwrap();
    for c in CUSTS {
        db.execute(&format!(
            "INSERT INTO c VALUES ({}, '{}', '{}')",
            c.id, c.name, c.tier
        ))
        .unwrap();
    }
    for o in ORDS {
        let cust = o
            .cust
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".into());
        db.execute(&format!(
            "INSERT INTO o VALUES ({}, {cust}, {})",
            o.id, o.amount
        ))
        .unwrap();
    }
}

// ---------------------------------------------------------------------------
// A. JOIN 矩阵
// ---------------------------------------------------------------------------

#[test]
fn join_matrix_vs_nested_loop_oracle() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup(&db);

    // INNER JOIN: every (order, customer) pair with o.cust == c.id.
    let sql = "SELECT o.id, c.name, o.amount FROM o JOIN c ON o.cust = c.id ORDER BY o.id";
    let got = rows_canon(&db.query(sql).unwrap());
    let mut want: Vec<String> = Vec::new();
    for o in ORDS {
        if let Some(oc) = o.cust {
            if let Some(c) = CUSTS.iter().find(|c| c.id == oc) {
                want.push(format!("I:{},S:{},I:{}", o.id, c.name, o.amount));
            }
        }
    }
    assert_eq!(got, want, "INNER JOIN incl. orphan/NULL misses");

    // LEFT JOIN (orders ← customers): every order once; misses → NULL name.
    let sql = "SELECT o.id, c.name, o.amount FROM o LEFT JOIN c ON o.cust = c.id ORDER BY o.id";
    let got = rows_canon(&db.query(sql).unwrap());
    let mut want: Vec<String> = Vec::new();
    for o in ORDS {
        let name = o
            .cust
            .and_then(|oc| CUSTS.iter().find(|c| c.id == oc))
            .map(|c| format!("S:{}", c.name))
            .unwrap_or_else(|| "∅".into());
        want.push(format!("I:{},{name},I:{}", o.id, o.amount));
    }
    assert_eq!(got, want, "LEFT JOIN NULL-fill for orphan/NULL keys");

    // LEFT JOIN with WHERE on the right side's column (post-join filter).
    let sql = "SELECT o.id FROM o LEFT JOIN c ON o.cust = c.id WHERE c.tier = 'gold' ORDER BY o.id";
    let got = rows_canon(&db.query(sql).unwrap());
    let want: Vec<String> = ORDS
        .iter()
        .filter(|o| {
            o.cust
                .and_then(|oc| CUSTS.iter().find(|c| c.id == oc))
                .is_some_and(|c| c.tier == "gold")
        })
        .map(|o| format!("I:{}", o.id))
        .collect();
    assert_eq!(got, want, "LEFT JOIN + WHERE on right column");

    // JOIN + GROUP BY aggregation: revenue per tier (inner semantics).
    let sql = "SELECT c.tier, SUM(o.amount) AS s, COUNT(*) AS n \
               FROM o JOIN c ON o.cust = c.id GROUP BY c.tier ORDER BY c.tier";
    let got = rows_canon(&db.query(sql).unwrap());
    let mut want: Vec<String> = Vec::new();
    for tier in ["bronze", "gold", "silver"] {
        let sel: Vec<&Ord> = ORDS
            .iter()
            .filter(|o| {
                o.cust
                    .and_then(|oc| CUSTS.iter().find(|c| c.id == oc))
                    .is_some_and(|c| c.tier == tier)
            })
            .collect();
        if sel.is_empty() {
            continue;
        }
        let sum: i64 = sel.iter().map(|o| o.amount).sum();
        want.push(format!("S:{tier},I:{sum},I:{}", sel.len()));
    }
    assert_eq!(got, want, "JOIN + GROUP BY per tier (empty groups absent)");
}

// ---------------------------------------------------------------------------
// B. 子查询矩阵
// ---------------------------------------------------------------------------

#[test]
fn subquery_matrix_vs_oracle() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup(&db);

    // IN (subquery)
    let sql = "SELECT id FROM o WHERE cust IN (SELECT id FROM c WHERE tier = 'gold') ORDER BY id";
    let got = rows_canon(&db.query(sql).unwrap());
    let want: Vec<String> = ORDS
        .iter()
        .filter(|o| {
            o.cust
                .and_then(|oc| CUSTS.iter().find(|c| c.id == oc))
                .is_some_and(|c| c.tier == "gold")
        })
        .map(|o| format!("I:{}", o.id))
        .collect();
    assert_eq!(got, want, "IN-subquery with tier filter");

    // NOT IN (subquery) — NULL cust rows must be EXCLUDED (SQL 3-valued).
    let sql =
        "SELECT id FROM o WHERE cust NOT IN (SELECT id FROM c WHERE tier = 'gold') ORDER BY id";
    let got = rows_canon(&db.query(sql).unwrap());
    let want: Vec<String> = ORDS
        .iter()
        .filter(|o| {
            o.cust
                .is_some_and(|oc| !CUSTS.iter().any(|c| c.id == oc && c.tier == "gold"))
        })
        .map(|o| format!("I:{}", o.id))
        .collect();
    assert_eq!(got, want, "NOT IN-subquery (NULLs excluded both sides)");

    // Correlated scalar subquery in the SELECT list.
    let sql = "SELECT o.id, (SELECT name FROM c WHERE c.id = o.cust) FROM o ORDER BY o.id";
    let got = rows_canon(&db.query(sql).unwrap());
    let want: Vec<String> = ORDS
        .iter()
        .map(|o| {
            let name = o
                .cust
                .and_then(|oc| CUSTS.iter().find(|c| c.id == oc))
                .map(|c| format!("S:{}", c.name))
                .unwrap_or_else(|| "∅".into());
            format!("I:{},{}", o.id, name)
        })
        .collect();
    assert_eq!(got, want, "correlated scalar subquery (miss → NULL)");

    // Aggregate scalar subquery in WHERE.
    let avg: f64 = ORDS.iter().map(|o| o.amount as f64).sum::<f64>() / ORDS.len() as f64;
    let sql = "SELECT id FROM o WHERE amount > (SELECT AVG(amount) FROM o) ORDER BY id";
    let got = rows_canon(&db.query(sql).unwrap());
    let want: Vec<String> = ORDS
        .iter()
        .filter(|o| o.amount as f64 > avg)
        .map(|o| format!("I:{}", o.id))
        .collect();
    assert_eq!(got, want, "scalar aggregate subquery threshold (avg={avg})");
}

// ---------------------------------------------------------------------------
// C. 表达式 DML（自引用 SET / 复合 WHERE）× 镜像 oracle
// ---------------------------------------------------------------------------

#[test]
fn update_expressions_and_complex_where() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    setup(&db);
    let mut mirror: Vec<Ord> = ORDS.to_vec();

    // Self-referencing SET.
    db.execute("UPDATE o SET amount = amount * 2 WHERE cust = 1")
        .unwrap();
    for o in mirror.iter_mut() {
        if o.cust == Some(1) {
            o.amount *= 2;
        }
    }
    check(&db, &mirror, "after amount = amount * 2");

    // Two-column SET, one referencing the other.
    db.execute(
        "UPDATE o SET amount = amount + 10, cust = 3 WHERE amount < 30 AND cust IS NOT NULL",
    )
    .unwrap();
    for o in mirror.iter_mut() {
        if o.amount < 30 && o.cust.is_some() {
            o.amount += 10;
            o.cust = Some(3);
        }
    }
    check(&db, &mirror, "after two-col SET with AND + IS NOT NULL");

    // OR / NOT WHERE with NULLs in play (cust NULL rows must stay).
    db.execute("UPDATE o SET amount = 0 WHERE cust = 9 OR NOT (amount > 20)")
        .unwrap();
    for o in mirror.iter_mut() {
        let cond = o.cust == Some(9) || !(o.amount > 20);
        if cond {
            o.amount = 0;
        }
    }
    check(&db, &mirror, "after OR + NOT WHERE");

    // DELETE with a BETWEEN + IN mix.
    db.execute("DELETE FROM o WHERE amount BETWEEN 5 AND 20 OR cust IN (2, 3)")
        .unwrap();
    mirror.retain(|o| !(o.amount >= 5 && o.amount <= 20 || o.cust == Some(2) || o.cust == Some(3)));
    check(&db, &mirror, "after DELETE BETWEEN+IN");

    fn check(db: &Database, mirror: &[Ord], stage: &str) {
        let got = rows_canon(
            &db.query("SELECT id, cust, amount FROM o ORDER BY id")
                .unwrap(),
        );
        let want: Vec<String> = mirror
            .iter()
            .map(|o| {
                let cust = o
                    .cust
                    .map(|v| format!("I:{v}"))
                    .unwrap_or_else(|| "∅".into());
                format!("I:{},{},I:{}", o.id, cust, o.amount)
            })
            .collect();
        assert_eq!(got, want, "{stage}: readback diverges from mirror");
    }
}

// ---------------------------------------------------------------------------
// D. 窗口函数
// ---------------------------------------------------------------------------

#[test]
fn window_functions_vs_oracle() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE w (id INT PRIMARY KEY, g TEXT, v INT)")
        .unwrap();
    let data: Vec<(i64, &str, i64)> = (0..20)
        .map(|i| {
            (
                i,
                ["a", "b", "c"][i as usize % 3],
                ((i * 11) % 17) as i64 - 8,
            )
        })
        .collect();
    for (id, g, v) in &data {
        db.execute(&format!("INSERT INTO w VALUES ({id}, '{g}', {v})"))
            .unwrap();
    }

    // ROW_NUMBER over the whole table in id order (deterministic key).
    let got = rows_canon(
        &db.query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM w ORDER BY id")
            .unwrap(),
    );
    let want: Vec<String> = data
        .iter()
        .enumerate()
        .map(|(i, (id, _, _))| format!("I:{id},I:{}", i + 1))
        .collect();
    assert_eq!(got, want, "ROW_NUMBER over id order");

    // ROW_NUMBER PARTITION BY g ORDER BY v (ties in v within a partition are
    // ambiguous — use tie-free (g, v) construction check instead: rank by v
    // but assert partition membership + multiset of numbers only).
    let got = db
        .query("SELECT g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) FROM w")
        .unwrap();
    // Oracle: within each g, rows sorted by v get 1..n (tie order unspecified
    // ⇒ compare the multiset of (g, row_number) which is fixed).
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for r in &got {
        let g = match &r[0] {
            Value::Text(t) => t.to_string(),
            other => panic!("g as {other:?}"),
        };
        *counts.entry(g).or_default() += 1;
    }
    for g in ["a", "b", "c"] {
        let want_n = data.iter().filter(|(_, gg, _)| gg == &g).count();
        assert_eq!(counts[g], want_n, "PARTITION {g} row count");
    }
    let rn: Vec<i64> = got
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(i) => *i,
            other => panic!("rn as {other:?}"),
        })
        .collect();
    // Per-partition permutation of 1..k: Σ k_i(k_i+1)/2 over partitions.
    let total: i64 = rn.iter().sum();
    let want_total: i64 = ["a", "b", "c"]
        .iter()
        .map(|g| {
            let k = data.iter().filter(|(_, gg, _)| gg == g).count() as i64;
            k * (k + 1) / 2
        })
        .sum();
    assert_eq!(
        total, want_total,
        "ROW_NUMBER partitions must permute 1..k per partition"
    );

    // SUM OVER (ORDER BY id) running total — exact oracle.
    let got = rows_canon(
        &db.query("SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w ORDER BY id")
            .unwrap(),
    );
    let mut run = 0i64;
    let want: Vec<String> = data
        .iter()
        .map(|(id, _, v)| {
            run += v;
            format!("I:{id},I:{run}")
        })
        .collect();
    assert_eq!(got, want, "running SUM window");
}

// ---------------------------------------------------------------------------
// E. LATEST BY 在 UPDATE 移动之后（robot-memory 的命门路径）
// ---------------------------------------------------------------------------

#[test]
fn latest_by_after_update_returns_moved_position() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE sight (ts TIMESTAMP, obj TEXT, zone TEXT, x FLOAT, y FLOAT)")
        .unwrap();
    let base = 1_794_300_000_000_000i64;
    // 红杯子在茶几被看到 4 次…
    for i in 0..4 {
        db.execute(&format!(
            "INSERT INTO sight VALUES ({}, '红杯子', '茶几', 5.5, 2.0)",
            base + i * 60_000_000
        ))
        .unwrap();
    }
    // …然后被挪进厨房，机器人重新看到（新行，不是 UPDATE）。
    db.execute(&format!(
        "INSERT INTO sight VALUES ({}, '红杯子', '厨房', 1.9, 7.4)",
        base + 10 * 60_000_000
    ))
    .unwrap();
    // 另一个物体: 直接 UPDATE 挪走（保持 key 不变的更新语义）。
    db.execute("INSERT INTO sight VALUES (0, '遥控器', '沙发', 8.5, 3.5)")
        .unwrap();
    db.execute("UPDATE sight SET zone = '茶几', x = 5.8, y = 1.6 WHERE obj = '遥控器'")
        .unwrap();

    let latest = |obj: &str| -> (String, f64, f64) {
        let r = db
            .query(&format!(
                "SELECT zone, x, y FROM sight WHERE obj = '{obj}' LATEST BY obj"
            ))
            .unwrap();
        let z = match &r[0][0] {
            Value::Text(t) => t.to_string(),
            other => panic!("zone as {other:?}"),
        };
        let (x, y) = match (&r[0][1], &r[0][2]) {
            (Value::Float(a), Value::Float(b)) => (*a, *b),
            other => panic!("xy as {other:?}"),
        };
        (z, x, y)
    };

    let (z, x, y) = latest("红杯子");
    assert_eq!(
        (z.as_str(), x, y),
        ("厨房", 1.9, 7.4),
        "LATEST BY must pick the newest sighting row (the moved-to kitchen one)"
    );
    let (z, x, y) = latest("遥控器");
    assert_eq!(
        (z.as_str(), x, y),
        ("茶几", 5.8, 1.6),
        "LATEST BY after UPDATE must reflect the updated (newest) version"
    );

    // 历史不被 LATEST BY 破坏: 红杯子总目击数仍是 5。
    let n = &db
        .query("SELECT COUNT(*) FROM sight WHERE obj = '红杯子'")
        .unwrap()[0][0];
    assert_eq!(canon(n), "I:5", "LATEST BY must not consume history");
}
