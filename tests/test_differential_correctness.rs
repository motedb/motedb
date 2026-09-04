//! Differential correctness vs SQLite, CI-sized. Same randomized workload on
//! both engines, results compared exactly — including JOIN shapes and
//! boundary values (large integers, extreme floats, tricky strings).
//! Deep sweeps: `cargo run --release --example differential_fuzz`.
use motedb::{DBConfig, Database};
use rusqlite::Connection;
use tempfile::TempDir;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn norm_cell(v: rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{:.4}", f),
        Value::Text(s) => s,
        Value::Blob(b) => format!("blob({})", b.len()),
    }
}

fn norm_mote(v: &motedb::types::Value) -> String {
    use motedb::types::Value as V;
    match v {
        V::Null => "NULL".into(),
        V::Integer(i) => i.to_string(),
        V::Float(f) => format!("{:.4}", f),
        V::Text(s) => s.as_str().to_string(),
        V::Bool(b) => (if *b { 1 } else { 0 }).to_string(),
        other => format!("{other:?}"),
    }
}

fn run_sqlite(con: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = con.prepare(sql).map_err(|e| format!("sqlite: {e}"))?;
    let ncols = stmt.column_count();
    let mut rows_iter = stmt.query([]).map_err(|e| format!("sqlite: {e}"))?;
    let mut rows = Vec::new();
    while let Some(r) = rows_iter.next().map_err(|e| format!("sqlite: {e}"))? {
        let mut row = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v: rusqlite::types::Value = r.get(i).unwrap_or(rusqlite::types::Value::Null);
            row.push(norm_cell(v));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn run_mote(db: &Database, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let r = db
        .execute(sql)
        .map_err(|e| format!("mote: {e}"))?
        .materialize()
        .map_err(|e| format!("mote: {e}"))?;
    let Some((_, rows)) = r.select_rows() else {
        return Ok(Vec::new());
    };
    Ok(rows
        .iter()
        .map(|row| row.iter().map(norm_mote).collect())
        .collect())
}

fn dump_state(db: &Database, con: &Connection) -> String {
    let mote_ids = run_mote(db, "SELECT id FROM t ORDER BY id")
        .unwrap_or_default()
        .iter()
        .map(|r| r.join(","))
        .collect::<Vec<_>>();
    let sqlite_ids = run_sqlite(con, "SELECT id FROM t ORDER BY id")
        .unwrap_or_default()
        .iter()
        .map(|r| r.join(","))
        .collect::<Vec<_>>();
    let probes = [
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(a) FROM t",
        "SELECT COUNT(b) FROM t",
        "SELECT COUNT(c) FROM t",
        "SELECT COUNT(a), COUNT(b) FROM t",
        "SELECT COUNT(a), COUNT(c) FROM t",
        "SELECT COUNT(*), COUNT(a) FROM t",
    ];
    let mut probe_out = String::new();
    for q in probes {
        let m = run_mote(db, q).unwrap_or_default();
        let sq = run_sqlite(con, q).unwrap_or_default();
        probe_out.push_str(&format!("\n      {q}: mote {m:?} sqlite {sq:?}",));
    }
    format!(
        "\n      mote ids({}): {mote_ids:?}\n      sqlite ids({}): {sqlite_ids:?}{probe_out}",
        mote_ids.len(),
        sqlite_ids.len()
    )
}

fn compare(
    db: &Database,
    con: &Connection,
    sql: &str,
    ordered: bool,
    label: &str,
    divergences: &mut Vec<String>,
) {
    let a = run_sqlite(con, sql);
    let b = run_mote(db, sql);
    match (&a, &b) {
        (Err(_), Err(_)) => {}
        (Err(ea), Ok(_)) => divergences.push(format!(
            "{label} {sql}\n  sqlite errors ({ea}), MoteDB succeeds"
        )),
        (Ok(_), Err(eb)) => divergences.push(format!(
            "{label} {sql}\n  MoteDB errors ({eb}), sqlite succeeds"
        )),
        (Ok(ra), Ok(rb)) => {
            let (mut ra, mut rb) = (ra.clone(), rb.clone());
            if !ordered {
                ra.sort();
                rb.sort();
            }
            if ra != rb {
                let dump = if divergences.is_empty() {
                    dump_state(db, con)
                } else {
                    String::new()
                };
                divergences.push(format!(
                    "{label} {sql}\n  sqlite ({}) {ra:?}\n  mote   ({}) {rb:?}{dump}",
                    ra.len(),
                    rb.len()
                ));
            }
        }
    }
}

/// Boundary values exercising edges without f64-precision ambiguity.
const EDGE_INTS: &[i64] = &[
    0,
    1,
    -1,
    i64::MAX / 4,
    -(i64::MAX / 4),
    4_611_686_018_427_387_904,  // 2^62
    -4_611_686_018_427_387_904, // -2^62
    9_007_199_254_740_991,      // 2^53 - 1
];
const EDGE_FLOATS: &[&str] = &[
    "0.0",
    "-0.0",
    "1e300",
    "-1e300",
    "5e-324",
    "2.2250738585072014e-308",
];
const EDGE_TEXTS: &[&str] = &[
    "''",
    "'x'",
    "'unpaired surrogate later'",
    "'quote''s'",
    "'new\\nline'",
    "'中文字符∪emoji'",
    "' leading and trailing '",
    "'zero\\0free'",
];

#[test]
fn differential_vs_sqlite_including_joins_and_boundaries() {
    let mut total_div = Vec::new();
    for round in 0..3u64 {
        let dir = TempDir::new().unwrap();
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        let con = Connection::open_in_memory().unwrap();

        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b REAL, c TEXT)",
            "CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER, tag TEXT, score REAL)",
        ] {
            db.execute(stmt).unwrap();
            con.execute_batch(stmt).unwrap();
        }

        let mut rng = Lcg(0xD1FFE + round * 104729);
        let mut divergences: Vec<String> = Vec::new();
        let mut next_id: i64 = 1;

        // Seed rows incl. boundary values so both engines see identical data.
        for k in 0..40i64 {
            let id = next_id;
            next_id += 1;
            let a = if k < 8 {
                EDGE_INTS[k as usize].to_string()
            } else {
                rng.below(21).to_string()
            };
            let b = if k < 6 {
                EDGE_FLOATS[k as usize].to_string()
            } else {
                format!("{:.2}", rng.below(10000) as f64 / 7.0)
            };
            let c = if k < 8 {
                EDGE_TEXTS[k as usize].to_string()
            } else {
                format!("'t{}'", rng.below(6))
            };
            let sql = format!("INSERT INTO t (id, a, b, c) VALUES ({id}, {a}, {b}, {c})");
            let _ = con.execute(&sql, ());
            let _ = db.execute(&sql);
            // Join table row referencing a random t id (some NULL / dangling).
            let t_ref = if rng.below(6) == 0 {
                "NULL".to_string()
            } else {
                rng.below(50).to_string()
            };
            let score = format!("{:.2}", rng.below(500) as f64 / 3.0);
            let tag = format!("'g{}'", rng.below(5));
            let usql = format!(
                "INSERT INTO u (id, t_id, tag, score) VALUES ({id}, {t_ref}, {tag}, {score})"
            );
            let _ = con.execute(&usql, ());
            let _ = db.execute(&usql);
        }

        // Random mutations + query battery.
        for step in 0..24 {
            let a_val = rng.below(21) as i64;
            match rng.below(8) {
                0..=1 => {
                    let id = next_id;
                    next_id += 1;
                    let sql = format!(
                        "INSERT INTO t (id, a, b, c) VALUES ({id}, {}, {:.2}, 't{}')",
                        rng.below(21),
                        rng.below(1000) as f64 / 7.0,
                        rng.below(6)
                    );
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                2 => {
                    let sql = format!(
                        "UPDATE t SET b = {:.2} WHERE a >= {a_val}",
                        rng.below(500) as f64 / 3.0
                    );
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                3 => {
                    let sql = format!("DELETE FROM t WHERE a = {a_val} AND c = 't1'");
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                4 => {
                    let sql = format!(
                        "UPDATE u SET tag = 'gx' WHERE t_id IS NULL OR score > {:.2}",
                        rng.below(400) as f64 / 3.0
                    );
                    let _ = con.execute(&sql, ());
                    let _ = db.execute(&sql);
                }
                _ => {}
            }

            if step % 3 == 0 {
                let av = rng.below(21);
                let label = format!("[r{round} s{step}]");
                let mut run =
                    |sql: String, o: bool| compare(&db, &con, &sql, o, &label, &mut divergences);
                // Core shapes.
                run(format!("SELECT COUNT(*) FROM t"), false);
                run(format!("SELECT COUNT(a), COUNT(b) FROM t"), false);
                run(format!("SELECT SUM(a) FROM t WHERE a < {av}"), false);
                run(format!("SELECT AVG(b) FROM t WHERE a >= {av}"), false);
                run(
                    format!("SELECT MIN(c), MAX(c) FROM t WHERE c IS NOT NULL"),
                    false,
                );
                // JOIN shapes (INNER / LEFT / RIGHT / FULL, with predicates).
                run(
                    format!("SELECT COUNT(*) FROM t INNER JOIN u ON u.t_id = t.id"),
                    false,
                );
                run(
                    format!(
                        "SELECT t.id, u.id FROM t INNER JOIN u ON u.t_id = t.id WHERE t.a >= {av}"
                    ),
                    false,
                );
                run(
                    format!("SELECT t.id, u.tag FROM t LEFT JOIN u ON u.t_id = t.id AND u.score > {:.2}", rng.below(400) as f64 / 3.0),
                    false,
                );
                run(
                    format!(
                        "SELECT COUNT(*) FROM t LEFT JOIN u ON u.t_id = t.id WHERE u.id IS NULL"
                    ),
                    false,
                );
                run(
                    format!("SELECT u.tag, COUNT(*), AVG(t.a) FROM t INNER JOIN u ON u.t_id = t.id GROUP BY u.tag"),
                    false,
                );
                // RIGHT / FULL JOIN.
                run(
                    format!(
                        "SELECT t.id, u.id FROM u RIGHT JOIN t ON u.t_id = t.id WHERE t.a < {av}"
                    ),
                    false,
                );
                run(
                    format!("SELECT COUNT(*) FROM t FULL JOIN u ON u.t_id = t.id"),
                    false,
                );
                // Boundary predicates.
                run(
                    format!("SELECT COUNT(*) FROM t WHERE a > 9000000000000000000"),
                    false,
                );
                run(
                    format!("SELECT COUNT(*) FROM t WHERE b > 1e299 OR b < -1e299"),
                    false,
                );
                run(format!("SELECT COUNT(*) FROM t WHERE c = 'x'"), false);
                run(
                    format!("SELECT id FROM t WHERE a = 4611686018427387904"),
                    false,
                );
                run(format!("SELECT COUNT(*) FROM t WHERE b = 0.0"), false);
                // Ordered shape with tie-break.
                run(
                    format!("SELECT id, a FROM t WHERE a IS NOT NULL ORDER BY a DESC, id LIMIT 5 OFFSET {av}"),
                    true,
                );
            }
        }
        if !divergences.is_empty() {
            total_div.extend(divergences);
        }
    }
    assert!(
        total_div.is_empty(),
        "differential divergences vs SQLite:\n{}",
        total_div
            .iter()
            .take(12)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n──\n")
    );
}
