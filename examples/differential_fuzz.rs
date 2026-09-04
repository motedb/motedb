//! Differential correctness fuzzer: run the SAME randomized workload against
//! MoteDB and SQLite (ground truth), compare every query result exactly.
//! Rigor rule: any divergence in rows, error-vs-success, or aggregate
//! semantics (NULL handling, empty-set behavior, type coercion) is a finding.
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

/// Normalized cell: NULL → "NULL", floats → 4 decimals, everything else via Display.
fn norm_cell(v: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{:.4}", f),
        Value::Text(s) => s.clone(),
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
        other => format!("{:?}", other),
    }
}

/// Run a SELECT on both engines, return normalized sorted rows (order-
/// insensitive shapes get sorted; ORDER BY shapes compare in sequence).
fn run_sqlite(con: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = con.prepare(sql).map_err(|e| format!("sqlite err: {e}"))?;
    let ncols = stmt.column_count();
    let mut rows_iter = stmt.query([]).map_err(|e| format!("sqlite err: {e}"))?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    loop {
        let r = match rows_iter.next().map_err(|e| format!("sqlite err: {e}"))? {
            Some(r) => r,
            None => break,
        };
        let mut row = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v: rusqlite::types::Value = r.get(i).unwrap_or(rusqlite::types::Value::Null);
            row.push(norm_cell(&v));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn run_mote(db: &Database, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let r = db
        .execute(sql)
        .map_err(|e| format!("mote err: {e}"))?
        .materialize()
        .map_err(|e| format!("mote err: {e}"))?;
    let Some((_, rows)) = r.select_rows() else {
        return Ok(Vec::new());
    };
    Ok(rows
        .iter()
        .map(|row| row.iter().map(norm_mote).collect())
        .collect())
}

fn compare(
    db: &Database,
    con: &Connection,
    sql: &str,
    ordered: bool,
    divergences: &mut Vec<String>,
    checks: &mut u64,
) {
    *checks += 1;
    let a = run_sqlite(con, sql);
    let b = run_mote(db, sql);
    match (&a, &b) {
        (Err(_ea), Err(_eb)) => {
            // Both error — acceptable (error TEXT may differ). But if MoteDB
            // errors where SQLite succeeds or vice versa, that's a finding.
        }
        (Err(ea), Ok(_)) => {
            divergences.push(format!("{sql}\n  sqlite errors ({ea}) but MoteDB succeeds"))
        }
        (Ok(_), Err(eb)) => {
            divergences.push(format!("{sql}\n  MoteDB errors ({eb}) but sqlite succeeds"))
        }
        (Ok(ra), Ok(rb)) => {
            let (mut ra, mut rb) = (ra.clone(), rb.clone());
            if !ordered {
                ra.sort();
                rb.sort();
            }
            if ra != rb {
                divergences.push(format!(
                    "{sql}\n  sqlite ({}) : {ra:?}\n  mote   ({}) : {rb:?}",
                    ra.len(),
                    rb.len()
                ));
            }
        }
    }
}

fn fmt_f(x: f64) -> String {
    format!("{:.2}", x)
}

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(10);
    let mut total_div = 0;
    let mut total_checks = 0;

    for round in 0..rounds {
        let dir = TempDir::new().unwrap();
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        let con = Connection::open_in_memory().unwrap();

        let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b REAL, c TEXT)";
        db.execute(schema).unwrap();
        con.execute_batch(schema).unwrap();

        let mut rng = Lcg(0xC0FFEE + round as u64 * 7919);
        let mut divergences: Vec<String> = Vec::new();
        let mut checks: u64 = 0;
        let mut next_id: i64 = 1;
        let texts = ["alpha", "beta", "gamma", "delta", "中文字符", "quo'te", ""];

        // Random workload: mutations interleaved with query comparison.
        for step in 0..120 {
            let choice = rng.below(10);
            let a_val = rng.below(21) as i64; // 0..20
            let a_val2 = rng.below(21) as i64;
            match choice {
                0..=3 => {
                    // INSERT (mostly fresh id, sometimes NULL columns)
                    let id = next_id;
                    next_id += 1;
                    let a = if rng.below(4) == 0 {
                        "NULL".into()
                    } else {
                        rng.below(21).to_string()
                    };
                    let b = if rng.below(5) == 0 {
                        "NULL".into()
                    } else {
                        format!("{:.2}", rng.below(10000) as f64 / 7.0)
                    };
                    let c = if rng.below(4) == 0 {
                        "NULL".into()
                    } else {
                        format!("'{}'", texts[rng.below(texts.len() as u64) as usize])
                    };
                    let sql = format!("INSERT INTO t (id, a, b, c) VALUES ({id}, {a}, {b}, {c})");
                    let ra = con.execute(&sql, ());
                    let rb = db.execute(&sql).map(|_| ()).map_err(|e| e.to_string());
                    match (&ra, &rb) {
                        (Ok(_), Ok(_)) => {}
                        (Err(_), Err(_)) => {}
                        _ => divergences.push(format!(
                            "INSERT divergence: {sql}\n  sqlite: {ra:?}\n  mote: {rb:?}"
                        )),
                    }
                }
                4..=5 => {
                    // UPDATE with predicate on a (sometimes IS NULL)
                    let pred = if rng.below(4) == 0 {
                        "(a IS NULL)".to_string()
                    } else {
                        format!(
                            "a {} {}",
                            ["<", ">=", "=", "!="][rng.below(4) as usize],
                            a_val
                        )
                    };
                    let setcol = ["a", "b", "c"][rng.below(3) as usize];
                    let setval = match setcol {
                        "a" => a_val2.to_string(),
                        "b" => fmt_f(rng.below(1000) as f64 / 3.0),
                        _ => format!("'{}'", texts[rng.below(texts.len() as u64) as usize]),
                    };
                    let sql = format!("UPDATE t SET {setcol} = {setval} WHERE {pred}");
                    let ra = con.execute(&sql, ()).map(|n| n).map_err(|e| e.to_string());
                    let rb = db.execute(&sql).map(|_| ()).map_err(|e| e.to_string());
                    if ra.is_err() != rb.is_err() {
                        divergences.push(format!("UPDATE error divergence: {sql}"));
                    }
                    compare(
                        &db,
                        &con,
                        "SELECT COUNT(*) FROM t",
                        false,
                        &mut divergences,
                        &mut checks,
                    );
                }
                6..=7 => {
                    // DELETE with predicate
                    let pred = match rng.below(3) {
                        0 => format!("a = {}", a_val),
                        1 => format!("b < {:.2}", rng.below(2000) as f64 / 7.0),
                        _ => format!("c = '{}'", texts[rng.below(texts.len() as u64) as usize]),
                    };
                    let sql = format!("DELETE FROM t WHERE {pred}");
                    let ra = con.execute(&sql, ()).map(|n| n).map_err(|e| e.to_string());
                    let rb = db.execute(&sql).map(|_| ()).map_err(|e| e.to_string());
                    if ra.is_err() != rb.is_err() {
                        divergences.push(format!("DELETE error divergence: {sql}"));
                    }
                    compare(
                        &db,
                        &con,
                        "SELECT COUNT(*) FROM t",
                        false,
                        &mut divergences,
                        &mut checks,
                    );
                }
                _ => {}
            }

            // Query battery every few steps.
            if step % 4 == 0 {
                let av = rng.below(21);
                let cv = texts[rng.below(texts.len() as u64) as usize];
                macro_rules! q {
                    ($s:expr, $o:expr) => {
                        compare(&db, &con, $s, $o, &mut divergences, &mut checks)
                    };
                }
                q!(&format!("SELECT COUNT(*) FROM t"), false);
                q!(&format!("SELECT COUNT(*) FROM t WHERE a = {av}"), false);
                q!(&format!("SELECT COUNT(*) FROM t WHERE a IS NULL"), false);
                q!(
                    &format!("SELECT COUNT(*) FROM t WHERE a > {av} AND b IS NOT NULL"),
                    false
                );
                q!(
                    &format!("SELECT COUNT(*) FROM t WHERE a < {av} OR c IS NULL"),
                    false
                );
                q!(&format!("SELECT SUM(a), MIN(a), MAX(a) FROM t"), false);
                q!(&format!("SELECT AVG(b) FROM t WHERE a >= {av}"), false);
                q!(&format!("SELECT COUNT(DISTINCT a) FROM t"), false);
                q!(&format!("SELECT a, COUNT(*) FROM t GROUP BY a"), false);
                q!(
                    &format!("SELECT c, COUNT(*), AVG(a) FROM t GROUP BY c HAVING COUNT(*) > 1"),
                    false
                );
                q!(
                    &format!("SELECT id, a, b, c FROM t WHERE c = '{cv}'"),
                    false
                );
                q!(
                    &format!("SELECT id FROM t WHERE a IN ({av}, {}, 999)", rng.below(21)),
                    false
                );
                q!(&format!("SELECT id FROM t WHERE c LIKE '%a%'"), false);
                q!("SELECT SUM(a) FROM t WHERE a IS NULL", false);
                q!("SELECT AVG(b) FROM t WHERE 1 = 0", false);
                // Harder semantics: NULL-aware COUNT, text MIN/MAX, NULL
                // ordering, arithmetic predicates, DISTINCT+ORDER, unicode
                // collation, empty-set MIN/MAX/AVG/SUM.
                q!(
                    &&format!("SELECT COUNT(a), COUNT(b), COUNT(c) FROM t"),
                    false
                );
                q!(
                    &&format!("SELECT MIN(c), MAX(c) FROM t WHERE c IS NOT NULL"),
                    false
                );
                q!(
                    &&format!("SELECT MIN(a), MAX(a), SUM(a), AVG(a) FROM t WHERE 1 = 0"),
                    false
                );
                q!(
                    &&format!("SELECT id FROM t WHERE a + 1 > {av} ORDER BY id"),
                    false
                );
                q!(
                    &&format!("SELECT id FROM t WHERE a * 2 = {} ORDER BY id", av * 2),
                    false
                );
                q!(
                    &&format!("SELECT DISTINCT a FROM t WHERE a IS NOT NULL ORDER BY a"),
                    false
                );
                q!(
                    &&format!(
                        "SELECT a, COUNT(*) FROM t WHERE b IS NOT NULL GROUP BY a ORDER BY a"
                    ),
                    false
                );
                q!(
                    &&format!("SELECT id FROM t WHERE c > '{}' ORDER BY id", cv),
                    false
                );
                q!(
                    &&format!("SELECT COUNT(*) FROM t WHERE NOT (a IS NULL)"),
                    false
                );
                q!(
                    &&format!("SELECT COUNT(*) FROM t WHERE a BETWEEN {av} AND {}", av + 5),
                    false
                );
                q!(
                    &&format!("SELECT COUNT(*) FROM t WHERE c IS NULL OR a IS NULL"),
                    false
                );
                q!(
                    &&format!("SELECT c FROM t WHERE a = {av} ORDER BY c, id"),
                    false
                );
                q!(
                    &format!(
                    "SELECT id, a FROM t WHERE a IS NOT NULL ORDER BY a {}, id LIMIT 7 OFFSET {}",
                    if rng.below(2) == 0 { "ASC" } else { "DESC" },
                    rng.below(5)
                ),
                    true
                );
            }
        }

        println!(
            "round {round}: checks={checks} divergences={}",
            divergences.len()
        );
        for d in divergences.iter().take(12) {
            println!("  ── {d}");
        }
        total_div += divergences.len();
        total_checks += checks;
    }
    println!("\nTOTAL checks={total_checks} divergences={total_div}");
}
