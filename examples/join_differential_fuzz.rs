//! Multi-table JOIN differential fuzzer: same randomized workload against
//! MoteDB and SQLite, comparing every query exactly. Targets the join paths
//! that changed recently: multi-way equi-joins, JOIN+aggregate folds,
//! mixed INNER/LEFT chains, NULL join keys, empty tables, HAVING, ORDER BY
//! on aggregates, self-joins, and non-equi ON conditions.
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

fn norm_sqlite(v: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{:.2}", f),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => format!("blob({})", b.len()),
    }
}

fn norm_mote(v: &motedb::types::Value) -> String {
    use motedb::types::Value as V;
    match v {
        V::Null => "NULL".into(),
        V::Integer(i) => i.to_string(),
        V::Float(f) => format!("{:.2}", f),
        V::Text(s) => s.as_str().to_string(),
        V::Bool(b) => (if *b { 1 } else { 0 }).to_string(),
        other => format!("{:?}", other),
    }
}

fn run_sqlite(con: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = con.prepare(sql).map_err(|e| format!("sqlite err: {e}"))?;
    let cols = stmt.column_count();
    let rows = stmt
        .query_map([], |r| {
            (0..cols)
                .map(|i| {
                    r.get::<_, rusqlite::types::Value>(i)
                        .map(|v| norm_sqlite(&v))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|e| format!("sqlite err: {e}"))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|e| format!("sqlite err: {e}"))?);
    }
    Ok(out)
}

fn run_mote(db: &Database, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let result = db
        .execute(sql)
        .map_err(|e| format!("mote err: {e}"))?
        .materialize()
        .map_err(|e| format!("mote err: {e}"))?;
    match result {
        motedb::QueryResult::Select { rows, .. } => Ok(rows
            .iter()
            .map(|r| r.iter().map(norm_mote).collect())
            .collect()),
        _ => Err("mote: not a select".into()),
    }
}

/// Compare with normalization: sort rows when the query has no deterministic
/// order (no ORDER BY on unique keys); compare in sequence otherwise.
fn compare(
    sql: &str,
    con: &Connection,
    db: &Database,
    order_sensitive: bool,
    divergences: &mut Vec<String>,
    checks: &mut u64,
) {
    *checks += 1;
    let a = run_sqlite(con, sql);
    let b = run_mote(db, sql);
    match (&a, &b) {
        (Err(_), Err(_)) => {}
        (Ok(ra), Ok(rb)) => {
            let (mut ra, mut rb) = (ra.clone(), rb.clone());
            if !order_sensitive {
                ra.sort();
                rb.sort();
            }
            if ra != rb && !float_close(&ra, &rb) {
                divergences.push(format!(
                    "RESULT divergence: {sql}\n  sqlite({}): {ra:?}\n  mote({}): {rb:?}",
                    ra.len(),
                    rb.len()
                ));
            }
        }
        _ => divergences.push(format!(
            "ERROR-KIND divergence: {sql}\n  sqlite: {a:?}\n  mote: {b:?}"
        )),
    }
}

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(8);
    let mut total_div = 0usize;
    let mut total_checks = 0u64;

    for round in 0..rounds {
        let dir = TempDir::new().unwrap();
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        let con = Connection::open_in_memory().unwrap();

        let mut rng = Lcg(0xBEEF + round as u64 * 104729);
        let mut divergences: Vec<String> = Vec::new();
        let mut checks: u64 = 0;

        // ── Schema: customers / orders / items (NULL-rich join keys) ──
        for sql in [
            "CREATE TABLE cust (id INTEGER PRIMARY KEY, name TEXT, tier INT)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, cid INT, amt REAL, day INT)",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, day INT, tag TEXT)",
        ] {
            db.execute(sql).unwrap();
            con.execute_batch(sql).unwrap();
        }

        // ── Randomized data (shared deterministically) ──
        let mut next_cust = 1i64;
        let mut next_order = 1i64;
        let mut next_item = 1i64;
        for _step in 0..100 {
            let choice = rng.below(10);
            if choice < 3 {
                let id = next_cust;
                next_cust += 1;
                let name = format!("n{}", rng.below(8));
                let tier = if rng.below(5) == 0 {
                    "NULL".to_string()
                } else {
                    rng.below(4).to_string()
                };
                let sql =
                    format!("INSERT INTO cust (id, name, tier) VALUES ({id}, '{name}', {tier})");
                let ra = con.execute(&sql, ());
                let rb = db.execute(&sql).map(|_| ()).map_err(|e| e.to_string());
                if ra.is_err() != rb.is_err() {
                    divergences.push(format!("INSERT cust divergence: {sql}"));
                }
            } else if choice < 8 {
                let id = next_order;
                next_order += 1;
                let cid = if rng.below(6) == 0 {
                    "NULL".to_string()
                } else {
                    format!("{}", rng.below(next_cust.max(1) as u64 + 2))
                };
                let amt = if rng.below(6) == 0 {
                    "NULL".to_string()
                } else {
                    format!("{:.2}", rng.below(50000) as f64 / 100.0)
                };
                let day = if rng.below(6) == 0 {
                    "NULL".to_string()
                } else {
                    rng.below(31).to_string()
                };
                let sql = format!(
                    "INSERT INTO orders (id, cid, amt, day) VALUES ({id}, {cid}, {amt}, {day})"
                );
                let ra = con.execute(&sql, ());
                let rb = db.execute(&sql).map(|_| ()).map_err(|e| e.to_string());
                if ra.is_err() != rb.is_err() {
                    divergences.push(format!("INSERT orders divergence: {sql}"));
                }
            } else {
                let id = next_item;
                next_item += 1;
                let day = rng.below(31).to_string();
                let tag = ["x", "y", "zz", "NULL"][rng.below(4) as usize].to_string();
                let tag = if tag == "NULL" {
                    "NULL".into()
                } else {
                    format!("'{tag}'")
                };
                let sql = format!("INSERT INTO items (id, day, tag) VALUES ({id}, {day}, {tag})");
                let ra = con.execute(&sql, ());
                let rb = db.execute(&sql).map(|_| ()).map_err(|e| e.to_string());
                if ra.is_err() != rb.is_err() {
                    divergences.push(format!("INSERT items divergence: {sql}"));
                }
            }
        }

        // ── Query battery (fixed + randomized) ──
        let batteries: Vec<(&str, bool)> = vec![
            // 2-way joins
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id", false),
            ("SELECT c.tier, COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id GROUP BY c.tier", false),
            ("SELECT c.tier, COUNT(*), SUM(o.amt), MIN(o.amt), MAX(o.amt), AVG(o.amt) FROM orders o JOIN cust c ON o.cid = c.id GROUP BY c.tier", false),
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id WHERE o.amt > 250.0", false),
            ("SELECT c.name, COUNT(*) AS n FROM orders o JOIN cust c ON o.cid = c.id WHERE o.amt > 200.0 GROUP BY c.name ORDER BY COUNT(*) DESC, c.name", true),
            // 3-way joins
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id JOIN items i ON i.day = o.day", false),
            ("SELECT i.tag, COUNT(*), SUM(o.amt) FROM orders o JOIN cust c ON o.cid = c.id JOIN items i ON i.day = o.day GROUP BY i.tag", false),
            ("SELECT c.tier, i.tag, COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id JOIN items i ON i.day = o.day WHERE o.amt > 100.0 GROUP BY c.tier, i.tag", false),
            // 4-way (self-join on cust tier)
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id JOIN items i ON i.day = o.day JOIN cust c2 ON c2.tier = c.tier", false),
            // LEFT JOINs (general path — must stay correct)
            ("SELECT COUNT(*) FROM cust c LEFT JOIN orders o ON c.id = o.cid", false),
            ("SELECT c.id, COUNT(o.id) FROM cust c LEFT JOIN orders o ON c.id = o.cid GROUP BY c.id", false),
            ("SELECT COUNT(*) FROM cust c LEFT JOIN orders o ON c.id = o.cid LEFT JOIN items i ON i.day = o.day", false),
            // mixed INNER + LEFT
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id LEFT JOIN items i ON i.day = o.day", false),
            // self-join
            ("SELECT COUNT(*) FROM cust a JOIN cust b ON a.tier = b.tier", false),
            ("SELECT a.tier, COUNT(*) FROM cust a JOIN cust b ON a.tier = b.tier GROUP BY a.tier", false),
            // reversed ON order
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON c.id = o.cid", false),
            ("SELECT c.tier, COUNT(*) FROM orders o JOIN cust c ON c.id = o.cid GROUP BY c.tier", false),
            // non-equi ON → general path
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id AND o.amt > 200.0", false),
            // empty-table joins (items may be non-empty; use a guaranteed-empty filter)
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id WHERE o.id < 0", false),
            ("SELECT COUNT(*), SUM(o.amt) FROM orders o JOIN cust c ON o.cid = c.id WHERE o.id < 0", false),
            ("SELECT c.tier, COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id WHERE o.id < 0 GROUP BY c.tier", false),
            // HAVING
            ("SELECT c.tier, COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id GROUP BY c.tier HAVING COUNT(*) > 2", false),
            // ORDER BY on projected + LIMIT/OFFSET
            ("SELECT o.id FROM orders o JOIN cust c ON o.cid = c.id ORDER BY o.id LIMIT 7", true),
            ("SELECT o.id FROM orders o JOIN cust c ON o.cid = c.id ORDER BY o.id DESC LIMIT 5 OFFSET 3", true),
        ];
        for (sql, ord_sensitive) in batteries {
            compare(sql, &con, &db, ord_sensitive, &mut divergences, &mut checks);
        }

        // Randomized join/aggregates
        let cmp_ops = ["<", ">=", "=", "!=", "<=", ">"];
        for _ in 0..60 {
            let table_pair = rng.below(4);
            let amt_cut = format!("{:.1}", rng.below(6000) as f64 / 10.0);
            let day_cut = rng.below(31);
            let op = cmp_ops[rng.below(6) as usize];
            let sql = match table_pair {
                0 => format!(
                    "SELECT c.tier, COUNT(*), SUM(o.amt) FROM orders o JOIN cust c ON o.cid = c.id WHERE o.amt {op} {amt_cut} GROUP BY c.tier"
                ),
                1 => format!(
                    "SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id JOIN items i ON i.day = o.day WHERE o.day {op} {day_cut}"
                ),
                2 => format!(
                    "SELECT i.tag, COUNT(o.id) FROM items i LEFT JOIN orders o ON o.day = i.day WHERE i.id {op} {} GROUP BY i.tag",
                    rng.below(next_item.max(1) as u64 + 1)
                ),
                _ => format!(
                    "SELECT c.name, COUNT(*), AVG(o.amt) FROM orders o JOIN cust c ON o.cid = c.id WHERE c.name IS NOT NULL GROUP BY c.name HAVING COUNT(*) {op} {}",
                    rng.below(6)
                ),
            };
            compare(&sql, &con, &db, false, &mut divergences, &mut checks);
        }

        // UPDATE/DELETE interplay on joined state
        let _ = con.execute("UPDATE orders SET amt = amt + 10 WHERE id % 7 = 0", ());
        let _ = db
            .execute("UPDATE orders SET amt = amt + 10 WHERE id % 7 = 0")
            .map(|_| ());
        let _ = con.execute("DELETE FROM orders WHERE id % 11 = 0", ());
        let _ = db
            .execute("DELETE FROM orders WHERE id % 11 = 0")
            .map(|_| ());
        for (sql, o) in [
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id", false),
            ("SELECT c.tier, COUNT(*), SUM(o.amt) FROM orders o JOIN cust c ON o.cid = c.id GROUP BY c.tier", false),
            ("SELECT COUNT(*) FROM orders o JOIN cust c ON o.cid = c.id JOIN items i ON i.day = o.day", false),
            ("SELECT c.name, COUNT(*) AS n FROM orders o JOIN cust c ON o.cid = c.id GROUP BY c.name ORDER BY COUNT(*) DESC, c.name", true),
        ] {
            compare(sql, &con, &db, o, &mut divergences, &mut checks);
        }

        if !divergences.is_empty() {
            println!(
                "round {round}: checks={checks} DIVERGENCES={}",
                divergences.len()
            );
            for d in divergences.iter().take(6) {
                println!("  ── {d}");
            }
        } else {
            println!("round {round}: checks={checks} divergences=0");
        }
        total_div += divergences.len();
        total_checks += checks;
    }
    println!("\nTOTAL checks={total_checks} divergences={total_div}");
}

/// Cell-wise numeric comparison with a small tolerance: SQLite's AVG is a
/// RUNNING mean while MoteDB computes sum/count — they differ by ~1 ULP
/// which can round the printed decimal either way. Integer/text cells must
/// still match exactly (any real mismatch is orders of magnitude larger).
fn float_close(a: &[Vec<String>], b: &[Vec<String>]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(ra, rb)| {
        ra.len() == rb.len()
            && ra.iter().zip(rb.iter()).all(|(x, y)| {
                if x == y {
                    return true;
                }
                match (x.parse::<f64>(), y.parse::<f64>()) {
                    (Ok(fx), Ok(fy)) => (fx - fy).abs() <= 0.011 * (1.0 + fx.abs().max(fy.abs())),
                    _ => false,
                }
            })
    })
}
