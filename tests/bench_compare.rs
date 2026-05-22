//! MoteDB vs SQLite Comparative Benchmark
//!
//! Run: cargo test --release --test bench_compare -- --nocapture --test-threads=1

use motedb::{Database, types::Value, sql::QueryResult};
use std::time::Instant;

// ═══════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════

fn setup_motedb(name: &str) -> Database {
    let dir = format!("/tmp/motedb_bench_{}", name);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(format!("{}.mote", &dir));
    Database::create(&dir).unwrap()
}

fn setup_sqlite(name: &str) -> rusqlite::Connection {
    let path = format!("/tmp/motedb_bench_{}.sqlite", name);
    let _ = std::fs::remove_file(&path);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    conn
}

fn m_exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn fmt_dur(us: u128) -> String {
    if us < 1_000 { format!("{}µs", us) }
    else if us < 1_000_000 { format!("{:.1}ms", us as f64 / 1000.0) }
    else { format!("{:.2}s", us as f64 / 1_000_000.0) }
}

fn fmt_ops(count: usize, us: u128) -> String {
    if us == 0 { return "N/A".into(); }
    let ops = count as f64 / (us as f64 / 1_000_000.0);
    if ops >= 1_000_000.0 { format!("{:.1}M ops/s", ops / 1_000_000.0) }
    else if ops >= 1000.0 { format!("{:.1}K ops/s", ops / 1000.0) }
    else { format!("{:.0} ops/s", ops) }
}

fn fmt_lat(us: u128) -> String {
    if us < 1000 { format!("{}µs", us) }
    else { format!("{:.1}µs", us as f64) }
}

fn divider() {
    println!("├──────────────────────────────┼──────────────────────┼──────────────────────┤");
}

fn header(title: &str) {
    println!("┌──────────────────────────────┩");
    println!("│  {}",
        if title.len() > 80 { &title[..80] } else { title });
    // recalculate with padding
    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  Benchmark                   │  MoteDB              │  SQLite WAL          │");
    divider();
}

fn row(label: &str, mote: &str, sqlite: &str) {
    println!("│  {:<28}│  {:<20}│  {:<20}│", label, mote, sqlite);
}

fn footer() {
    println!("└──────────────────────────────┴──────────────────────┴──────────────────────┘\n");
}

fn ratio(mote_us: u128, sqlite_us: u128) -> String {
    if sqlite_us == 0 { return "N/A".into(); }
    let r = mote_us as f64 / sqlite_us as f64;
    if r < 1.0 { format!("{:.1}x faster", 1.0 / r) }
    else { format!("{:.1}x slower", r) }
}

// ═══════════════════════════════════════════════════════════════
// Benchmark
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_motedb_vs_sqlite() {
    let n: usize = if std::env::var("CI").is_ok() { 5_000 } else { 10_000 };

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║     MoteDB vs SQLite WAL  —  Comparative Benchmark Report      ║");
    println!("║     Dataset: {} rows  ·  Release build  ·  MacBook             ║", n);
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // ═══════════════════════════════════════════════════
    // 1. INSERT THROUGHPUT (prepared statement)
    // ═══════════════════════════════════════════════════
    let (mote_insert, sqlite_insert) = {
        // MoteDB
        let db = setup_motedb("ins");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT, score FLOAT)").unwrap();
        db.execute("CREATE INDEX idx_age ON t (age)").unwrap();

        let t = Instant::now();
        for i in 0..n {
            db.execute_prepared(
                "INSERT INTO t (id, name, age, score) VALUES (?, ?, ?, ?)",
                vec![Value::Integer(i as i64), Value::text(format!("user_{}", i)),
                     Value::Integer(20 + (i as i64 % 50)), Value::Float(50.0 + (i as f64 % 100.0))],
            ).unwrap();
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        // SQLite
        let conn = setup_sqlite("ins");
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, score REAL);
             CREATE INDEX idx_age ON t(age);"
        ).unwrap();
        let mut stmt = conn.prepare("INSERT INTO t (id, name, age, score) VALUES (?, ?, ?, ?)").unwrap();

        let t = Instant::now();
        for i in 0..n {
            stmt.execute(rusqlite::params![i as i64, format!("user_{}", i), 20 + (i as i64 % 50), 50.0 + (i as f64 % 100.0)]).unwrap();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(stmt); drop(conn);

        (mote_us, sqlite_us)
    };

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  1. INSERT (prepared, +idx)  │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Rows", &n.to_string(), &n.to_string());
    row("Total time", &fmt_dur(mote_insert), &fmt_dur(sqlite_insert));
    row("Throughput", &fmt_ops(n, mote_insert), &fmt_ops(n, sqlite_insert));
    row("Avg latency", &fmt_lat(mote_insert / n as u128), &fmt_lat(sqlite_insert / n as u128));
    row("Comparison", &ratio(mote_insert, sqlite_insert), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 2. RAW SQL INSERT (no prepared statement)
    // ═══════════════════════════════════════════════════
    let (mote_raw_ins, sqlite_raw_ins) = {
        let ins_n = n.min(5_000);

        let db = setup_motedb("raw_ins");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)").unwrap();

        let t = Instant::now();
        for i in 0..ins_n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', {})", i, i, i)).unwrap();
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("raw_ins");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();

        let t = Instant::now();
        for i in 0..ins_n {
            conn.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', {})", i, i, i), []).unwrap();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us)
    };
    let raw_n = n.min(5_000);

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  2. RAW SQL INSERT (no prep) │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Rows", &raw_n.to_string(), &raw_n.to_string());
    row("Total time", &fmt_dur(mote_raw_ins), &fmt_dur(sqlite_raw_ins));
    row("Throughput", &fmt_ops(raw_n, mote_raw_ins), &fmt_ops(raw_n, sqlite_raw_ins));
    row("Comparison", &ratio(mote_raw_ins, sqlite_raw_ins), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 3. PK POINT QUERY (raw SQL)
    // ═══════════════════════════════════════════════════
    let (mote_pk, sqlite_pk) = {
        let pk_n = 2_000;

        let db = setup_motedb("pk");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', 25)", i, i)).unwrap();
        }

        let t = Instant::now();
        for i in 0..pk_n {
            let _ = m_exec(&db, &format!("SELECT * FROM t WHERE id = {}", i * 3 % n));
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("pk");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', 25)", i, i), []).unwrap();
            }
            tx.commit().unwrap();
        }

        let t = Instant::now();
        for i in 0..pk_n {
            let _: String = conn.query_row(
                &format!("SELECT name FROM t WHERE id = {}", i * 3 % n), [], |r| r.get(0)
            ).unwrap();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us)
    };
    let pk_q = 2_000;

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  3. PK POINT QUERY (raw SQL) │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Queries", &pk_q.to_string(), &pk_q.to_string());
    row("Total time", &fmt_dur(mote_pk), &fmt_dur(sqlite_pk));
    row("QPS", &fmt_ops(pk_q, mote_pk), &fmt_ops(pk_q, sqlite_pk));
    row("Avg latency", &fmt_lat(mote_pk / pk_q as u128), &fmt_lat(sqlite_pk / pk_q as u128));
    row("Comparison", &ratio(mote_pk, sqlite_pk), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 4. PK POINT QUERY (prepared statement)
    // ═══════════════════════════════════════════════════
    let (mote_ppk, sqlite_ppk) = {
        let ppk_n = 10_000;

        let db = setup_motedb("ppk");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', {})", i, i, i * 10)).unwrap();
        }
        // warm up prepared stmt cache
        for i in 0..100 {
            let _ = db.execute_prepared("SELECT * FROM t WHERE id = ?", vec![Value::Integer(i)]).unwrap().materialize().unwrap();
        }

        let t = Instant::now();
        for i in 0..ppk_n {
            let _ = db.execute_prepared("SELECT * FROM t WHERE id = ?", vec![Value::Integer((i % n) as i64)]).unwrap().materialize().unwrap();
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("ppk");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', {})", i, i, i * 10), []).unwrap();
            }
            tx.commit().unwrap();
        }
        let mut stmt = conn.prepare("SELECT name, val FROM t WHERE id = ?").unwrap();

        let t = Instant::now();
        for i in 0..ppk_n {
            let _: (String, i64) = stmt.query_row(rusqlite::params![((i % n) as i64)], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(stmt); drop(conn);

        (mote_us, sqlite_us)
    };
    let ppk_q = 10_000;

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  4. PK QUERY (prepared stmt) │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Queries", &ppk_q.to_string(), &ppk_q.to_string());
    row("Total time", &fmt_dur(mote_ppk), &fmt_dur(sqlite_ppk));
    row("QPS", &fmt_ops(ppk_q, mote_ppk), &fmt_ops(ppk_q, sqlite_ppk));
    row("Avg latency", &fmt_lat(mote_ppk / ppk_q as u128), &fmt_lat(sqlite_ppk / ppk_q as u128));
    row("Comparison", &ratio(mote_ppk, sqlite_ppk), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 5. COLUMN INDEX POINT QUERY
    // ═══════════════════════════════════════════════════
    let (mote_idx, sqlite_idx) = {
        let idx_q = 2_000;

        let db = setup_motedb("idx");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)").unwrap();
        db.execute("CREATE INDEX idx_age ON t (age)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', {})", i, i, 20 + (i as i64 % 50))).unwrap();
        }
        db.wait_for_indexes_ready();

        let t = Instant::now();
        for i in 0..idx_q {
            let _ = m_exec(&db, &format!("SELECT * FROM t WHERE age = {}", 30 + (i as i64 % 20)));
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("idx");
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             CREATE INDEX idx_age ON t(age);"
        ).unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, 'u{}', {})", i, i, 20 + (i as i64 % 50)), []).unwrap();
            }
            tx.commit().unwrap();
        }

        let t = Instant::now();
        for i in 0..idx_q {
            let _ = conn.query_row(
                &format!("SELECT name FROM t WHERE age = {}", 30 + (i as i64 % 20)), [], |r| r.get::<_, String>(0)
            ).ok();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us)
    };
    let idx_q = 2_000;

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  5. COLUMN INDEX (age = ?)   │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Queries", &idx_q.to_string(), &idx_q.to_string());
    row("Total time", &fmt_dur(mote_idx), &fmt_dur(sqlite_idx));
    row("QPS", &fmt_ops(idx_q, mote_idx), &fmt_ops(idx_q, sqlite_idx));
    row("Comparison", &ratio(mote_idx, sqlite_idx), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 6. RANGE QUERY (two-sided)
    // ═══════════════════════════════════════════════════
    let (mote_range, sqlite_range, range_q) = {
        let range_q = 1_000;

        let db = setup_motedb("range");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute("CREATE INDEX idx_val ON t (val)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 100)).unwrap();
        }
        db.wait_for_indexes_ready();

        let t = Instant::now();
        for i in 0..range_q {
            let v = i as i64 % 40;
            let _ = m_exec(&db, &format!("SELECT * FROM t WHERE val >= {} AND val <= {}", v, v + 10));
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("range");
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);
             CREATE INDEX idx_val ON t(val);"
        ).unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 100), []).unwrap();
            }
            tx.commit().unwrap();
        }

        let t = Instant::now();
        for i in 0..range_q {
            let v = i as i64 % 40;
            let mut stmt = conn.prepare(&format!("SELECT id FROM t WHERE val >= {} AND val <= {}", v, v + 10)).unwrap();
            let rows: Vec<i64> = stmt.query_map([], |r| r.get(0)).unwrap().filter_map(|r| r.ok()).collect();
            let _ = rows.len();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us, range_q)
    };

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  6. RANGE QUERY (val>=?<=?)  │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Queries", &range_q.to_string(), &range_q.to_string());
    row("Total time", &fmt_dur(mote_range), &fmt_dur(sqlite_range));
    row("QPS", &fmt_ops(range_q, mote_range), &fmt_ops(range_q, sqlite_range));
    row("Comparison", &ratio(mote_range, sqlite_range), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 7. SINGLE-SIDED RANGE QUERY (val > ?)
    // ═══════════════════════════════════════════════════
    let (mote_ss, sqlite_ss, ss_q) = {
        let ss_q = 1_000;

        let db = setup_motedb("ss_range");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute("CREATE INDEX idx_val ON t (val)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 100)).unwrap();
        }
        db.wait_for_indexes_ready();

        let t = Instant::now();
        for i in 0..ss_q {
            let v = i as i64 % 80;
            let _ = m_exec(&db, &format!("SELECT * FROM t WHERE val > {}", v));
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("ss_range");
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);
             CREATE INDEX idx_val ON t(val);"
        ).unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 100), []).unwrap();
            }
            tx.commit().unwrap();
        }

        let t = Instant::now();
        for i in 0..ss_q {
            let v = i as i64 % 80;
            let mut stmt = conn.prepare(&format!("SELECT id FROM t WHERE val > {}", v)).unwrap();
            let rows: Vec<i64> = stmt.query_map([], |r| r.get(0)).unwrap().filter_map(|r| r.ok()).collect();
            let _ = rows.len();
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us, ss_q)
    };

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  7. RANGE QUERY (val > ?)    │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Queries", &ss_q.to_string(), &ss_q.to_string());
    row("Total time", &fmt_dur(mote_ss), &fmt_dur(sqlite_ss));
    row("QPS", &fmt_ops(ss_q, mote_ss), &fmt_ops(ss_q, sqlite_ss));
    row("Comparison", &ratio(mote_ss, sqlite_ss), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 8. FULL TABLE SCAN
    // ═══════════════════════════════════════════════════
    let (mote_scan, sqlite_scan) = {
        let db = setup_motedb("scan");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, data TEXT)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'data_{}')", i, i)).unwrap();
        }

        let t = Instant::now();
        let result = m_exec(&db, "SELECT * FROM t");
        let mote_rows = match result { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("scan");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)").unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, 'data_{}')", i, i), []).unwrap();
            }
            tx.commit().unwrap();
        }

        let t = Instant::now();
        let sqlite_rows = {
            let mut stmt = conn.prepare("SELECT id, data FROM t").unwrap();
            let rows: Vec<(i64, String)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().filter_map(|r| r.ok()).collect();
            rows.len()
        };
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        ((mote_us, mote_rows), (sqlite_us, sqlite_rows))
    };

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  8. FULL TABLE SCAN          │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Rows", &mote_scan.1.to_string(), &sqlite_scan.1.to_string());
    row("Total time", &fmt_dur(mote_scan.0), &fmt_dur(sqlite_scan.0));
    row("Throughput", &fmt_ops(mote_scan.1, mote_scan.0), &fmt_ops(sqlite_scan.1, sqlite_scan.0));
    row("Comparison", &ratio(mote_scan.0, sqlite_scan.0), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 9. MIXED CRUD
    // ═══════════════════════════════════════════════════
    let ops = 1_000.min(n);
    let (mote_crud, sqlite_crud) = {

        let db = setup_motedb("crud");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute("CREATE INDEX idx_val ON t (val)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
        }

        let t = Instant::now();
        for i in 0..ops {
            match i % 10 {
                0..=4 => { let _ = m_exec(&db, &format!("SELECT * FROM t WHERE id = {}", i)); }
                5..=6 => { let _ = m_exec(&db, &format!("UPDATE t SET val = {} WHERE id = {}", i * 100, i)); }
                7..=8 => { let _ = m_exec(&db, &format!("SELECT * FROM t WHERE val = {}", i * 100)); }
                9 => {
                    let rid = n + i;
                    let _ = m_exec(&db, &format!("INSERT INTO t VALUES ({}, {})", rid, rid));
                    let _ = m_exec(&db, &format!("DELETE FROM t WHERE id = {}", rid));
                }
                _ => {}
            }
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        let conn = setup_sqlite("crud");
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);
             CREATE INDEX idx_val ON t(val);"
        ).unwrap();
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10), []).unwrap();
            }
            tx.commit().unwrap();
        }

        let t = Instant::now();
        for i in 0..ops {
            match i % 10 {
                0..=4 => { let _ = conn.query_row(&format!("SELECT val FROM t WHERE id = {}", i), [], |r| r.get::<_, i64>(0)); }
                5..=6 => { conn.execute(&format!("UPDATE t SET val = {} WHERE id = {}", i * 100, i), []).unwrap(); }
                7..=8 => { let _ = conn.query_row(&format!("SELECT id FROM t WHERE val = {}", i * 100), [], |r| r.get::<_, i64>(0)); }
                9 => {
                    let rid = n + i;
                    conn.execute(&format!("INSERT INTO t VALUES ({}, {})", rid, rid), []).unwrap();
                    conn.execute(&format!("DELETE FROM t WHERE id = {}", rid), []).unwrap();
                }
                _ => {}
            }
        }
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us)
    };

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  9. MIXED CRUD (50/20/20/10) │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Operations", &ops.to_string(), &ops.to_string());
    row("Total time", &fmt_dur(mote_crud), &fmt_dur(sqlite_crud));
    row("Throughput", &fmt_ops(ops, mote_crud), &fmt_ops(ops, sqlite_crud));
    row("Comparison", &ratio(mote_crud, sqlite_crud), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 10. WAL RECOVERY
    // ═══════════════════════════════════════════════════
    let (mote_recovery, sqlite_recovery) = {
        // MoteDB
        let dir = "/tmp/motedb_bench_recovery";
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(format!("{}.mote", dir));
        {
            let db = Database::create(dir).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
            for i in 0..n {
                db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
            }
            db.flush().unwrap();
            db.close().unwrap();
        }
        let t = Instant::now();
        let db = Database::open(dir).unwrap();
        let _ = m_exec(&db, "SELECT COUNT(*) FROM t");
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        // SQLite
        let path = "/tmp/motedb_bench_recovery.sqlite";
        let _ = std::fs::remove_file(path);
        {
            let conn = rusqlite::Connection::open(path).unwrap();
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..n {
                tx.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i), []).unwrap();
            }
            tx.commit().unwrap();
            drop(conn);
        }
        let t = Instant::now();
        let conn = rusqlite::Connection::open(path).unwrap();
        let _: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
        let sqlite_us = t.elapsed().as_micros();
        drop(conn);

        (mote_us, sqlite_us)
    };

    println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
    println!("│  10. WAL RECOVERY            │  MoteDB              │  SQLite WAL          │");
    divider();
    row("Rows", &n.to_string(), &n.to_string());
    row("Recovery time", &fmt_dur(mote_recovery), &fmt_dur(sqlite_recovery));
    row("Comparison", &ratio(mote_recovery, sqlite_recovery), "");
    footer();

    // ═══════════════════════════════════════════════════
    // 11. FULL-TEXT SEARCH (MoteDB only — no direct SQLite equivalent)
    // ═══════════════════════════════════════════════════
    {
        let db = setup_motedb("fts");
        db.execute("CREATE TABLE docs (id INT, content TEXT)").unwrap();
        let fts_n = n / 2;
        let words = ["database", "vector", "search", "index", "query", "performance", "rust", "embedded", "columnar", "spatial"];
        for i in 0..fts_n {
            let w1 = words[i % words.len()];
            let w2 = words[(i + 3) % words.len()];
            let content = format!("This is document {} about {} and {} technology systems.", i, w1, w2);
            db.execute(&format!("INSERT INTO docs (id, content) VALUES ({}, '{}')", i, content)).unwrap();
        }
        db.execute("CREATE TEXT INDEX idx_content ON docs(content)").unwrap();

        let fts_q = 500.min(fts_n);
        let t = Instant::now();
        for i in 0..fts_q {
            let term = words[i % words.len()];
            let _ = m_exec(&db, &format!("SELECT id FROM docs WHERE MATCH(content, '{}') ORDER BY id", term));
        }
        let mote_us = t.elapsed().as_micros();
        db.close().unwrap();

        println!("┌─────────────────────────────────────────────────────────────────┐");
        println!("│  11. FULL-TEXT SEARCH — MoteDB only (no SQLite FTS equiv)       │");
        println!("├─────────────────────┬───────────────────────────────────────────┤");
        println!("│  Documents          │  {:>12}                            │", fts_n);
        println!("│  Queries            │  {:>12}                            │", fts_q);
        println!("│  Total time         │  {:>12}                           │", fmt_dur(mote_us));
        println!("│  QPS                │  {:>12}                         │", fmt_ops(fts_q, mote_us));
        println!("│  Avg latency        │  {:>12}                           │", fmt_lat(mote_us / fts_q as u128));
        println!("└─────────────────────┴───────────────────────────────────────────┘\n");
    }

    // ═══════════════════════════════════════════════════
    // 12. PREPARED vs RAW (MoteDB speedup factor)
    // ═══════════════════════════════════════════════════
    let (mote_raw_q, mote_prep_q) = {
        let q = 2_000.min(n);

        let db = setup_motedb("prep_vs_raw");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").unwrap();
        for i in 0..n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'val_{}')", i, i)).unwrap();
        }

        // Raw SQL
        let t = Instant::now();
        for i in 0..q {
            let _ = m_exec(&db, &format!("SELECT * FROM t WHERE id = {}", i % n));
        }
        let raw_us = t.elapsed().as_micros();

        // Prepared (warmed)
        let sql = "SELECT * FROM t WHERE id = ?";
        for i in 0..100 {
            let _ = db.execute_prepared(sql, vec![Value::Integer(i as i64)]).unwrap().materialize().unwrap();
        }

        let t = Instant::now();
        for i in 0..q {
            let _ = db.execute_prepared(sql, vec![Value::Integer((i % n) as i64)]).unwrap().materialize().unwrap();
        }
        let prep_us = t.elapsed().as_micros();

        db.close().unwrap();
        (raw_us, prep_us)
    };
    let prep_q = 2_000.min(n);

    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│  12. PREPARED vs RAW (MoteDB speedup factor)                    │");
    println!("├─────────────────────┬───────────────────────────────────────────┤");
    println!("│  Queries            │  {:>12}                            │", prep_q);
    println!("│  Raw SQL            │  {:>12}  ({:>10})           │", fmt_dur(mote_raw_q), fmt_ops(prep_q, mote_raw_q));
    println!("│  Prepared stmt      │  {:>12}  ({:>10})           │", fmt_dur(mote_prep_q), fmt_ops(prep_q, mote_prep_q));
    println!("│  Speedup            │  {:>12.1}x                            │", mote_raw_q as f64 / mote_prep_q.max(1) as f64);
    println!("└─────────────────────┴───────────────────────────────────────────┘\n");

    // ═══════════════════════════════════════════════════
    // 13. DISK USAGE
    // ═══════════════════════════════════════════════════
    {
        let du_n = n;

        let db = setup_motedb("disk");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT, score FLOAT)").unwrap();
        for i in 0..du_n {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'user_{}', {}, {})",
                i, i, 20 + (i as i64 % 50), 50.0 + (i as f64 % 100.0))).unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();

        let mote_size: u64 = walkdir_size("/tmp/motedb_bench_disk");

        let conn = setup_sqlite("disk");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, score REAL)").unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..du_n {
            tx.execute(&format!("INSERT INTO t VALUES ({}, 'user_{}', {}, {})",
                i, i, 20 + (i as i64 % 50), 50.0 + (i as f64 % 100.0)), []).unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);

        let sqlite_size = std::fs::metadata("/tmp/motedb_bench_disk.sqlite")
            .map(|m| m.len()).unwrap_or(0);

        let mote_per_row = if du_n > 0 { mote_size / du_n as u64 } else { 0 };
        let sqlite_per_row = if du_n > 0 { sqlite_size / du_n as u64 } else { 0 };

        println!("┌──────────────────────────────┬──────────────────────┬──────────────────────┐");
        println!("│  13. DISK USAGE              │  MoteDB              │  SQLite WAL          │");
        divider();
        row("Rows", &du_n.to_string(), &du_n.to_string());
        row("Total size", &fmt_bytes(mote_size), &fmt_bytes(sqlite_size));
        row("Per row", &fmt_bytes(mote_per_row), &fmt_bytes(sqlite_per_row));
        row("Ratio",
            &format!("{:.1}x", mote_size as f64 / sqlite_size.max(1) as f64),
            "1.0x (baseline)");
        println!("└──────────────────────────────┴──────────────────────┴──────────────────────┘\n");
    }

    // ═══════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                      Summary                                    ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  INSERT (prepared):   {:>12} vs {:>12}              ║",
        fmt_ops(n, mote_insert), fmt_ops(n, sqlite_insert));
    println!("║  PK Query (raw):      {:>12} vs {:>12}              ║",
        fmt_ops(pk_q, mote_pk), fmt_ops(pk_q, sqlite_pk));
    println!("║  PK Query (prepared): {:>12} vs {:>12}              ║",
        fmt_ops(ppk_q, mote_ppk), fmt_ops(ppk_q, sqlite_ppk));
    println!("║  Column Index:        {:>12} vs {:>12}              ║",
        fmt_ops(idx_q, mote_idx), fmt_ops(idx_q, sqlite_idx));
    println!("║  Range Query (>=,<=): {:>12} vs {:>12}              ║",
        fmt_ops(range_q, mote_range), fmt_ops(range_q, sqlite_range));
    println!("║  Range Query (>):     {:>12} vs {:>12}              ║",
        fmt_ops(ss_q, mote_ss), fmt_ops(ss_q, sqlite_ss));
    println!("║  Full Scan:           {:>12} vs {:>12}              ║",
        fmt_ops(mote_scan.1, mote_scan.0), fmt_ops(sqlite_scan.1, sqlite_scan.0));
    println!("║  Mixed CRUD:          {:>12} vs {:>12}              ║",
        fmt_ops(ops, mote_crud), fmt_ops(ops, sqlite_crud));
    println!("║  WAL Recovery:        {:>12} vs {:>12}              ║",
        fmt_dur(mote_recovery), fmt_dur(sqlite_recovery));
    println!("╚══════════════════════════════════════════════════════════════════╝\n");
}

fn walkdir_size(path: &str) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() { total += meta.len(); }
            }
        }
    }
    // Also check .mote directory
    let mote_path = format!("{}.mote", path);
    if let Ok(entries) = std::fs::read_dir(mote_path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() { total += meta.len(); }
            }
        }
    }
    total
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes < 1024 { format!("{} B", bytes) }
    else if bytes < 1024 * 1024 { format!("{:.1} KB", bytes as f64 / 1024.0) }
    else { format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0)) }
}
