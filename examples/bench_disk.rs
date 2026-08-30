//! Disk-usage audit: per-component on-disk size, bytes/row, SQLite anchor,
//! VACUUM reclaim.
use motedb::Database;
use std::path::Path;
use std::time::Instant;

fn dir_size(p: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let md = e.metadata();
            if let Ok(m) = md {
                if m.is_dir() {
                    total += dir_size(&e.path());
                } else {
                    total += m.len();
                }
            }
        }
    }
    total
}

fn breakdown(root: &Path) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(root) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Ok(m) = e.metadata() {
                if m.is_dir() {
                    out.push((format!("{name}/"), dir_size(&e.path())));
                } else {
                    out.push((name, m.len()));
                }
            }
        }
    }
    out.sort_by(|a, b| b.1.cmp(&a.1));
    out
}

fn main() {
    // 500K rows × 4 cols (id INT, user INT, kind INT, val REAL)
    let root = std::env::temp_dir().join(format!("motedb_disk_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let db = Database::create(&root).unwrap();
    // Database::create appends .mote — measure the real directory
    let root = root.with_extension("mote");
    println!("db dir: {}", root.display());
    db.execute("CREATE TABLE events (id INT PRIMARY KEY, user_id INT, kind INT, val REAL)")
        .unwrap();
    let t = Instant::now();
    for c in 0..500 {
        let vals: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "({}, {}, {}, {:.2})",
                    c * 1000 + i,
                    (i * 31 + c) % 50000,
                    i % 7,
                    (i as f64) * 0.5
                )
            })
            .collect();
        db.execute(&format!("INSERT INTO events VALUES {}", vals.join(", ")))
            .unwrap();
    }
    println!("insert 500K: {:?}", t.elapsed());
    db.checkpoint().unwrap();

    let mote = dir_size(&root);
    println!(
        "\nMoteDB 500K×4cols total: {:.2} MB ({:.1} B/row)",
        mote as f64 / 1e6,
        mote as f64 / 500_000.0
    );
    for (name, sz) in breakdown(&root).into_iter().take(8) {
        println!("  {:<24} {:>10.2} MB", name, sz as f64 / 1e6);
    }

    // raw payload estimate: 8+8+8+8 = 32B/row binary; CSV-ish text ~35B
    println!("  (raw binary payload ≈ 32 B/row)");

    // VACUUM reclaim
    let t = Instant::now();
    db.vacuum().unwrap();
    let after_vac = dir_size(&root);
    println!(
        "\nafter VACUUM: {:.2} MB ({:.1} B/row) in {:?}",
        after_vac as f64 / 1e6,
        after_vac as f64 / 500_000.0,
        t.elapsed()
    );

    // churn experiment: 200K updates (=tombstone+append) then half-deletes
    let t = Instant::now();
    for i in 0..200_000i64 {
        db.execute(&format!(
            "UPDATE events SET val = val + 1 WHERE id = {}",
            (i * 7) % 500_000
        ))
        .unwrap();
    }
    let after_upd = dir_size(&root);
    println!(
        "\nafter 200K updates: {:.2} MB ({:.1} B/row) {:?}",
        after_upd as f64 / 1e6,
        after_upd as f64 / 500_000.0,
        t.elapsed()
    );

    for i in 0..250_000i64 {
        db.execute(&format!("DELETE FROM events WHERE id = {}", i * 2))
            .unwrap();
    }
    let after_del = dir_size(&root);
    println!(
        "after delete 250K rows: {:.2} MB ({:.1} B/row live 250K = {:.1} B/live-row)",
        after_del as f64 / 1e6,
        after_del as f64 / 500_000.0,
        after_del as f64 / 250_000.0
    );

    let t = Instant::now();
    db.vacuum().unwrap();
    let reclaimed = dir_size(&root);
    println!(
        "after VACUUM: {:.2} MB ({:.1} B/live-row) in {:?}",
        reclaimed as f64 / 1e6,
        reclaimed as f64 / 250_000.0,
        t.elapsed()
    );
    println!(
        "VACUUM reclaimed {:.1}% of pre-vacuum bytes",
        (1.0 - reclaimed as f64 / after_del as f64) * 100.0
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&root);

    // SQLite anchor, same data
    let sq_path =
        std::env::temp_dir().join(format!("motedb_disk_sqlite_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&sq_path);
    let conn = rusqlite::Connection::open(&sq_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, kind INTEGER, val REAL);").unwrap();
    let t = Instant::now();
    for c in 0..500 {
        let mut stmt = conn.prepare("INSERT INTO events VALUES (?,?,?,?)").unwrap();
        for i in 0..1000 {
            let id = c * 1000 + i;
            stmt.execute(rusqlite::params![
                id,
                (i * 31 + c) % 50000,
                i % 7,
                (i as f64) * 0.5
            ])
            .unwrap();
        }
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); VACUUM;")
        .unwrap();
    let sq = std::fs::metadata(&sq_path).unwrap().len();
    println!(
        "\nSQLite 500K same schema: {:.2} MB ({:.1} B/row) insert {:?}",
        sq as f64 / 1e6,
        sq as f64 / 500_000.0,
        t.elapsed()
    );
    drop(conn);
    let _ = std::fs::remove_file(&sq_path);
    let _ = std::fs::remove_file(sq_path.with_extension("db-wal"));
    let _ = std::fs::remove_file(sq_path.with_extension("db-shm"));
}
