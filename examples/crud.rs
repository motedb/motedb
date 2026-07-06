//! MoteDB CRUD walkthrough — the full read/write/update/delete cycle plus a
//! crash-recovery round-trip (the two things every embedded-DB user wants to
//! see first).
//!
//! Run with:
//!   cargo run --example crud

use motedb::{Database, QueryResult};

fn count(db: &Database, sql: &str) -> usize {
    if let QueryResult::Select { rows, .. } = db.execute(sql).unwrap().materialize().unwrap() {
        rows.len()
    } else {
        0
    }
}

fn main() -> motedb::Result<()> {
    let path = std::env::temp_dir().join("motedb_crud_demo.mote");
    let _ = std::fs::remove_dir_all(&path);

    // ── Phase 1: create + populate, then close ─────────────────────────────
    {
        let db = Database::create(&path)?;

        db.execute(
            "CREATE TABLE sensors (
            id INT PRIMARY KEY AUTO_INCREMENT,
            location TEXT,
            reading FLOAT
        )",
        )?;

        // Batch via a single multi-row INSERT (fast path).
        let mut sql = String::from("INSERT INTO sensors (location, reading) VALUES ");
        for i in 0..1000 {
            let loc = if i % 2 == 0 { "roof" } else { "basement" };
            sql.push_str(&format!("('{}', {:.2}),", loc, i as f64 * 0.5));
        }
        sql.truncate(sql.len() - 1); // drop trailing comma
        db.execute(&sql)?;

        // UPDATE
        let updated = db.execute("UPDATE sensors SET reading = -1.0 WHERE location = 'roof'")?;
        println!("updated rows: {:?}", updated.affected_rows());

        // DELETE
        let deleted = db.execute("DELETE FROM sensors WHERE id <= 10")?;
        println!("deleted rows: {:?}", deleted.affected_rows());

        // SELECT with WHERE + aggregate
        println!(
            "roof sensors after update: {}",
            count(&db, "SELECT * FROM sensors WHERE reading = -1.0")
        );
        println!("rows remaining: {}", count(&db, "SELECT * FROM sensors"));

        // A clean close flushes the write buffer and stops background threads.
        db.close()?;
    }

    // ── Phase 2: reopen — data must survive ────────────────────────────────
    {
        let db = Database::open(&path)?;
        println!("rows after reopen: {}", count(&db, "SELECT * FROM sensors"));
        println!(
            "roof sensors after reopen: {}",
            count(&db, "SELECT * FROM sensors WHERE reading = -1.0")
        );
    }

    let _ = std::fs::remove_dir_all(&path);
    Ok(())
}
