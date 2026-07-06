//! Minimal MoteDB "hello world" — create a database, insert, and query.
//!
//! Run with:
//!   cargo run --example hello_world
//!
//! This mirrors the Quick Start in the crate-level docs / README and is the
//! smallest example that exercises CREATE / INSERT / SELECT end to end.

use motedb::{Database, QueryResult};

fn main() -> motedb::Result<()> {
    // Database::create makes a fresh database at the given path
    // (a directory holding the columnar segments + WAL sidecars).
    // The example cleans up any previous run so it's re-runnable.
    let _ = std::fs::remove_dir_all("hello.mote");
    let db = Database::create("hello.mote")?;

    // Standard SQL DDL/DML. (CREATE TABLE IF NOT EXISTS is not yet supported —
    // the examples always start from a fresh path, so plain CREATE is fine.)
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")?;
    db.execute("INSERT INTO users VALUES (1, 'Ada', 36)")?;
    db.execute("INSERT INTO users VALUES (2, 'Linus', 54)")?;
    db.execute("INSERT INTO users VALUES (3, 'Grace', 85)")?;

    // A SELECT returns a streaming result; materialize() collects it.
    let result = db.execute("SELECT name, age FROM users WHERE age > 40 ORDER BY age")?;
    if let QueryResult::Select { columns, rows } = result.materialize()? {
        println!("{:?}", columns);
        for row in &rows {
            println!("{:?}", row);
        }
        // rows: [[Linus, 54], [Grace, 85]]
    }

    // Aggregate queries work too.
    let count = db.execute("SELECT COUNT(*) FROM users")?.materialize()?;
    if let QueryResult::Select { rows, .. } = count {
        println!("total users: {:?}", rows[0][0]);
    }

    Ok(())
}
