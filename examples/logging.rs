//! Demonstrates MoteDB's observability via the `log` crate.
//!
//! MoteDB emits lifecycle/durability events (WAL flush, checkpoint, errors)
//! through the `log` facade. The library installs NO logger itself — the
//! application chooses one. Here we use `env_logger`.
//!
//! Run with:
//!   RUST_LOG=motedb=info cargo run --example logging
//!
//! You'll see lines like:
//!   [info  motedb] [WAL] shutdown: gc=true flush=true
//! as the database opens/closes.

use motedb::{Database, QueryResult};

fn main() -> motedb::Result<()> {
    // Install a logger. env_logger reads RUST_LOG to set the level per module.
    // With no RUST_LOG set, nothing prints (zero overhead) — production default.
    // With RUST_LOG=motedb=info you get lifecycle events; =debug adds hot-path
    // detail; =warn shows degraded-but-functional conditions.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("off"))
        .format_timestamp_millis()
        .try_init();

    let _ = std::fs::remove_dir_all("logging_demo.mote");
    let db = Database::create("logging_demo.mote")?;

    db.execute("CREATE TABLE sensors (id INT PRIMARY KEY, v FLOAT)")?;
    db.execute("INSERT INTO sensors VALUES (1, 21.5)")?;
    db.execute("INSERT INTO sensors VALUES (2, 22.0)")?;

    let r = db.execute("SELECT * FROM sensors ORDER BY v")?;
    if let QueryResult::Select { rows, .. } = r.materialize()? {
        println!("rows: {:?}", rows.len());
    }

    // A checkpoint forces the WAL to flush — emits an info-level event when a
    // logger is installed.
    db.checkpoint()?;
    // close() stops background threads and does a final sync; the WAL shutdown
    // line is an info-level log.
    db.close()?;
    let _ = std::fs::remove_dir_all("logging_demo.mote");
    Ok(())
}
