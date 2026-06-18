use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, region TEXT)").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Alice', 1.0, 'US')").unwrap();
    db.execute("INSERT INTO t (name, val, region) VALUES ('Bob', 2.0, 'EU')").unwrap();

    let count_before = db.execute("SELECT * FROM t").unwrap().materialize().unwrap().row_count();
    eprintln!("[debug] count before delete: {}", count_before);

    db.execute("DELETE FROM t WHERE name = 'Alice'").unwrap();

    let count_after = db.execute("SELECT * FROM t").unwrap().materialize().unwrap().row_count();
    eprintln!("[debug] count after delete: {} (expect 1)", count_after);

    // Also test with WHERE
    let r = db.execute("SELECT * FROM t WHERE name = 'Bob'").unwrap().materialize().unwrap();
    eprintln!("[debug] WHERE name='Bob': {} rows", r.row_count());
    println!("DONE");
}
