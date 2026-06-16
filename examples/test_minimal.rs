use motedb::{Database};
use tempfile::TempDir;
fn main() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT, val INT)").unwrap();
    for i in 0..20i64 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'tag_{}', {})", i, i % 5, i)).unwrap();
    }
    db.execute("CREATE INDEX idx_tag ON t (tag) USING COLUMN").unwrap();
    db.flush().unwrap();
    let r = db.execute("SELECT * FROM t").unwrap().materialize().unwrap();
    eprintln!("SELECT *: {} rows", match &r { motedb::QueryResult::Select { rows, .. } => rows.len(), _ => 0 });
    println!("SUCCESS");
}
