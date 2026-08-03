use motedb::types::Value;
use motedb::Database;
use motedb::QueryResult;
use tempfile::TempDir;
#[test]
fn dbg() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let r1 = db.execute("INSERT INTO t VALUES (1,10),(1,20)");
    println!(
        "batch dup PK: {:?}",
        r1.map(|_| ()).map_err(|e| e.to_string())
    );
    let r2 = db
        .execute("SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    match r2 {
        QueryResult::Select { rows, .. } => println!("rows after failed batch: {:?}", rows),
        _ => {}
    }
    let _ = Value::Integer(0);
}
