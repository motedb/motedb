use motedb::Database;
use tempfile::TempDir;
fn main() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    println!("reopening");
    let db = Database::open(&path).unwrap();
    println!("opened, now insert");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    let r = db.execute("INSERT INTO t VALUES (2, 20)");
    println!("insert result: {:?}", r.is_ok());
    db.close().unwrap();
    println!("done");
}
