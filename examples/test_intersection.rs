//! Debug AND query — check if this is a pre-existing bug.
use motedb::{Database, DBConfig, QueryResult};
use tempfile::TempDir;

fn main() {
    let dir = TempDir::new().expect("temp");
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).expect("create");
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, a TEXT, b TEXT)").unwrap();

    // Insert with simple individual inserts
    for i in 0..100usize {
        let a = format!("'a{}'", i % 10);
        let b = format!("'b{}'", i % 20);
        db.execute(&format!("INSERT INTO t (a, b) VALUES ({}, {})", a, b)).unwrap();
    }
    db.flush().ok();

    let sql_and = "SELECT * FROM t WHERE a = 'a3' AND b = 'b7'";
    let sql_a = "SELECT * FROM t WHERE a = 'a3'";
    let sql_b = "SELECT * FROM t WHERE b = 'b7'";

    let r_a = db.execute(sql_a).unwrap().materialize().unwrap();
    let ca = match &r_a { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
    println!("a='a3': {} rows", ca);

    let r_b = db.execute(sql_b).unwrap().materialize().unwrap();
    let cb = match &r_b { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
    println!("b='b7': {} rows", cb);

    let r_and = db.execute(sql_and).unwrap().materialize().unwrap();
    let cand = match &r_and { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
    println!("a='a3' AND b='b7': {} rows", cand);

    // Also test with for_each to see actual count
    let mut count = 0usize;
    let _ = db.execute(sql_and).unwrap()
        .for_each(|_cols, _row| { count += 1; Ok(motedb::StreamingControl::Continue) }, None).unwrap();
    println!("for_each count: {}", count);
    
    // Try with streaming
    let result = db.execute(sql_and).unwrap();
    match result {
        motedb::StreamingQueryResult::SelectReady { rows, .. } => println!("SelectReady: {} rows", rows.len()),
        motedb::StreamingQueryResult::SelectStreaming { .. } => println!("SelectStreaming (will materialize)"),
        _ => println!("Other"),
    }
    
    // Try explicit column names instead of *
    let sql2 = "SELECT id, a, b FROM t WHERE a = 'a3' AND b = 'b7'";
    let r2 = db.execute(sql2).unwrap().materialize().unwrap();
    let c2 = match &r2 { QueryResult::Select { rows, .. } => rows.len(), _ => 0 };
    println!("Explicit columns: {} rows", c2);
}
