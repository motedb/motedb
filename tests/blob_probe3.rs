use motedb::Database;
use tempfile::TempDir;
fn res(db: &Database, sql: &str) -> Result<String, String> {
    db.execute(sql)
        .map_err(|e| format!("exec: {e}"))?
        .materialize()
        .map_err(|e| format!("mat: {e}"))?
        .select_rows()
        .map(|(_, r)| format!("{r:?}"))
        .ok_or("not select".into())
}
#[test]
fn probe_index_order() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("i.mote")).unwrap();
    db.execute("CREATE TABLE t1 (_uid INTEGER PRIMARY KEY, log_time TIMESTAMP, data BLOB)")
        .unwrap();
    db.execute("CREATE TIMESTAMP INDEX i1 ON t1(log_time)")
        .unwrap(); // BEFORE inserts
    for i in 0..5i64 {
        let hex: String = (0..28usize)
            .map(|j| format!("{:02x}", (i + j as i64) as u8))
            .collect();
        db.execute(&format!(
            "INSERT INTO t1 VALUES ({i}, {}, X'{hex}')",
            1700000000000000 + i * 10
        ))
        .unwrap();
    }
    println!("idx-before range: {:?}", res(&db, "SELECT data FROM t1 WHERE log_time >= 1700000000000000 AND log_time < 1700000000000030 LIMIT 1").map(|s| s.chars().take(50).collect::<String>()));

    db.execute("CREATE TABLE t2 (_uid INTEGER PRIMARY KEY, log_time TIMESTAMP, data BLOB)")
        .unwrap();
    for i in 0..5i64 {
        let hex: String = (0..28usize)
            .map(|j| format!("{:02x}", (i + j as i64) as u8))
            .collect();
        db.execute(&format!(
            "INSERT INTO t2 VALUES ({i}, {}, X'{hex}')",
            1700000000000000 + i * 10
        ))
        .unwrap();
    }
    db.execute("CREATE TIMESTAMP INDEX i2 ON t2(log_time)")
        .unwrap(); // AFTER inserts
    println!("idx-after range: {:?}", res(&db, "SELECT data FROM t2 WHERE log_time >= 1700000000000000 AND log_time < 1700000000000030 LIMIT 1").map(|s| s.chars().take(50).collect::<String>()));

    // Exact MCAP importer shape: index BEFORE + one multi-row batch INSERT.
    db.execute("CREATE TABLE t3 (_uid INTEGER PRIMARY KEY, log_time TIMESTAMP, publish_time TIMESTAMP, data BLOB)").unwrap();
    db.execute("CREATE TIMESTAMP INDEX i3 ON t3(log_time)")
        .unwrap();
    let vals: Vec<String> = (0..50i64)
        .map(|i| {
            let hex: String = (0..28usize)
                .map(|j| format!("{:02x}", (i + j as i64) as u8))
                .collect();
            format!(
                "({}, {}, {}, X'{}')",
                (1u64 << 40) + i as u64,
                1700000000000000 + i * 10,
                1700000000000000 + i * 10,
                hex
            )
        })
        .collect();
    db.execute(&format!("INSERT INTO t3 VALUES {}", vals.join(",")))
        .unwrap();
    println!("mcap-shape range: {:?}", res(&db, "SELECT data FROM t3 WHERE log_time >= 1700000000000000 AND log_time < 1700000000000030 LIMIT 1").map(|s| s.chars().take(50).collect::<String>()));
    println!(
        "mcap-shape count: {:?}",
        res(&db, "SELECT COUNT(*) FROM t3")
    );
}
