//! Large rows (inline → Blob spill path) across live / clean-reopen /
//! crash-sim reopen.

use motedb::Database;
use tempfile::TempDir;

fn large_text(n: usize) -> String {
    "x".repeat(n)
}

#[test]
fn test_large_rows_live_query() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    for i in 0..5 {
        let t = large_text(50_000 + i);
        db.execute(&format!("INSERT INTO big VALUES ({0}, '{1}')", i, t))
            .unwrap();
    }
    let r = db
        .query("SELECT id, LENGTH(body) FROM big ORDER BY id")
        .unwrap();
    assert_eq!(r.len(), 5);
    for (i, row) in r.iter().enumerate() {
        match (&row[0], &row[1]) {
            (motedb::types::Value::Integer(id), motedb::types::Value::Integer(len)) => {
                assert_eq!(*id, i as i64);
                assert_eq!(*len as usize, 50_000 + i);
            }
            o => panic!("row {i}: {o:?}"),
        }
    }
}

#[test]
fn test_large_rows_clean_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        for i in 0..5 {
            db.execute(&format!(
                "INSERT INTO big VALUES ({0}, '{1}')",
                i,
                large_text(50_000 + i)
            ))
            .unwrap();
        }
        let _ = db.close();
    }
    let db = Database::open(&p).unwrap();
    let r = db
        .query("SELECT id, LENGTH(body) FROM big ORDER BY id")
        .unwrap();
    assert_eq!(r.len(), 5, "large rows lost on clean reopen");
    for (i, row) in r.iter().enumerate() {
        if let motedb::types::Value::Integer(len) = &row[1] {
            assert_eq!(*len as usize, 50_000 + i);
        }
    }
}

#[test]
fn test_large_rows_crash_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        for i in 0..5 {
            db.execute(&format!(
                "INSERT INTO big VALUES ({0}, '{1}')",
                i,
                large_text(50_000 + i)
            ))
            .unwrap();
        }
        std::mem::forget(db);
    }
    std::fs::remove_file(p.join(".lock")).ok();
    let db = Database::open(&p).unwrap();
    let r = db
        .query("SELECT id, LENGTH(body) FROM big ORDER BY id")
        .unwrap();
    assert_eq!(r.len(), 5, "large rows lost after crash recovery");
    for (i, row) in r.iter().enumerate() {
        if let motedb::types::Value::Integer(len) = &row[1] {
            assert_eq!(*len as usize, 50_000 + i, "row {i} corrupted");
        }
    }
}

#[test]
fn test_oversized_text_rejected_at_write() {
    // 65,534 is the columnar TEXT ceiling (0xFFFF reserved for NULL).
    // Oversized values are rejected at INSERT time — previously they stored
    // fine but could never be read back (read path error).
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    let ok = format!("INSERT INTO t VALUES (1, '{}')", "x".repeat(65_534));
    let too_big = format!("INSERT INTO t VALUES (2, '{}')", "x".repeat(65_535));
    assert!(db.execute(&ok).is_ok(), "65,534 bytes must be accepted");
    let err = match db.execute(&too_big) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("oversized TEXT must be rejected at write"),
    };
    assert!(
        err.contains("65534"),
        "oversized TEXT must be rejected at write: {err}"
    );
}

#[test]
fn test_large_rows_point_query() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    for i in 0..3 {
        db.execute(&format!(
            "INSERT INTO big VALUES ({0}, '{1}')",
            i,
            large_text(60_000)
        ))
        .unwrap();
    }
    // Point fetch returns the full payload. (Values stay under the 65,534-
    // byte columnar TEXT limit — 0xFFFF is reserved for NULL.)
    let r = db
        .query("SELECT LENGTH(body) FROM big WHERE id = 1")
        .unwrap();
    match &r[0][0] {
        motedb::types::Value::Integer(n) => assert_eq!(*n, 60_000),
        o => panic!("{o:?}"),
    }
}
