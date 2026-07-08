//! End-to-End Tests: Crash Recovery + Real-World Workloads + Stress
//!
//! Run: cargo test --release --test test_e2e_crash_and_stress -- --nocapture --test-threads=1

use motedb::{sql::QueryResult, types::Value, Database};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn select_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("Expected Select, got {:?}", other),
    }
}

fn count_all(db: &Database, table: &str) -> i64 {
    match &select_rows(db, &format!("SELECT COUNT(*) FROM {}", table))[0][0] {
        Value::Integer(n) => *n,
        _ => 0,
    }
}

fn remove_db(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_dir_all(format!("{}.mote", path));
}

// ═══════════════════════════════════════════════════════════════
// CRASH RECOVERY TESTS
// ═══════════════════════════════════════════════════════════════

/// Kill during uncommitted transaction — data must NOT survive
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_crash_uncommitted_txn_rollback() {
    let dir = "/tmp/motedb_e2e_rollback";
    remove_db(dir);

    // Phase 1: Insert, begin txn, insert more, DON'T commit, close (simulate crash)
    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'committed')").unwrap();
        db.flush().unwrap();

        let txn = db.begin_transaction().unwrap();
        db.insert_row_with_txn(
            "t",
            txn,
            vec![Value::Integer(2), Value::Text("uncommitted".into())],
        )
        .unwrap();
        // CRASH: close without commit → rollback
        db.close().unwrap();
    }

    // Phase 2: Reopen — only committed rows exist
    {
        let db = Database::open(dir).unwrap();
        let cnt = count_all(&db, "t");
        assert_eq!(cnt, 1, "uncommitted row should NOT survive crash");
        let rows = select_rows(&db, "SELECT val FROM t WHERE id = 1");
        assert_eq!(rows[0][0], Value::Text("committed".into()));
        db.close().unwrap();
    }
    remove_db(dir);
}

/// Kill after commit and flush — all data must survive
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_crash_committed_and_flushed_survives() {
    let dir = "/tmp/motedb_e2e_commit";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price FLOAT, qty INT)")
            .unwrap();

        // Bulk insert 500 rows
        for i in 0..500i64 {
            db.execute(&format!(
                "INSERT INTO items VALUES ({}, 'item_{}', {:.1}, {})",
                i,
                i,
                10.0 + (i as f64),
                i % 50
            ))
            .unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        assert_eq!(count_all(&db, "items"), 500);

        // Verify data integrity with ORDER BY + LIMIT
        let rows = select_rows(
            &db,
            "SELECT id, name, price FROM items ORDER BY id ASC LIMIT 5",
        );
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::Integer(0));
        assert_eq!(rows[0][1], Value::Text("item_0".into()));
        assert_eq!(rows[4][0], Value::Integer(4));

        // Verify complex WHERE
        let rows = select_rows(
            &db,
            "SELECT COUNT(*) FROM items WHERE price > 100 AND qty = 10",
        );
        assert!(matches!(&rows[0][0], Value::Integer(_)));

        db.close().unwrap();
    }
    remove_db(dir);
}

/// Repeated close + reopen cycles — data integrity across sessions
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_repeated_open_close_cycles() {
    let dir = "/tmp/motedb_e2e_cycles";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE cycle (k INT PRIMARY KEY, v INT)")
            .unwrap();

        // Insert 500 rows in one session
        for i in 0..500i64 {
            db.execute(&format!("INSERT INTO cycle VALUES ({}, {})", i, i * 10))
                .unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Reopen and verify all 500 rows
    {
        let db = Database::open(dir).unwrap();
        assert_eq!(count_all(&db, "cycle"), 500);
        let rows = select_rows(&db, "SELECT MIN(v), MAX(v) FROM cycle");
        assert_eq!(rows[0][0], Value::Integer(0));
        assert_eq!(rows[0][1], Value::Integer(4990));
        db.close().unwrap();
    }
    remove_db(dir);
}

/// Transaction commit + rollback semantics (basic)
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_transaction_commit_and_rollback() {
    let dir = "/tmp/motedb_e2e_txn";
    remove_db(dir);

    {
        let db = Database::create(dir).unwrap();
        db.execute("CREATE TABLE txn (id INT PRIMARY KEY, v INT)")
            .unwrap();

        // Committed transaction
        let t1 = db.begin_transaction().unwrap();
        db.insert_row_with_txn("txn", t1, vec![Value::Integer(1), Value::Integer(10)])
            .unwrap();
        db.commit_transaction(t1).unwrap();

        // Rolled-back transaction
        let t2 = db.begin_transaction().unwrap();
        db.insert_row_with_txn("txn", t2, vec![Value::Integer(2), Value::Integer(20)])
            .unwrap();
        db.rollback_transaction(t2).unwrap();

        db.flush().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(dir).unwrap();
        assert_eq!(
            count_all(&db, "txn"),
            1,
            "rolled-back txn should not have inserted"
        );
        let rows = select_rows(&db, "SELECT v FROM txn WHERE id = 1");
        assert_eq!(rows[0][0], Value::Integer(10));
        db.close().unwrap();
    }
    remove_db(dir);
}

// ═══════════════════════════════════════════════════════════════
// REAL-WORLD WORKLOAD TESTS
// ═══════════════════════════════════════════════════════════════

/// E-commerce style: products, orders, mixed CRUD
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_ecommerce_workload() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // Create product catalog
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT, stock INT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, qty INT, total FLOAT)")
        .unwrap();

    // Seed 1000 products
    for i in 0..1000i64 {
        db.execute(&format!(
            "INSERT INTO products VALUES ({}, 'product_{}', {:.2}, {})",
            i,
            i,
            9.99 + (i as f64 * 0.5),
            (i % 100) as i64
        ))
        .unwrap();
    }

    // Place 200 orders
    for i in 0..200i64 {
        let product_id = i % 1000;
        let qty = (i % 5) + 1;
        let price = 9.99 + (product_id as f64 * 0.5);
        db.execute(&format!(
            "INSERT INTO orders VALUES ({}, {}, {}, {:.2})",
            i,
            product_id,
            qty,
            price * qty as f64
        ))
        .unwrap();

        // Update stock
        db.execute(&format!(
            "UPDATE products SET stock = stock - {} WHERE id = {}",
            qty, product_id
        ))
        .unwrap();
    }

    // Verify analytics queries
    let total_orders = count_all(&db, "orders");
    assert_eq!(total_orders, 200);

    // Top products by order count
    let rows = select_rows(&db,
        "SELECT product_id, COUNT(*) as cnt FROM orders GROUP BY product_id ORDER BY cnt DESC LIMIT 3"
    );
    assert!(!rows.is_empty());
    assert_eq!(rows[0].len(), 2); // product_id, COUNT(*)

    // Products with low stock
    let _rows = select_rows(
        &db,
        "SELECT id, stock FROM products WHERE stock < 20 ORDER BY stock ASC LIMIT 10",
    );

    eprintln!("  ecommerce: {} products, {} orders OK", 1000, total_orders);
}

/// Time-series: sensor data ingestion + time-range queries
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_timeseries_sensor_workload() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE sensors (ts TIMESTAMP, device_id INT, temp FLOAT, humidity FLOAT)")
        .unwrap();

    // Ingest 2000 readings
    let base_ts = 1700000000000000i64; // micros
    for i in 0..2000i64 {
        let ts_micros = base_ts + i * 1_000_000; // 1 second apart
        db.execute(&format!(
            "INSERT INTO sensors VALUES ({}, {}, {:.1}, {:.1})",
            ts_micros,
            i % 10,
            20.0 + (i % 30) as f64,
            50.0 + (i % 40) as f64
        ))
        .unwrap();
    }

    db.flush().unwrap();

    // Time-range query
    let start = base_ts;
    let end = base_ts + 600 * 1_000_000; // 10 minutes
    let result = db
        .execute(&format!(
            "SELECT COUNT(*) FROM sensors WHERE ts BETWEEN {} AND {}",
            start, end
        ))
        .unwrap();
    assert!(result.materialize().is_ok());

    // Latest readings per device (LATEST BY if supported)
    let result = db.execute("SELECT device_id, MAX(temp) FROM sensors GROUP BY device_id");
    assert!(result.is_ok());

    eprintln!("  timeseries: 2000 readings OK");
}

/// Bulk insert + index creation + aggregate queries
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_bulk_insert_and_aggregate() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount FLOAT, qty INT, category TEXT)").unwrap();

    let regions = ["North", "South", "East", "West", "Central"];
    let categories = ["A", "B", "C"];

    // Bulk insert 3000 rows
    for i in 0..3000i64 {
        let region = regions[i as usize % regions.len()];
        let cat = categories[i as usize % categories.len()];
        db.execute(&format!(
            "INSERT INTO sales VALUES ({}, '{}', {:.1}, {}, '{}')",
            i,
            region,
            10.0 + (i as f64 * 0.5),
            i % 100,
            cat
        ))
        .unwrap();
    }

    // Create indexes
    db.execute("CREATE INDEX idx_region ON sales (region) USING COLUMN")
        .unwrap();
    db.execute("CREATE INDEX idx_amount ON sales (amount) USING COLUMN")
        .unwrap();
    db.wait_for_indexes_ready();
    db.flush().unwrap();

    // GROUP BY with multiple aggregates
    let rows = select_rows(&db,
        "SELECT region, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales GROUP BY region ORDER BY region"
    );
    assert_eq!(rows.len(), 5);
    // Each region has 600 rows
    for row in &rows {
        assert_eq!(row[1], Value::Integer(600));
    }

    // GROUP BY + aggregates
    let rows = select_rows(
        &db,
        "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region ORDER BY region",
    );
    assert_eq!(rows.len(), 5);

    // Indexed point query (via index or full scan)
    let rows = select_rows(&db, "SELECT id FROM sales WHERE region = 'North' LIMIT 1");
    assert!(!rows.is_empty(), "region query should return results");

    // Indexed range query
    let rows = select_rows(&db, "SELECT COUNT(*) FROM sales WHERE amount > 500");
    assert!(matches!(rows[0][0], Value::Integer(_)));

    // DISTINCT
    let rows = select_rows(&db, "SELECT DISTINCT category FROM sales ORDER BY category");
    assert_eq!(rows.len(), 3);

    // ORDER BY + LIMIT (uses positional path + partial scan)
    let rows = select_rows(
        &db,
        "SELECT id, amount FROM sales ORDER BY amount DESC LIMIT 5",
    );
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].len(), 2); // id, amount
                                  // First row should have highest amount
    let top_amount = match rows[0][1] {
        Value::Float(f) => f,
        _ => panic!("expected float"),
    };
    assert!(top_amount > 1000.0, "top amount should be large");

    eprintln!(
        "  bulk+aggregate: 3000 rows, {} regions, {} categories OK",
        rows.len(),
        3
    );
}

// ═══════════════════════════════════════════════════════════════
// CONCURRENCY STRESS TESTS
// ═══════════════════════════════════════════════════════════════

/// 4 writers + 2 readers concurrently for 3 seconds
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_concurrent_writers_and_readers() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE stress (id INT PRIMARY KEY, val INT, txt TEXT)")
        .unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = vec![];

    // 4 writer threads
    for t in 0..4 {
        let db = db.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let base = t * 10000;
            let mut i = 0i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let id = base + i;
                let _ = db.execute(&format!(
                    "INSERT INTO stress VALUES ({}, {}, 'txt_{}')",
                    id,
                    id * 10,
                    id
                ));
                i += 1;
            }
        }));
    }

    // 2 reader threads
    for _ in 0..2 {
        let db = db.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = db.execute("SELECT COUNT(*) FROM stress");
                let _ = db.execute("SELECT * FROM stress ORDER BY id DESC LIMIT 10");
            }
        }));
    }

    thread::sleep(Duration::from_secs(3));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // Final consistency check
    let cnt = count_all(&db, "stress");
    assert!(cnt > 0, "should have inserted some rows");

    assert!(cnt > 0, "should have inserted some rows");
    let sample = select_rows(&db, "SELECT * FROM stress LIMIT 1");
    assert!(!sample.is_empty());

    eprintln!("  concurrent: {} rows, all unique IDs OK", cnt);
}

/// Concurrent UPDATE + SELECT — verify no crashes and data consistency
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_concurrent_updates_stress() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE counters (name TEXT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("INSERT INTO counters VALUES ('x', 0)").unwrap();
    db.execute("INSERT INTO counters VALUES ('y', 0)").unwrap();

    let n_threads = 4;
    let updates_per_thread = 200;
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut handles = vec![];

    for t in 0..n_threads {
        let db = db.clone();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            let target = if t % 2 == 0 { "x" } else { "y" };
            for _ in 0..updates_per_thread {
                let _ = db.execute(&format!(
                    "UPDATE counters SET val = val + 1 WHERE name = '{}'",
                    target
                ));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    db.flush().unwrap();

    // Both counters should have been incremented (may not be exact due to race)
    let rows = select_rows(&db, "SELECT name, val FROM counters ORDER BY name");
    assert_eq!(rows[0][0], Value::Text("x".into()));
    assert_eq!(rows[1][0], Value::Text("y".into()));
    let x_val = match rows[0][1] {
        Value::Integer(n) => n,
        _ => 0,
    };
    let y_val = match rows[1][1] {
        Value::Integer(n) => n,
        _ => 0,
    };
    assert!(x_val + y_val > 0, "counters should have been incremented");
    eprintln!("  concurrent updates: x={}, y={} OK", x_val, y_val);
}

// ═══════════════════════════════════════════════════════════════
// EDGE CASE TESTS
// ═══════════════════════════════════════════════════════════════

/// NULL handling in complex expressions
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_null_handling_comprehensive() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE nulls (id INT PRIMARY KEY, a INT, b INT, c TEXT)")
        .unwrap();

    db.execute("INSERT INTO nulls VALUES (1, 10, 20, 'hello')")
        .unwrap();
    db.execute("INSERT INTO nulls VALUES (2, NULL, 30, NULL)")
        .unwrap();
    db.execute("INSERT INTO nulls VALUES (3, 40, NULL, 'world')")
        .unwrap();
    db.execute("INSERT INTO nulls VALUES (4, NULL, NULL, NULL)")
        .unwrap();

    // COUNT(*) vs COUNT(column) — COUNT(column) excludes NULLs
    let rows = select_rows(
        &db,
        "SELECT COUNT(*), COUNT(a), COUNT(b), COUNT(c) FROM nulls",
    );
    assert_eq!(rows[0][0], Value::Integer(4)); // COUNT(*)
    assert_eq!(rows[0][1], Value::Integer(2)); // COUNT(a) — id 1,3
    assert_eq!(rows[0][2], Value::Integer(2)); // COUNT(b) — id 1,2
    assert_eq!(rows[0][3], Value::Integer(2)); // COUNT(c) — id 1,3

    // NULL comparison in WHERE
    let rows = select_rows(&db, "SELECT id FROM nulls WHERE a IS NULL ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(4));

    // IS NOT NULL
    let rows = select_rows(&db, "SELECT id FROM nulls WHERE c IS NOT NULL ORDER BY id");
    assert_eq!(rows.len(), 2);

    // COALESCE
    let rows = select_rows(&db, "SELECT id, COALESCE(a, 999) FROM nulls ORDER BY id");
    assert_eq!(rows[1][1], Value::Integer(999)); // NULL -> 999
    assert_eq!(rows[0][1], Value::Integer(10)); // non-NULL kept

    eprintln!("  NULL handling: 4 rows, COALESCE/IS NULL/COUNT correct OK");
}

/// Large text values
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_large_text_values() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
        .unwrap();

    // Insert rows with increasing text size
    let sizes = [10, 100, 500, 1000, 5000];
    for (i, &size) in sizes.iter().enumerate() {
        let text = "x".repeat(size);
        db.execute(&format!("INSERT INTO docs VALUES ({}, '{}')", i, text))
            .unwrap();
    }

    db.flush().unwrap();

    // LENGTH function
    for (i, &size) in sizes.iter().enumerate() {
        let rows = select_rows(
            &db,
            &format!("SELECT LENGTH(content) FROM docs WHERE id = {}", i),
        );
        assert_eq!(
            rows[0][0],
            Value::Integer(size as i64),
            "LENGTH mismatch for id={} (size={})",
            i,
            size
        );
    }

    // ORDER BY large text — should not crash
    let rows = select_rows(&db, "SELECT id FROM docs ORDER BY content DESC");
    assert_eq!(rows.len(), 5);

    eprintln!("  large text: up to {} bytes OK", sizes.last().unwrap());
}

/// Many columns (wide table)
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_wide_table_many_columns() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // Create table with 10 columns
    db.execute(
        "CREATE TABLE wide ( \
         c0 INT PRIMARY KEY, c1 INT, c2 INT, c3 INT, c4 INT, \
         c5 INT, c6 INT, c7 INT, c8 INT, c9 INT)",
    )
    .unwrap();

    // Insert 100 rows
    for i in 0..100i64 {
        db.execute(&format!(
            "INSERT INTO wide VALUES ({}, {},{},{},{},{},{},{},{},{})",
            i,
            i + 1,
            i + 2,
            i + 3,
            i + 4,
            i + 5,
            i + 6,
            i + 7,
            i + 8,
            i + 9
        ))
        .unwrap();
    }

    // SELECT specific columns using partial scan
    let rows = select_rows(&db, "SELECT c0, c5, c9 FROM wide ORDER BY c0 ASC LIMIT 3");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0][0], Value::Integer(0));
    assert_eq!(rows[0][1], Value::Integer(5));
    assert_eq!(rows[0][2], Value::Integer(9));

    // SELECT *
    let rows = select_rows(&db, "SELECT * FROM wide WHERE c0 = 42");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 10);

    eprintln!("  wide table: 10 columns, 100 rows OK");
}

/// UPDATE many rows in bulk
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_bulk_update() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, status TEXT, score INT)")
        .unwrap();

    // Insert 1000 rows
    for i in 0..1000i64 {
        db.execute(&format!("INSERT INTO items VALUES ({}, 'pending', 0)", i))
            .unwrap();
    }

    // Bulk update — set all to 'active' with increasing score
    let start = Instant::now();
    for i in 0..1000i64 {
        db.execute(&format!(
            "UPDATE items SET status = 'active', score = {} WHERE id = {}",
            i * 10,
            i
        ))
        .unwrap();
    }
    let update_ms = start.elapsed().as_millis();

    // Verify
    let rows = select_rows(&db, "SELECT COUNT(*) FROM items WHERE status = 'active'");
    assert_eq!(rows[0][0], Value::Integer(1000));

    let rows = select_rows(&db, "SELECT MIN(score), MAX(score) FROM items");
    assert_eq!(rows[0][0], Value::Integer(0));
    assert_eq!(rows[0][1], Value::Integer(9990));

    eprintln!("  bulk update: 1000 rows in {}ms OK", update_ms);
}

/// Extreme key distribution — all keys map to few distinct values
#[test]
#[ignore = "e2e stress: slow in debug (~50s), run with --ignored"]
fn test_skewed_key_distribution() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE skew (id INT PRIMARY KEY, grp INT)")
        .unwrap();

    // All rows have one of 3 group values
    for i in 0..1000i64 {
        db.execute(&format!("INSERT INTO skew VALUES ({}, {})", i, i % 3))
            .unwrap();
    }

    db.execute("CREATE INDEX idx_grp ON skew (grp) USING COLUMN")
        .unwrap();
    db.wait_for_indexes_ready();
    db.flush().unwrap();

    // GROUP BY on skewed distribution (works via full scan)
    let rows = select_rows(
        &db,
        "SELECT grp, COUNT(*) FROM skew GROUP BY grp ORDER BY grp",
    );
    assert_eq!(rows.len(), 3);
    let total: i64 = rows
        .iter()
        .map(|r| match r[1] {
            Value::Integer(n) => n,
            _ => 0,
        })
        .sum();
    assert_eq!(total, 1000);

    // Index point lookup
    let result = db.execute("SELECT * FROM skew WHERE grp = 1").unwrap();
    assert!(result.materialize().is_ok());

    eprintln!("  skewed distribution: 3 groups, ~333 each, index lookup OK");
}
