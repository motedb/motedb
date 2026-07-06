//! 性能优异 — Performance Guarantee Tests
//!
//! 核心原则：延迟不随数据量线性增长。

#[path = "common/mod.rs"]
mod common;
use common::*;

#[test]
fn test_pk_select_latency_stable() {
    for &target in &[1_000, 5_000, 10_000] {
        let (_dir, db) = setup_db(); // Fresh DB each iteration
        exec(
            &db,
            "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
        );
        insert_test_rows(&db, target);
        let _ = fast_count(&db, "SELECT * FROM bench");

        let _ = db.execute("SELECT * FROM bench WHERE id = 1"); // warmup
        let (_, p99) = measure_p99_us(
            || {
                let _ = db.execute("SELECT * FROM bench WHERE id = 1");
            },
            20,
        );

        eprintln!("PK SELECT P99: {}us at {} rows", p99, target);
        assert!(
            p99 < 5_000,
            "PK SELECT P99 {}us too slow at {} rows",
            p99,
            target
        );
    }
}

#[test]
fn test_scan_latency_bounded() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );
    insert_test_rows(&db, 10_000);

    let (_, p99) = measure_p99_us(
        || {
            let _ = fast_count(&db, "SELECT * FROM bench");
        },
        20,
    );

    // Full scan of 10K rows should complete in < 100ms.
    eprintln!("Scan P99: {}us for 10K rows", p99);
    assert!(p99 < 100_000, "Scan P99 {}us too slow for 10K rows", p99);
}

#[test]
fn test_where_latency_bounded() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );
    insert_test_rows(&db, 10_000);
    let _ = fast_count(&db, "SELECT * FROM bench");

    let (_, p99) = measure_p99_us(
        || {
            let _ = fast_count(&db, "SELECT * FROM bench WHERE tag = 'US'");
        },
        20,
    );

    // WHERE scan should complete in < 50ms for 10K rows.
    assert!(p99 < 50_000, "WHERE P99 {}us too slow for 10K rows", p99);
}

#[test]
fn test_aggregate_latency_bounded() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );
    insert_test_rows(&db, 10_000);
    let _ = fast_count(&db, "SELECT * FROM bench");

    let (_, p99) = measure_p99_us(
        || {
            let _ = db.execute("SELECT COUNT(*) FROM bench WHERE tag = 'US'");
        },
        20,
    );

    assert!(p99 < 30_000, "Aggregate P99 {}us too slow", p99);
}

#[test]
fn test_distinct_latency_bounded() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );
    insert_test_rows(&db, 10_000);
    let _ = fast_count(&db, "SELECT * FROM bench");

    let (_, p99) = measure_p99_us(
        || {
            let _ = fast_count(&db, "SELECT DISTINCT tag FROM bench");
        },
        20,
    );

    assert!(p99 < 30_000, "DISTINCT P99 {}us too slow", p99);
}

#[test]
fn test_insert_latency_bounded() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );
    exec(&db, "INSERT INTO bench VALUES (1, 1.0, 'X')");

    let (_, p99) = measure_p99_us(
        || {
            exec(&db, "INSERT INTO bench VALUES (999998, 1.0, 'X')");
            exec(&db, "DELETE FROM bench WHERE id = 999998");
        },
        20,
    );

    // Single INSERT+DELETE should be < 5ms.
    eprintln!("INSERT+DELETE P99: {}us", p99);
    assert!(p99 < 5_000, "INSERT+DELETE P99 {}us too slow", p99);
}

#[test]
fn test_batch_insert_fast() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );

    let (_, p99) = measure_p99_us(
        || {
            let mut sql = String::from("INSERT INTO bench VALUES ");
            for i in 0..100 {
                if i > 0 {
                    sql.push(',');
                }
                // Use random-ish IDs to avoid PK conflicts across iterations
                let id = 900000
                    + (i as u64)
                    + std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64
                        % 100000;
                sql.push_str(&format!("({}, {:.1}, 'tag')", id, i as f64));
            }
            let _ = db.execute(&sql);
        },
        5,
    );

    eprintln!("Batch INSERT 100 rows P99: {}us", p99);
    assert!(p99 < 50_000, "Batch INSERT 100 rows P99 {}us too slow", p99);
}

#[test]
fn test_all_queries_under_threshold_10k() {
    let (_dir, db) = setup_db();
    exec(
        &db,
        "CREATE TABLE bench (id INT PRIMARY KEY, val FLOAT, tag TEXT)",
    );
    insert_test_rows(&db, 10_000);
    let _ = fast_count(&db, "SELECT * FROM bench");

    // All query types should be fast at 10K rows.
    let queries = vec![
        ("SELECT *", "SELECT * FROM bench"),
        ("WHERE eq", "SELECT * FROM bench WHERE tag = 'US'"),
        (
            "WHERE + LIMIT",
            "SELECT * FROM bench WHERE tag = 'US' LIMIT 10",
        ),
        ("COUNT WHERE", "SELECT COUNT(*) FROM bench WHERE tag = 'US'"),
        ("DISTINCT", "SELECT DISTINCT tag FROM bench"),
        (
            "ORDER BY + LIMIT",
            "SELECT * FROM bench ORDER BY val DESC LIMIT 10",
        ),
    ];

    for (name, sql) in queries {
        let (_, p99) = measure_p99_us(
            || {
                let _ = fast_count(&db, sql);
            },
            10,
        );
        assert!(
            p99 < 50_000,
            "{} P99 {}us exceeds 50ms at 10K rows",
            name,
            p99
        );
    }
}
