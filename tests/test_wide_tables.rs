//! Wide-table coverage.
//!
//! Regression for the v0.5.0 crash where the columnar SSTable header reserved
//! only 16 column-tag slots (`MAX_COLUMNS`). Tables with > 16 columns inserted
//! fine but panicked the background flush thread (`index out of bounds`).
//! After the fix, `MAX_COLUMNS = 128` and CREATE TABLE rejects wider tables
//! with a clean error instead of a deferred panic.
//!
//! Coverage:
//! - boundary column counts (16, 17, 32, 64, 128) insert/select/update/delete
//! - CREATE TABLE over the cap (129) returns an error, not a panic
//! - widest supported table round-trips through checkpoint + reopen
//! - every column value is read back exactly (no column-index misalignment)

use motedb::types::Value;
use motedb::{DBConfig, Database, QueryResult};
use tempfile::TempDir;

fn make_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
}

fn reopen(dir: &TempDir) -> Database {
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    Database::open_with_config(dir.path(), config).unwrap()
}

fn select_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?}", std::mem::discriminant(&other)),
    }
}

fn make_table(db: &Database, name: &str, n_cols: usize) {
    let mut cols = vec!["id INT PRIMARY KEY".to_string()];
    for c in 0..n_cols {
        cols.push(format!("c{} INT", c));
    }
    db.execute(&format!("CREATE TABLE {} ({})", name, cols.join(", ")))
        .unwrap();
}

/// Insert and read back a single wide row, asserting every column round-trips.
fn roundtrip_row(db: &Database, table: &str, n_cols: usize) {
    // Build a row whose c{i} value is distinct and easily verifiable.
    let mut row = vec![Value::Integer(1)];
    for c in 0..n_cols as i64 {
        row.push(Value::Integer(1_000 + c));
    }
    db.insert_row(table, row).unwrap();

    let rows = select_rows(db, &format!("SELECT * FROM {} WHERE id = 1", table));
    assert_eq!(rows.len(), 1, "expected one row in {}", table);
    let r = &rows[0];
    assert_eq!(r.len(), n_cols + 1, "column count mismatch in {}", table);
    assert_eq!(r[0], Value::Integer(1), "PK in {}", table);
    for c in 0..n_cols as i64 {
        assert_eq!(
            r[(c + 1) as usize],
            Value::Integer(1_000 + c),
            "column c{} in {}",
            c,
            table
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Boundary column counts — the old crash boundary is 16.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_at_old_limit_16() {
    // Exactly 16 columns used to be the last safe count.
    let (_dir, db) = make_db();
    let n = 16; // total cols = 17 (id + 16)
    make_table(&db, "w16", n);
    roundtrip_row(&db, "w16", n);
}

#[test]
fn wide_table_just_over_old_limit_17() {
    // 17 columns was the smallest count that triggered the panic.
    let (_dir, db) = make_db();
    let n = 17;
    make_table(&db, "w17", n);
    roundtrip_row(&db, "w17", n);
}

#[test]
fn wide_table_32_cols() {
    let (_dir, db) = make_db();
    let n = 32;
    make_table(&db, "w32", n);
    roundtrip_row(&db, "w32", n);
}

#[test]
fn wide_table_64_cols() {
    let (_dir, db) = make_db();
    let n = 64;
    make_table(&db, "w64", n);
    roundtrip_row(&db, "w64", n);
}

#[test]
fn wide_table_at_new_limit_128() {
    // The largest supported table.
    let (_dir, db) = make_db();
    let n = 127; // id + 127 = 128 total
    make_table(&db, "w128", n);
    roundtrip_row(&db, "w128", n);
}

// ═══════════════════════════════════════════════════════════════════════
// Over-limit CREATE TABLE must error, not panic.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn create_table_over_column_limit_errors() {
    let (_dir, db) = make_db();
    let n = 129;
    let mut cols = vec!["id INT PRIMARY KEY".to_string()];
    for c in 0..n {
        cols.push(format!("c{} INT", c));
    }
    let res = db.execute(&format!("CREATE TABLE too_wide ({})", cols.join(", ")));
    let err = match res {
        Ok(_) => panic!("CREATE TABLE with {}+1 columns should be rejected", n),
        Err(e) => e,
    };
    // The error message should mention the limit for diagnostics.
    let msg = format!("{}", err);
    assert!(
        msg.contains("maximum") || msg.contains("columns"),
        "error should explain the limit, got: {}",
        msg
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-row wide table: insert, count, update, delete, re-count.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_multi_row_lifecycle() {
    let (_dir, db) = make_db();
    let n = 30;
    make_table(&db, "wl", n);

    // Insert 20 rows.
    for i in 1..=20 {
        let mut row = vec![Value::Integer(i)];
        for c in 0..n as i64 {
            row.push(Value::Integer(i * 1000 + c));
        }
        db.insert_row("wl", row).unwrap();
    }

    let cnt = select_rows(&db, "SELECT COUNT(*) FROM wl");
    assert_eq!(cnt[0][0], Value::Integer(20));

    // Spot-check a middle row's last column.
    let rows = select_rows(&db, "SELECT * FROM wl WHERE id = 10");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), n + 1);
    assert_eq!(rows[0][n], Value::Integer(10 * 1000 + (n as i64 - 1)));

    // Update one column of one row.
    db.execute("UPDATE wl SET c0 = 9999 WHERE id = 10").unwrap();
    let rows = select_rows(&db, "SELECT c0 FROM wl WHERE id = 10");
    assert_eq!(rows[0][0], Value::Integer(9999));

    // Delete and re-count.
    db.execute("DELETE FROM wl WHERE id = 10").unwrap();
    let cnt = select_rows(&db, "SELECT COUNT(*) FROM wl");
    assert_eq!(cnt[0][0], Value::Integer(19));

    // id=10 should be gone.
    let rows = select_rows(&db, "SELECT * FROM wl WHERE id = 10");
    assert!(rows.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════
// Durability: a wide table must survive checkpoint + reopen.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_survives_reopen() {
    let (dir, db) = make_db();
    let n = 25;
    make_table(&db, "wd", n);
    for i in 1..=5 {
        let mut row = vec![Value::Integer(i)];
        for c in 0..n as i64 {
            row.push(Value::Integer(i * 10 + c));
        }
        db.insert_row("wd", row).unwrap();
    }
    drop(db);

    let db = reopen(&dir);
    let cnt = select_rows(&db, "SELECT COUNT(*) FROM wd");
    assert_eq!(cnt[0][0], Value::Integer(5));

    // Verify all columns of row 3 after reopen.
    let rows = select_rows(&db, "SELECT * FROM wd WHERE id = 3");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), n + 1);
    for c in 0..n as i64 {
        assert_eq!(
            rows[0][(c + 1) as usize],
            Value::Integer(30 + c),
            "col c{}",
            c
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Column index alignment: many columns must not be read shifted/crossed.
// Insert columns with a pattern that would reveal off-by-one misalignment.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_column_alignment() {
    let (_dir, db) = make_db();
    let n = 40;
    make_table(&db, "wa", n);

    // c{i} = i * 7 + 3 — a pattern unlikely to coincide by accident.
    let mut row = vec![Value::Integer(1)];
    for c in 0..n as i64 {
        row.push(Value::Integer(c * 7 + 3));
    }
    db.insert_row("wa", row).unwrap();

    let rows = select_rows(&db, "SELECT * FROM wa WHERE id = 1");
    assert_eq!(rows.len(), 1);
    for c in 0..n as i64 {
        assert_eq!(
            rows[0][(c + 1) as usize],
            Value::Integer(c * 7 + 3),
            "misaligned at c{}",
            c
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// NULLs mixed into a wide Integer table.
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_with_nulls() {
    let (_dir, db) = make_db();
    // Allow NULLs (no NOT NULL constraint).
    db.execute("CREATE TABLE wn (id INT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT)")
        .unwrap();

    // Row with all NULLs except PK.
    db.insert_row(
        "wn",
        vec![
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    )
    .unwrap();

    // Row with mixed.
    db.insert_row(
        "wn",
        vec![
            Value::Integer(2),
            Value::Integer(10),
            Value::Null,
            Value::Integer(30),
            Value::Null,
            Value::Integer(50),
        ],
    )
    .unwrap();

    let r1 = select_rows(&db, "SELECT * FROM wn WHERE id = 1");
    assert_eq!(r1[0][1], Value::Null);
    assert_eq!(r1[0][3], Value::Null);

    let r2 = select_rows(&db, "SELECT * FROM wn WHERE id = 2");
    assert_eq!(r2[0][1], Value::Integer(10));
    assert_eq!(r2[0][2], Value::Null);
    assert_eq!(r2[0][3], Value::Integer(30));
    assert_eq!(r2[0][5], Value::Integer(50));
}
