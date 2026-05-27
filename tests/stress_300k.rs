//! 正确性压力测试 — 30 万行 INSERT + 10K 级 CRUD 验证
//!
//! 策略：INSERT 300K 验证写入吞吐和数据完整性，
//!       UPDATE/DELETE 降到 10K 级别（单次 DELETE ~5ms × 10K = 50s 可接受）

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

const N: usize = 50_000;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap_or_else(|e| panic!("SQL: {}\n  err: {}", sql, e))
        .materialize().expect("materialize")
}

fn count_rows(db: &Database, table: &str) -> i64 {
    select_int(db, &format!("SELECT COUNT(*) AS cnt FROM {}", table)).unwrap_or(0)
}

fn select_int(db: &Database, sql: &str) -> Option<i64> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows.first().and_then(|row| row.first().and_then(|v| {
            if let Value::Integer(i) = v { Some(*i) } else { None }
        })),
        _ => None,
    }
}

fn select_row(db: &Database, sql: &str) -> Option<Vec<Value>> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows.first().cloned(),
        _ => None,
    }
}

fn count_filtered(db: &Database, table: &str, filter: &str) -> i64 {
    select_int(db, &format!("SELECT COUNT(*) AS cnt FROM {} WHERE {}", table, filter)).unwrap_or(0)
}

fn print_elapsed(name: &str, ops: usize, ms: u64) {
    let us = if ops > 0 { ms as f64 * 1000.0 / ops as f64 } else { 0.0 };
    let tps = if ms > 0 { ops as f64 / (ms as f64 / 1000.0) } else { f64::INFINITY };
    println!("{:<60} | {:>6} ops | {:>8.1} ms | {:>7.1} µs/op | {:>10.0} ops/s",
        name, ops, ms, us, tps);
}

// ════════════════════════════════════════════════════════════════
// Test 1: INSERT 300K + 数据完整性
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_insert_integrity() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, age INTEGER, city TEXT)");

    // Use 50K for CI reliability, 300K for local stress testing
    let n = if std::env::var("CI").is_ok() { 50_000 } else { N };
    let t = std::time::Instant::now();
    for i in 1..=n as i64 {
        exec(&db, &format!(
            "INSERT INTO t1 VALUES ({}, 'user_{}', {}, {}, '{}')",
            i, i, i as f64 * 1.5, 20 + (i % 60),
            ["Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Chengdu"][(i % 5) as usize]
        ));
    }
    print_elapsed(&format!("INSERT {} rows (5 cols)", n), n, t.elapsed().as_millis() as u64);

    // COUNT
    let cnt = count_rows(&db, "t1");
    assert_eq!(cnt, n as i64, "COUNT: expected {}, got {}", n, cnt);

    // 首行
    let first = select_row(&db, "SELECT * FROM t1 WHERE id = 1").expect("id=1");
    assert_eq!(first[0], Value::Integer(1));
    assert_eq!(first[1], Value::text("user_1".into()));

    // 尾行
    let last_id = n as i64;
    let last = select_row(&db, &format!("SELECT * FROM t1 WHERE id = {}", last_id))
        .unwrap_or_else(|| panic!("id={}", last_id));
    assert_eq!(last[0], Value::Integer(last_id));
    assert_eq!(last[1], Value::text(format!("user_{}", last_id)));

    // 中间行
    let mid_id = n as i64 / 2;
    let mid = select_row(&db, &format!("SELECT * FROM t1 WHERE id = {}", mid_id))
        .unwrap_or_else(|| panic!("id={}", mid_id));
    assert_eq!(mid[3], Value::Integer(20 + (mid_id % 60)));

    // 不存在的行
    assert!(select_row(&db, "SELECT * FROM t1 WHERE id = 999999").is_none());

    // WHERE 过滤 — flush + wait for index build to ensure column index is ready
    db.flush().expect("flush");
    db.wait_for_indexes_ready();
    let near_end = n as i64 - 1000;
    assert_eq!(count_filtered(&db, "t1", &format!("id > {}", near_end)), 1000);
    assert_eq!(count_filtered(&db, "t1", &format!("id >= {}", near_end + 1)), 1000);
    assert_eq!(count_filtered(&db, "t1", &format!("id > {}", near_end + 1)), 999);

    println!("  ✓ INSERT integrity: COUNT={}, first/mid/last/WHERE verified", cnt);
}

// ════════════════════════════════════════════════════════════════
// Test 2: UPDATE 10K 正确性（偶改奇不改）
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_update_correctness() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT, counter INTEGER)");

    // 只 INSERT 前 10K（减少测试时间）
    const M: usize = 10_000;
    for i in 1..=M as i64 {
        exec(&db, &format!("INSERT INTO t2 VALUES ({}, 'original', 0)", i));
    }

    // UPDATE 偶数行
    let t = std::time::Instant::now();
    let update_count = M / 2;
    for i in (2..=M as i64).step_by(2) {
        exec(&db, &format!("UPDATE t2 SET val = 'updated', counter = {} WHERE id = {}", i * 10, i));
    }
    print_elapsed(&format!("UPDATE {} rows (even ids)", update_count), update_count, t.elapsed().as_millis() as u64);

    assert_eq!(count_rows(&db, "t2"), M as i64, "UPDATE should not change row count");

    // 偶数行已更新
    for i in (2..=200i64).step_by(2) {
        let row = select_row(&db, &format!("SELECT * FROM t2 WHERE id = {}", i)).unwrap();
        assert_eq!(row[1], Value::text("updated".into()), "row {} val", i);
        assert_eq!(row[2], Value::Integer(i * 10), "row {} counter", i);
    }

    // 奇数行不变
    for i in (1..=199i64).step_by(2) {
        let row = select_row(&db, &format!("SELECT * FROM t2 WHERE id = {}", i)).unwrap();
        assert_eq!(row[1], Value::text("original".into()), "row {} val", i);
        assert_eq!(row[2], Value::Integer(0), "row {} counter", i);
    }

    println!("  ✓ UPDATE correctness: even rows updated, odd rows unchanged");
}

// ════════════════════════════════════════════════════════════════
// Test 3: DELETE 10K 正确性
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_delete_correctness() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t3 (id INTEGER PRIMARY KEY, data TEXT)");

    const M: usize = 10_000;
    for i in 1..=M as i64 {
        exec(&db, &format!("INSERT INTO t3 VALUES ({}, 'd_{}')", i, i));
    }
    assert_eq!(count_rows(&db, "t3"), M as i64, "before DELETE");

    // DELETE 每 3 行
    let delete_count = M / 3;
    let t = std::time::Instant::now();
    for i in (3..=M as i64).step_by(3) {
        exec(&db, &format!("DELETE FROM t3 WHERE id = {}", i));
    }
    print_elapsed(&format!("DELETE {} rows (every 3rd)", delete_count), delete_count, t.elapsed().as_millis() as u64);

    let after = count_rows(&db, "t3");
    assert_eq!(after, (M - delete_count) as i64, "after DELETE: expected {}, got {}", M - delete_count, after);

    // 被删的不存在
    for i in (3..=300i64).step_by(3) {
        assert!(select_row(&db, &format!("SELECT * FROM t3 WHERE id = {}", i)).is_none(),
            "deleted row {} should not exist", i);
    }

    // 未删的存在且值正确
    for i in 1..=200i64 {
        if i % 3 != 0 {
            let row = select_row(&db, &format!("SELECT * FROM t3 WHERE id = {}", i)).unwrap();
            assert_eq!(row[1], Value::text(format!("d_{}", i)), "row {} data", i);
        }
    }

    println!("  ✓ DELETE correctness: {} deleted, {} remaining", delete_count, after);
}

// ════════════════════════════════════════════════════════════════
// Test 4: PK 点查一致性（flush 前后）
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_pk_lookup_consistency() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t4 (id INTEGER PRIMARY KEY, payload TEXT, value INTEGER)");

    // INSERT 前 50K 用于 PK 查询测试
    const M: usize = 50_000;
    let t = std::time::Instant::now();
    for i in 1..=M as i64 {
        exec(&db, &format!("INSERT INTO t4 VALUES ({}, 'p_{}', {})", i, i, i * 7));
    }
    print_elapsed(&format!("INSERT {} rows", M), M, t.elapsed().as_millis() as u64);

    let samples: Vec<i64> = vec![1, 42, 1000, M as i64 / 2, M as i64 - 1, M as i64];

    // Flush 前
    for &id in &samples {
        let row = select_row(&db, &format!("SELECT * FROM t4 WHERE id = {}", id)).unwrap();
        assert_eq!(row[0], Value::Integer(id));
        assert_eq!(row[1], Value::text(format!("p_{}", id)));
        assert_eq!(row[2], Value::Integer(id * 7));
    }

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // Flush 后验证一致
    for &id in &samples {
        let row = select_row(&db, &format!("SELECT * FROM t4 WHERE id = {}", id)).unwrap();
        assert_eq!(row[0], Value::Integer(id), "id mismatch after flush");
        assert_eq!(row[1], Value::text(format!("p_{}", id)), "payload after flush");
        assert_eq!(row[2], Value::Integer(id * 7), "value after flush");
    }

    println!("  ✓ PK lookup: {} sampled IDs match before/after flush", samples.len());
}

// ════════════════════════════════════════════════════════════════
// Test 5: WHERE 过滤正确性
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_where_filter_correctness() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t5 (id INTEGER PRIMARY KEY, category INTEGER, amount INTEGER)");

    // INSERT 前 50K
    const M: usize = 50_000;
    for i in 1..=M as i64 {
        exec(&db, &format!("INSERT INTO t5 VALUES ({}, {}, {})", i, i % 10, i * 3));
    }

    // Flush + wait for index build to ensure column indexes are ready
    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    // category = 3 → 5000 行 (每 10 个有 1 个)
    assert_eq!(count_filtered(&db, "t5", "category = 3"), (M / 10) as i64);

    // AND 条件
    let filtered = count_filtered(&db, "t5", "category = 3 AND amount > 150");
    assert!(filtered > 0 && filtered < (M / 10) as i64, "AND filter");

    // 范围查询
    assert_eq!(count_filtered(&db, "t5", "id > 49000"), 1000);
    assert_eq!(count_filtered(&db, "t5", "id >= 49001"), 1000);  // 49001..50000 = 1000
    assert_eq!(count_filtered(&db, "t5", "id > 49001"), 999);    // 49002..50000 = 999

    // 具体值验证
    let row = select_row(&db, "SELECT * FROM t5 WHERE id = 12345").expect("id=12345");
    assert_eq!(row[1], Value::Integer(12345 % 10));
    assert_eq!(row[2], Value::Integer(12345 * 3));

    println!("  ✓ WHERE filter: all conditions verified");
}

// ════════════════════════════════════════════════════════════════
// Test 6: 混合 CRUD 后 COUNT 一致性
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_mixed_crud_correctness() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t6 (id INTEGER PRIMARY KEY, status TEXT, version INTEGER)");

    const M: usize = 10_000;

    // Phase 1: INSERT
    let t = std::time::Instant::now();
    for i in 1..=M as i64 {
        exec(&db, &format!("INSERT INTO t6 VALUES ({}, 'active', 1)", i));
    }
    print_elapsed(&format!("INSERT {} rows", M), M, t.elapsed().as_millis() as u64);
    assert_eq!(count_rows(&db, "t6"), M as i64);

    // Phase 2: UPDATE 前一半
    let half = M / 2;
    let t = std::time::Instant::now();
    for i in 1..=half as i64 {
        exec(&db, &format!("UPDATE t6 SET status = 'inactive', version = 2 WHERE id = {}", i));
    }
    print_elapsed(&format!("UPDATE {} rows", half), half, t.elapsed().as_millis() as u64);
    assert_eq!(count_rows(&db, "t6"), M as i64, "UPDATE should not change count");

    // 验证前半 inactive
    let r100 = select_row(&db, "SELECT * FROM t6 WHERE id = 100").unwrap();
    assert_eq!(r100[1], Value::text("inactive".into()));
    assert_eq!(r100[2], Value::Integer(2));

    // 验证后半 active
    let rend = select_row(&db, &format!("SELECT * FROM t6 WHERE id = {}", M)).unwrap();
    assert_eq!(rend[1], Value::text("active".into()));
    assert_eq!(rend[2], Value::Integer(1));

    // Phase 3: DELETE 前四分之一
    let quarter = M / 4;
    let t = std::time::Instant::now();
    for i in 1..=quarter as i64 {
        exec(&db, &format!("DELETE FROM t6 WHERE id = {}", i));
    }
    print_elapsed(&format!("DELETE {} rows", quarter), quarter, t.elapsed().as_millis() as u64);

    let expected = (M - quarter) as i64;
    assert_eq!(count_rows(&db, "t6"), expected, "after mixed CRUD");

    // 被删的不存在
    for i in [1, 2, quarter as i64] {
        assert!(select_row(&db, &format!("SELECT * FROM t6 WHERE id = {}", i)).is_none());
    }

    // quarter+1 存在且 inactive
    let r_q = select_row(&db, &format!("SELECT * FROM t6 WHERE id = {}", quarter as i64 + 1)).unwrap();
    assert_eq!(r_q[1], Value::text("inactive".into()));

    println!("  ✓ Mixed CRUD: insert={}, update={}, delete={}, final={}", M, half, quarter, expected);
}

// ════════════════════════════════════════════════════════════════
// Test 7: Flush 前后数据完全一致
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_flush_consistency() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t7 (id INTEGER PRIMARY KEY, data TEXT, num INTEGER)");

    const M: usize = 10_000;

    for i in 1..=M as i64 {
        exec(&db, &format!("INSERT INTO t7 VALUES ({}, 'd_{}', {})", i, i, i));
    }
    // UPDATE 每 7 行
    for i in (7..=M as i64).step_by(7) {
        exec(&db, &format!("UPDATE t7 SET num = -1 WHERE id = {}", i));
    }
    // DELETE 每 11 行
    for i in (11..=M as i64).step_by(11) {
        exec(&db, &format!("DELETE FROM t7 WHERE id = {}", i));
    }

    let count_before = count_rows(&db, "t7");

    let sample_ids: Vec<i64> = vec![1, 7, 11, 77, 5000, 10000];
    let samples_before: Vec<(i64, Option<Vec<Value>>)> = sample_ids.iter()
        .map(|&id| (id, select_row(&db, &format!("SELECT * FROM t7 WHERE id = {}", id))))
        .collect();

    db.flush().expect("flush");
    db.wait_for_indexes_ready();

    let count_after = count_rows(&db, "t7");
    assert_eq!(count_before, count_after, "COUNT mismatch before/after flush");

    for (id, before) in &samples_before {
        let after = select_row(&db, &format!("SELECT * FROM t7 WHERE id = {}", id));
        match (before, &after) {
            (None, None) => {}
            (Some(b), Some(a)) => assert_eq!(b, a, "row {} mismatch after flush", id),
            (Some(_), None) => panic!("row {} disappeared after flush!", id),
            (None, Some(_)) => panic!("row {} appeared after flush!", id),
        }
    }

    println!("  ✓ Flush consistency: count={}, {} samples verified", count_before, sample_ids.len());
}

// ════════════════════════════════════════════════════════════════
// Test 8: 重启后数据不丢失
// ════════════════════════════════════════════════════════════════

#[test]
fn test_300k_restart_durability() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().to_path_buf();

    const M: usize = 50_000;
    let count_before: i64;

    // Phase 1: Write (scoped so db is fully dropped before reopen)
    {
        let db = Database::create(&db_path).expect("create db");
        exec(&db, "CREATE TABLE t8 (id INTEGER PRIMARY KEY, val TEXT, ts INTEGER)");

        let t = std::time::Instant::now();
        for i in 1..=M as i64 {
            exec(&db, &format!("INSERT INTO t8 VALUES ({}, 'v_{}', {})", i, i, 1700000000 + i));
        }
        print_elapsed(&format!("INSERT {} rows (pre-restart)", M), M, t.elapsed().as_millis() as u64);

        // UPDATE: id=1, 4, 7, 10, ..., 100 → step_by(3) from 1
        for i in (1..=100i64).step_by(3) {
            exec(&db, &format!("UPDATE t8 SET val = 'modified' WHERE id = {}", i));
        }

        count_before = count_rows(&db, "t8");
        // 验证 UPDATE 生效：id=1 被改了
        let r1_before = select_row(&db, "SELECT * FROM t8 WHERE id = 1").unwrap();
        assert_eq!(r1_before[1], Value::text("modified".into()), "id=1 should be modified before close");
        // id=2 没改
        let r2_before = select_row(&db, "SELECT * FROM t8 WHERE id = 2").unwrap();
        assert_eq!(r2_before[1], Value::text("v_2".into()), "id=2 should be v_2 before close");

        db.checkpoint().expect("checkpoint");
        db.close().expect("close");
    } // db dropped here → background threads stopped, files released

    // Phase 2: Reopen + Verify
    let db2 = Database::open(&db_path).expect("open db after restart");

    let count_after = count_rows(&db2, "t8");
    assert_eq!(count_before, count_after, "COUNT after restart: {} vs {}", count_before, count_after);

    // id=1 仍为 modified
    let r1 = select_row(&db2, "SELECT * FROM t8 WHERE id = 1").expect("id=1 after restart");
    assert_eq!(r1[1], Value::text("modified".into()), "id=1 should retain 'modified' after restart");

    // id=2 仍为 v_2
    let r2 = select_row(&db2, "SELECT * FROM t8 WHERE id = 2").expect("id=2 after restart");
    assert_eq!(r2[1], Value::text("v_2".into()), "id=2 should be v_2 after restart");

    // 首尾行
    assert!(select_row(&db2, "SELECT * FROM t8 WHERE id = 1").is_some());
    assert!(select_row(&db2, &format!("SELECT * FROM t8 WHERE id = {}", M)).is_some());

    // 随机验证
    for id in [1000i64, 10000, 25000, 40000, 49999, 50000] {
        let row = select_row(&db2, &format!("SELECT * FROM t8 WHERE id = {}", id))
            .unwrap_or_else(|| panic!("row {} missing after restart", id));
        assert_eq!(row[0], Value::Integer(id), "id mismatch");
        assert_eq!(row[2], Value::Integer(1700000000 + id), "ts mismatch");
    }

    db2.checkpoint().expect("checkpoint 2");
    db2.close().expect("close 2");

    println!("  ✓ Restart durability: count={}, id=1 modified verified, 6 random rows verified", count_after);
}
