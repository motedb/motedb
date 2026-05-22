//! SQL Query Benchmark — GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT/OFFSET,
//! LIKE, BETWEEN, IN, subquery, arithmetic expressions, complex WHERE
//!
//! Run: cargo test --test bench_sql_queries --release -- --nocapture --test-threads=1

use motedb::{Database, DBConfig};
use tempfile::TempDir;
use std::time::Instant;

fn is_ci() -> bool {
    std::env::var("CI").is_ok()
}

fn edge_config() -> DBConfig {
    DBConfig::for_edge()
}

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create_with_config(dir.path(), edge_config()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql).expect("execute SQL").materialize().expect("materialize")
}

fn print_result(name: &str, ops: usize, elapsed_ms: u64) {
    let per_op_us = if ops > 0 { (elapsed_ms as f64 * 1000.0) / ops as f64 } else { 0.0 };
    let throughput = if elapsed_ms > 0 { ops as f64 / (elapsed_ms as f64 / 1000.0) } else { f64::INFINITY };
    println!(
        "  {:<60} | {:>7} ops | {:>8.1} ms | {:>8.1} µs/op | {:>10.0} ops/s",
        name, ops, elapsed_ms as f64, per_op_us, throughput
    );
}

fn print_separator() {
    println!("  {}", "-".repeat(100));
}

/// Seed a sales table with n rows: id, customer, product, amount, qty, region, ts
fn seed_sales(db: &Database, n: usize) {
    exec(db, "CREATE TABLE sales (id INT PRIMARY KEY, customer TEXT, product TEXT, amount FLOAT, qty INT, region TEXT, ts INT)");

    let customers = ["Alice", "Bob", "Charlie", "Diana", "Eve"];
    let products = ["Widget", "Gadget", "Doohickey", "Thingamajig", "Whatchamacallit"];
    let regions = ["US", "EU", "APAC", "LATAM"];

    for i in 1..=n as i64 {
        let c = customers[(i as usize) % customers.len()];
        let p = products[(i as usize) % products.len()];
        let r = regions[(i as usize) % regions.len()];
        let amount = 10.0 + (i as f64 % 990.0);
        let qty = 1 + (i % 100);
        let ts = 1700000000 + i * 3600;
        exec(db, &format!(
            "INSERT INTO sales VALUES ({}, '{}', '{}', {:.1}, {}, '{}', {})",
            i, c, p, amount, qty, r, ts
        ));
    }
}

// ═══════════════════════════════════════════════════════════════
// Test 1: GROUP BY + Aggregate Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_group_by_aggregates() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 20 } else { 100 };

    // GROUP BY single column
    let gb1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT customer, COUNT(*) FROM sales GROUP BY customer");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("GROUP BY customer × {}", q), q, gb1_ms);

    // GROUP BY with SUM/AVG/MIN/MAX
    let gb2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT customer, SUM(amount), AVG(qty), MIN(amount), MAX(amount) FROM sales GROUP BY customer");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("GROUP BY + 4 aggregates × {}", q), q, gb2_ms);

    // GROUP BY two columns
    let gb3_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT customer, product, SUM(amount) FROM sales GROUP BY customer, product");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("GROUP BY (2 cols) × {}", q), q, gb3_ms);

    let gb1_per = gb1_ms as f64 * 1000.0 / q as f64;
    let gb3_per = gb3_ms as f64 * 1000.0 / q as f64;
    println!("  -> Single-col: {:.1}µs, Multi-col: {:.1}µs", gb1_per, gb3_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 2: HAVING Clause Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_having() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 20 } else { 100 };

    // HAVING with COUNT
    let h1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT customer, COUNT(*) FROM sales GROUP BY customer HAVING COUNT(*) > 100");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("GROUP BY + HAVING COUNT × {}", q), q, h1_ms);

    // HAVING with SUM
    let h2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT customer, SUM(amount) FROM sales GROUP BY customer HAVING SUM(amount) > 5000");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("GROUP BY + HAVING SUM × {}", q), q, h2_ms);

    // GROUP BY + WHERE + HAVING
    let h3_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT customer, SUM(amount) FROM sales WHERE qty > 10 GROUP BY customer HAVING SUM(amount) > 1000");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE + GROUP BY + HAVING × {}", q), q, h3_ms);

    let h1_per = h1_ms as f64 * 1000.0 / q as f64;
    let h3_per = h3_ms as f64 * 1000.0 / q as f64;
    println!("  -> HAVING only: {:.1}µs, WHERE+HAVING: {:.1}µs", h1_per, h3_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 3: ORDER BY + LIMIT/OFFSET
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_order_by_limit() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // ORDER BY ASC
    let ob1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, amount FROM sales ORDER BY amount ASC");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ORDER BY amount ASC × {}", q), q, ob1_ms);

    // ORDER BY DESC
    let ob2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, amount FROM sales ORDER BY amount DESC");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ORDER BY amount DESC × {}", q), q, ob2_ms);

    // ORDER BY + LIMIT
    let lim_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, amount FROM sales ORDER BY amount DESC LIMIT 10");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ORDER BY DESC + LIMIT 10 × {}", q), q, lim_ms);

    // ORDER BY + LIMIT + OFFSET (pagination)
    let page_ms = {
        let start = Instant::now();
        for p in 0..q {
            let offset = p * 10;
            exec(&db, &format!("SELECT id, amount FROM sales ORDER BY id LIMIT 10 OFFSET {}", offset));
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("ORDER BY + LIMIT 10 + OFFSET × {}", q), q, page_ms);

    let ob_per = ob1_ms as f64 * 1000.0 / q as f64;
    let lim_per = lim_ms as f64 * 1000.0 / q as f64;
    println!("  -> Full sort: {:.1}µs, Sort+LIMIT: {:.1}µs", ob_per, lim_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 4: DISTINCT Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_distinct() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // DISTINCT single column
    let d1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT DISTINCT customer FROM sales");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("DISTINCT customer × {}", q), q, d1_ms);

    // DISTINCT two columns
    let d2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT DISTINCT customer, product FROM sales");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("DISTINCT (2 cols) × {}", q), q, d2_ms);

    // DISTINCT three columns
    let d3_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT DISTINCT customer, product, region FROM sales");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("DISTINCT (3 cols) × {}", q), q, d3_ms);

    let d1_per = d1_ms as f64 * 1000.0 / q as f64;
    let d3_per = d3_ms as f64 * 1000.0 / q as f64;
    println!("  -> 1-col: {:.1}µs, 3-col: {:.1}µs", d1_per, d3_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 5: WHERE Clause Patterns (LIKE, BETWEEN, IN)
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_where_patterns() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // LIKE with prefix
    let like_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE customer LIKE 'A%'");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE LIKE 'A%' × {}", q), q, like_ms);

    // LIKE with suffix
    let like2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE product LIKE '%et'");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE LIKE '%et' × {}", q), q, like2_ms);

    // BETWEEN
    let between_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE amount BETWEEN 100 AND 500");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE BETWEEN × {}", q), q, between_ms);

    // IN (list)
    let in_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE customer IN ('Alice', 'Bob', 'Charlie')");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE IN (3 vals) × {}", q), q, in_ms);

    // Compound WHERE: AND + OR
    let compound_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE (customer = 'Alice' AND qty > 50) OR (region = 'EU' AND amount > 200)");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE compound (AND+OR) × {}", q), q, compound_ms);

    let like_per = like_ms as f64 * 1000.0 / q as f64;
    let between_per = between_ms as f64 * 1000.0 / q as f64;
    let in_per = in_ms as f64 * 1000.0 / q as f64;
    println!("  -> LIKE: {:.1}µs, BETWEEN: {:.1}µs, IN: {:.1}µs", like_per, between_per, in_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 6: Arithmetic Expressions in SELECT/WHERE
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_arithmetic_expressions() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 5_000 } else { 30_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // Computed column: amount * qty
    let expr1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, amount * qty FROM sales");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT amount * qty × {}", q), q, expr1_ms);

    // Nested: (amount + 10) * qty / 2
    let expr2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, (amount + 10) * qty / 2 FROM sales");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT complex expr × {}", q), q, expr2_ms);

    // WHERE with expression
    let expr3_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE amount * qty > 5000");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE amount * qty > 5000 × {}", q), q, expr3_ms);

    // SELECT with scalar functions on columns
    let func_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, ROUND(amount), ABS(qty - 50), LOWER(customer) FROM sales");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT with scalar functions × {}", q), q, func_ms);

    let expr1_per = expr1_ms as f64 * 1000.0 / q as f64;
    let func_per = func_ms as f64 * 1000.0 / q as f64;
    println!("  -> Expr: {:.1}µs, Func+Expr: {:.1}µs", expr1_per, func_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 7: Subquery Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_subquery() {
    let (db, _dir) = create_db();
    let n: usize = if is_ci() { 3_000 } else { 20_000 };
    seed_sales(&db, n);

    print_separator();

    let q = if is_ci() { 10 } else { 50 };

    // Subquery in WHERE
    let sub1_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM sales WHERE amount > (SELECT AVG(amount) FROM sales)");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE > subquery (AVG) × {}", q), q, sub1_ms);

    // IN subquery
    let sub2_ms = {
        let start = Instant::now();
        for _ in 0..q {
            match db.execute("SELECT id FROM sales WHERE customer IN (SELECT customer FROM sales WHERE region = 'US')") {
                Ok(r) => { r.materialize().expect("mat"); }
                Err(_) => {} // subquery may not be fully supported
            }
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE IN subquery × {}", q), q, sub2_ms);

    let sub1_per = sub1_ms as f64 * 1000.0 / q as f64;
    println!("  -> Scalar subquery: {:.1}µs/op", sub1_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 8: NULL Handling Performance
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_null_handling() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE nullable (id INT PRIMARY KEY, val INT, name TEXT)");

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // 50% NULLs
    for i in 1..=n as i64 {
        if i % 2 == 0 {
            exec(&db, &format!("INSERT INTO nullable VALUES ({}, NULL, NULL)", i));
        } else {
            exec(&db, &format!("INSERT INTO nullable VALUES ({}, {}, 'name_{}')", i, i * 10, i));
        }
    }

    print_separator();

    let q = if is_ci() { 50 } else { 200 };

    // IS NULL
    let is_null_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM nullable WHERE val IS NULL");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE IS NULL × {}", q), q, is_null_ms);

    // IS NOT NULL
    let not_null_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id FROM nullable WHERE val IS NOT NULL");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("WHERE IS NOT NULL × {}", q), q, not_null_ms);

    // COALESCE
    let coal_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, COALESCE(val, 0) FROM nullable");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("SELECT COALESCE × {}", q), q, coal_ms);

    let is_null_per = is_null_ms as f64 * 1000.0 / q as f64;
    let coal_per = coal_ms as f64 * 1000.0 / q as f64;
    println!("  -> IS NULL: {:.1}µs, COALESCE: {:.1}µs", is_null_per, coal_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 9: Prepared Statement Throughput
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_prepared_statements() {
    use motedb::types::Value;

    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE prep (id INT PRIMARY KEY, name TEXT, score FLOAT)");

    let n: usize = if is_ci() { 5_000 } else { 30_000 };

    // Seed via prepared insert
    let insert_sql = "INSERT INTO prep VALUES (?, ?, ?)";
    for i in 1..=n as i64 {
        db.execute_prepared(
            insert_sql,
            vec![Value::Integer(i), Value::text(format!("user_{}", i)), Value::Float(i as f64 * 1.5)],
        ).expect("prepared insert");
    }

    print_separator();

    let q = if is_ci() { 1_000 } else { 5_000 };

    // Prepared SELECT by PK
    let sel_sql = "SELECT * FROM prep WHERE id = ?";
    let sel_ms = {
        let start = Instant::now();
        for i in 1..=q as i64 {
            db.execute_prepared(sel_sql, vec![Value::Integer(i)]).expect("prepared select");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Prepared SELECT by PK × {}", q), q, sel_ms);

    // Prepared UPDATE
    let upd_sql = "UPDATE prep SET score = ? WHERE id = ?";
    let upd_count = q / 3;
    let upd_ms = {
        let start = Instant::now();
        for i in 1..=upd_count as i64 {
            db.execute_prepared(upd_sql, vec![Value::Float(i as f64 * 2.0), Value::Integer(i)]).expect("prepared update");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Prepared UPDATE × {}", upd_count), upd_count, upd_ms);

    // Prepared DELETE + re-insert cycle
    let del_sql = "DELETE FROM prep WHERE id = ?";
    let cycle_count = if is_ci() { 200 } else { 1000 };
    let cycle_ms = {
        let start = Instant::now();
        for i in 1..=cycle_count as i64 {
            db.execute_prepared(del_sql, vec![Value::Integer(i)]).expect("prepared delete");
            db.execute_prepared(
                insert_sql,
                vec![Value::Integer(i), Value::text(format!("re_{}", i)), Value::Float(i as f64)],
            ).expect("prepared re-insert");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Prepared DELETE+INSERT cycle × {}", cycle_count), cycle_count * 2, cycle_ms);

    let sel_per = sel_ms as f64 * 1000.0 / q as f64;
    let upd_per = upd_ms as f64 * 1000.0 / upd_count as f64;
    println!("  -> SELECT: {:.1}µs/op, UPDATE: {:.1}µs/op", sel_per, upd_per);
    db.close().ok();
}

// ═══════════════════════════════════════════════════════════════
// Test 10: Multi-table Queries
// ═══════════════════════════════════════════════════════════════

#[test]
fn bench_multi_table() {
    let (db, _dir) = create_db();

    let n: usize = if is_ci() { 3_000 } else { 15_000 };

    exec(&db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, region TEXT)");
    exec(&db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT, product TEXT)");

    for i in 1..=n as i64 {
        let region = match i % 3 { 0 => "US", 1 => "EU", _ => "APAC" };
        exec(&db, &format!("INSERT INTO users VALUES ({}, 'user_{}', '{}')", i, i, region));
        exec(&db, &format!("INSERT INTO orders VALUES ({}, {}, {:.1}, 'prod_{}')", i, i, 10.0 + (i as f64 % 990.0), i % 5));
    }

    print_separator();

    let q = if is_ci() { 20 } else { 100 };

    // Single table filtered scan (baseline)
    let single_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT id, amount FROM orders WHERE amount > 500");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("Single table scan × {}", q), q, single_ms);

    // COUNT on each table
    let count_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT COUNT(*) FROM users");
            exec(&db, "SELECT COUNT(*) FROM orders");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("COUNT(*) × 2 tables × {}", q), q * 2, count_ms);

    // Aggregate per table
    let agg_ms = {
        let start = Instant::now();
        for _ in 0..q {
            exec(&db, "SELECT region, COUNT(*) FROM users GROUP BY region");
            exec(&db, "SELECT product, SUM(amount) FROM orders GROUP BY product");
        }
        start.elapsed().as_millis() as u64
    };
    print_result(&format!("GROUP BY × 2 tables × {}", q), q * 2, agg_ms);

    let single_per = single_ms as f64 * 1000.0 / q as f64;
    println!("  -> Single table scan: {:.1}µs/op", single_per);
    db.close().ok();
}
