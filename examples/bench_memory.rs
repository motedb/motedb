/// Memory growth benchmark: measures RSS at each data-size milestone.
/// Tests: insert memory, query memory, cache growth, and vacuum effect.
use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn get_rss_kb() -> u64 {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
}

fn main() {
    println!("\n  MoteDB Memory Growth Benchmark");
    println!("  {}\n", "=".repeat(60));

    // Phase 1: empty DB
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let rss0 = get_rss_kb();

    println!(
        "  {:>12} rows | {:>10} KB | {:>8} B/row | phase",
        "count", "RSS", "B/row"
    );

    let db = Database::create_with_config(dir.path(), config).unwrap();
    let rss_create = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | empty DB created",
        0, rss_create, 0
    );

    db.execute(
        "CREATE TABLE t (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name TEXT,
        val FLOAT,
        code TEXT,
        ts BIGINT
    )",
    )
    .unwrap();
    db.execute("CREATE INDEX idx_code ON t (code) USING COLUMN")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t (val) USING COLUMN")
        .unwrap();
    let rss_schema = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | schema + 2 indexes",
        0, rss_schema, 0
    );

    // Phase 2: incremental inserts
    let milestones = [10_000, 50_000, 100_000, 200_000, 300_000, 500_000];
    let mut total = 0usize;
    let batch_size = 5000;

    for &milestone in &milestones {
        let to_insert = milestone - total;
        if to_insert == 0 {
            continue;
        }

        let t0 = Instant::now();
        for batch_start in (0..to_insert).step_by(batch_size) {
            let end = (batch_start + batch_size).min(to_insert);
            let mut batch = String::with_capacity(batch_size * 60);
            for i in batch_start..end {
                let idx = total + i;
                let name = format!("'user_{}'", idx % 5000);
                let val = (idx as f64 * 1.7 + 42.0) % 1000.0;
                let code = format!("'CD_{}'", idx % 1000);
                let ts = 1700000000000000u64 + idx as u64 * 1000;
                batch.push_str(&format!("({},{:.2},{},{}),", name, val, code, ts));
            }
            batch.truncate(batch.len() - 1);
            db.execute(&format!(
                "INSERT INTO t (name, val, code, ts) VALUES {}",
                batch
            ))
            .unwrap();
        }
        total = milestone;
        let insert_ms = t0.elapsed().as_millis();
        let rss = get_rss_kb();
        let b_per_row = if total > 0 {
            rss * 1024 / total as u64
        } else {
            0
        };
        println!(
            "  {:>12} | {:>10} | {:>8} | insert {}ms ({} r/s)",
            total,
            rss,
            b_per_row,
            insert_ms,
            (to_insert as u64 * 1000) / insert_ms.max(1) as u64
        );
    }

    // Phase 3: run queries and measure growth
    let rss_before_query = get_rss_kb();

    // Full scan (materializes all rows — biggest memory spike)
    let t0 = Instant::now();
    let r1 = db
        .execute("SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap()
        .row_count();
    let scan_ms = t0.elapsed().as_millis();
    let rss_after_scan = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | full scan {}ms ({} rows returned, Δ{}KB)",
        total,
        rss_after_scan,
        0,
        scan_ms,
        r1,
        rss_after_scan as i64 - rss_before_query as i64
    );

    // GROUP BY (intermediate memory)
    let rss_before_gb = get_rss_kb();
    let t0 = Instant::now();
    let r2 = db
        .execute("SELECT code, COUNT(*), AVG(val) FROM t GROUP BY code")
        .unwrap()
        .materialize()
        .unwrap()
        .row_count();
    let gb_ms = t0.elapsed().as_millis();
    let rss_after_gb = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | GROUP BY {}ms ({} groups, Δ{}KB)",
        total,
        rss_after_gb,
        0,
        gb_ms,
        r2,
        rss_after_gb as i64 - rss_before_gb as i64
    );

    // ORDER BY + LIMIT (streaming — low memory)
    let rss_before_ob = get_rss_kb();
    let t0 = Instant::now();
    let r3 = db
        .execute("SELECT * FROM t ORDER BY val DESC LIMIT 100")
        .unwrap()
        .materialize()
        .unwrap()
        .row_count();
    let ob_ms = t0.elapsed().as_millis();
    let rss_after_ob = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | ORDER BY LIMIT {}ms ({} rows, Δ{}KB)",
        total,
        rss_after_ob,
        0,
        ob_ms,
        r3,
        rss_after_ob as i64 - rss_before_ob as i64
    );

    // DISTINCT
    let rss_before_dist = get_rss_kb();
    let t0 = Instant::now();
    let r4 = db
        .execute("SELECT DISTINCT code FROM t")
        .unwrap()
        .materialize()
        .unwrap()
        .row_count();
    let dist_ms = t0.elapsed().as_millis();
    let rss_after_dist = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | DISTINCT {}ms ({} values, Δ{}KB)",
        total,
        rss_after_dist,
        0,
        dist_ms,
        r4,
        rss_after_dist as i64 - rss_before_dist as i64
    );

    // Phase 4: UPDATE and DELETE memory
    let rss_before_mut = get_rss_kb();
    let t0 = Instant::now();
    db.execute("UPDATE t SET val = 999.99 WHERE code = 'CD_42'")
        .unwrap();
    let update_ms = t0.elapsed().as_millis();
    let rss_after_update = get_rss_kb();

    let t0 = Instant::now();
    db.execute("DELETE FROM t WHERE id BETWEEN 1 AND 100")
        .unwrap();
    let delete_ms = t0.elapsed().as_millis();
    let rss_after_delete = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | UPDATE {}ms + DELETE {}ms (Δ{}KB)",
        total,
        rss_after_delete,
        0,
        update_ms,
        delete_ms,
        rss_after_delete as i64 - rss_before_mut as i64
    );

    // Phase 5: Vacuum effect
    let rss_before_vac = get_rss_kb();
    let t0 = Instant::now();
    db.vacuum().unwrap();
    let vac_ms = t0.elapsed().as_millis();
    let rss_after_vac = get_rss_kb();
    println!(
        "  {:>12} | {:>10} | {:>8} | vacuum {}ms (Δ{}KB)",
        total,
        rss_after_vac,
        0,
        vac_ms,
        rss_after_vac as i64 - rss_before_vac as i64
    );

    // Phase 6: Repeated query memory (check for leaks)
    println!("\n  Repeated scan memory stability:");
    let mut rss_history = Vec::new();
    for round in 0..10 {
        let rss_before = get_rss_kb();
        let _ = db
            .execute("SELECT * FROM t WHERE val > 500")
            .unwrap()
            .materialize()
            .unwrap()
            .row_count();
        let rss_after = get_rss_kb();
        rss_history.push(rss_after);
        println!(
            "    round {}: {}KB → {}KB (Δ{})",
            round + 1,
            rss_before,
            rss_after,
            rss_after as i64 - rss_before as i64
        );
    }
    let rss_drift = if rss_history.len() >= 2 {
        *rss_history.last().unwrap() as i64 - *rss_history.first().unwrap() as i64
    } else {
        0
    };
    println!("  10-round drift: {}KB", rss_drift);

    println!(
        "\n  Final RSS: {}KB ({}MB)",
        rss_history.last().unwrap_or(&0),
        rss_history.last().unwrap_or(&0) / 1024
    );
}
